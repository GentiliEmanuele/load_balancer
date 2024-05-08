package services

import (
	"errors"
	"fmt"
	_types "loadbalancer1/types"
	"math"
	"math/rand"
	"net/rpc"
	"sync"
	"time"
)

// LoadBalancer : Create a struct that maintain the state of the LoadBalancer
type LoadBalancer struct {
	NumberOfPending map[string]int //Maintain the number of pending request for each server
	Mutex           sync.RWMutex
	History         map[string]int
	NumberOfFails   map[string]int
	Tolerance       int
}

func (state *LoadBalancer) ServeRequest(args _types.Args, result *_types.Result) error {
	//Format service string
	service := state.formatServiceName(args.Service)
	for {
		if len(state.NumberOfPending) == 0 {
			//If there are no server available
			err := state.waitOneAvailableServer()
			if err != nil {
				return err
			}
		}
		if len(state.NumberOfPending) == 1 {
			//If there is one server available
			err := state.sendRequestToOneServer(service, args, result)
			if err == nil {
				break
			}
		} else if len(state.NumberOfPending) > 1 {
			//If there are more than one server
			errServer1, errServer2 := state.sendRequestToTwoServer(service, args, result)
			if errServer1 == nil || errServer2 == nil {
				break
			}
		}
	}
	return nil
}

func (state *LoadBalancer) AddNewServer(server _types.Server, updated *_types.Updated) error {
	state.lockedAddNewItem(string(server))
	for s := range state.NumberOfPending {
		*updated = append(*updated, s)
	}
	return nil
}

func (state *LoadBalancer) sendRequestToOneServer(service string, args _types.Args, result *_types.Result) error {
	server, serverName, err := state.connect()
	if err != nil {
		return err
	}
	done := server.Go(service, args.Input, result, nil)
	done = <-done.Done
	if done.Error != nil {
		//If server fail during computation we exclude it from load balancing
		fmt.Printf("An error occurred %s\n", done.Error)
		state.handleServerFailure(serverName)
		return done.Error
	}
	state.lockedDecrementPending(serverName)
	return nil
}

func (state *LoadBalancer) sendRequestToTwoServer(service string, args _types.Args, result *_types.Result) (error, error) {
	//Choose the two server and count the new request
	server1, serverName1, err1 := state.connect()
	//If connection not return a server mean that all server are fail
	if server1 == nil && err1 != nil {
		return err1, err1
	}
	server2, serverName2, err2 := state.connect()
	if server2 == nil && err2 != nil {
		return err2, err2
	}
	done1 := server1.Go(service, args.Input, result, nil)
	done2 := server2.Go(service, args.Input, result, nil)
	select {
	case <-done1.Done:
		if done1.Error != nil {
			//If during computation the server has got an error we remove it from scheduling
			fmt.Printf("An error occurred: %s\n", done1.Error)
			state.handleServerFailure(serverName1)
			select {
			//If the first server fail, wait the second server
			case <-done2.Done:
				if done2.Error != nil {
					state.handleServerFailure(serverName2)
					fmt.Printf("The servers %s and %s do not respond, try to choose new servers\n", serverName1, serverName2)
					return done1.Error, done2.Error
				} else {
					return done1.Error, nil
				}
			}
		}
		//At this point the computation of the server was complete, and it can stop the other computation
		terminated2 := server2.Go("Server.StopComputation", true, nil, nil)
		terminated2 = <-terminated2.Done
		state.lockedDecrementPending(serverName1)
		state.lockedDecrementPending(serverName2)
		if terminated2.Reply == false {
			fmt.Printf("Computation already terminated")
		} else {
			fmt.Printf("Computation for the service %s, of the server %s was interrupted from the server %s \n", args.Service, serverName2, serverName1)
		}
		return nil, nil
	case <-done2.Done:
		if done2.Error != nil {
			//If during computation the server has got an error we remove it from scheduling
			fmt.Printf("An error occurred: %s\n", done2.Error)
			state.handleServerFailure(serverName2)
			select {
			//If the first server fail, wait the second server
			case <-done1.Done:
				if done1.Error != nil {
					state.handleServerFailure(serverName1)
					fmt.Printf("The servers %s and %s do not respond, try to choose new servers\n", serverName1, serverName2)
					return done1.Error, done2.Error
				} else {
					return nil, done2.Error
				}
			}
		}
		//At this point the computation of the server was complete, and it can stop the other computation
		terminated1 := server1.Go("Server.StopComputation", true, nil, nil)
		terminated1 = <-terminated1.Done
		if terminated1.Reply == false {
			fmt.Printf("Computation already terminated")
		} else {
			state.lockedDecrementPending(serverName1)
			state.lockedDecrementPending(serverName2)
			fmt.Printf("Computation for the service %s, of the server %s was interrupted from the server %s \n", args.Service, serverName2, serverName1)
		}
		return nil, nil
	}
}

func (state *LoadBalancer) waitOneAvailableServer() error {
	start := time.Now()
	fmt.Printf("Wait available server \n")
	for {
		if len(state.NumberOfPending) > 0 {
			return nil
		}
		end := time.Now()
		if end.Sub(start) > 1*time.Minute {
			fmt.Printf("No server available now, wait a few minutes and retry \n")
			return errors.New("no server available now\n")
		}
	}
}

func (state *LoadBalancer) chooseServer() string {
	if len(state.NumberOfPending) == 1 {
		for key := range state.NumberOfPending {
			return key
		}
	}
	if len(state.NumberOfPending) == 0 {
		fmt.Printf("No server available now \n")
		return ""
	} else {
		minLoadServers := state.findMinus()
		if len(minLoadServers)-1 != 0 {
			index := rand.Intn(len(minLoadServers) - 1)
			return minLoadServers[index]
		} else {
			return minLoadServers[0]
		}
	}
}

func (state *LoadBalancer) findMinus() []string {
	minServers := make([]string, 0)
	minLoad := math.MaxInt
	//Find min load
	for _, load := range state.NumberOfPending {
		if load < minLoad {
			minLoad = load
		}
	}
	//group by min load
	for s, load := range state.NumberOfPending {
		if load == minLoad {
			minServers = append(minServers, s)
		}
	}
	return minServers
}

func (state *LoadBalancer) connect() (*rpc.Client, string, error) {
	state.Mutex.Lock()
	defer state.Mutex.Unlock()
	var server *rpc.Client
	var serverName string
	var err error
	for {
		serverName = state.chooseServer()
		server, err = rpc.Dial("tcp", serverName)
		if err != nil {
			state.NumberOfFails[serverName]++
			if state.NumberOfFails[serverName] > state.Tolerance {
				delete(state.NumberOfPending, serverName)
			}
			if len(state.NumberOfPending) == 0 {
				return nil, "", err
			}
		} else {
			state.NumberOfPending[serverName]++
			return server, serverName, nil
		}
	}
}

func (state *LoadBalancer) lockedDecrementPending(server string) {
	state.Mutex.Lock()
	state.NumberOfPending[server]--
	state.Mutex.Unlock()
}

func (state *LoadBalancer) handleServerFailure(server string) {
	state.Mutex.Lock()
	state.NumberOfFails[server]++
	if state.NumberOfFails[server] == state.Tolerance {
		delete(state.NumberOfPending, server)
	}
	state.Mutex.Unlock()
}

func (state *LoadBalancer) lockedAddNewItem(server string) {
	state.Mutex.Lock()
	state.NumberOfPending[server] = 0
	state.NumberOfFails[server] = 0
	state.Mutex.Unlock()
}

func (state *LoadBalancer) formatServiceName(service string) string {
	state.Mutex.Lock()
	service = fmt.Sprintf("Server.%s", service)
	state.Mutex.Unlock()
	return service
}

func (state *LoadBalancer) updateAndPrintHistory(server string) {
	state.Mutex.Lock()
	state.History[server]++
	fmt.Printf("--------------------------------------------------\n")
	for s, i := range state.History {
		fmt.Printf("%s : %d \n", s, i)
	}
	fmt.Printf("--------------------------------------------------\n")
	state.Mutex.Unlock()
}
