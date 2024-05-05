package services

import (
	"errors"
	"fmt"
	_types "loadbalancer1/types"
	"math"
	"math/rand"
	"net/rpc"
	"strings"
	"sync"
	"time"
)

// LoadBalancer : Create a struct that maintain the state of the LoadBalancer
type LoadBalancer struct {
	NumberOfPending map[string]int //Maintain the number of pending request for each server
	Mutex           sync.RWMutex
	History         map[string]int
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

func (state *LoadBalancer) UpdateAvailableServers(updated _types.Updated, idle *_types.Idle) error {
	state.Mutex.Lock()
	for key, nPending := range state.NumberOfPending {
		//Check if all server is in the updated list
		if checkAvailability(key, updated, nil) == false {
			//If a server there isn't we must remove it from the list and switch the pending request
			delete(state.NumberOfPending, key)
		}
		if nPending == 0 && checkAvailability(key, nil, state.NumberOfPending) == true {
			*idle = append(*idle, key)
		}
	}
	state.Mutex.Unlock()
	for key := range updated {
		//Check if there are new servers
		if checkAvailability(key, nil, state.NumberOfPending) == false {
			//If a server is not in load balancer list but is the updated list add it in the load balancer list
			state.lockedAddNewItem(key)
		}
	}
	return nil
}

func (state *LoadBalancer) sendRequestToOneServer(service string, args _types.Args, result *_types.Result) error {
	serverName := state.chooseServer("")
	state.lockedIncrementPending(serverName)
	server := state.connect(serverName)
	done := server.Go(service, args.Input, result, nil)
	done = <-done.Done
	if done.Error != nil {
		//If server fail during computation we exclude it from load balancing
		fmt.Printf("An error occurred %s\n", done.Error)
		state.lockedDeleteItem(serverName)
		return done.Error
	}
	state.lockedDecrementPending(serverName)
	return nil
}

func (state *LoadBalancer) sendRequestToTwoServer(service string, args _types.Args, result *_types.Result) (error, error) {
	//Choose the two server and count the new request
	serverName1 := state.chooseServer("")
	serverName2 := state.chooseServer(serverName1)
	state.lockedIncrementPending(serverName1)
	state.lockedIncrementPending(serverName2)
	server1 := state.connect(serverName1)
	server2 := state.connect(serverName2)
	done1 := server1.Go(service, args.Input, result, nil)
	done2 := server2.Go(service, args.Input, result, nil)
	select {
	case <-done1.Done:
		if done1.Error != nil {
			//If during computation the server has got an error we remove it from scheduling
			fmt.Printf("Error: %s\n", done1.Error)
			state.lockedDeleteItem(serverName1)
			select {
			//If the first server fail, wait the second server
			case <-done2.Done:
				if done2.Error != nil {
					state.lockedDeleteItem(serverName2)
					fmt.Printf("The servers %s and %s were failed, search new servers\n", serverName1, serverName2)
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
			state.lockedDeleteItem(serverName2)
			select {
			//If the first server fail, wait the second server
			case <-done1.Done:
				if done1.Error != nil {
					state.lockedDeleteItem(serverName1)
					fmt.Printf("The servers %s and %s were failed, search new servers\n", serverName1, serverName2)
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

func checkAvailability(server string, list map[string]string, list2 map[string]int) bool {
	in := false
	if list != nil {
		for key := range list {
			if strings.Compare(key, server) == 0 {
				in = true
				break
			}
		}
	} else if list2 != nil {
		for key := range list2 {
			if strings.Compare(key, server) == 0 {
				in = true
				break
			}
		}
	}
	return in
}

func (state *LoadBalancer) chooseServer(ignoredServer string) string {
	if len(state.NumberOfPending) == 1 {
		state.Mutex.Lock()
		for key := range state.NumberOfPending {
			state.Mutex.Unlock()
			return key
		}
	}
	if len(state.NumberOfPending) == 0 {
		fmt.Printf("No server available now \n")
		return ""
	} else {
		minLoadServers := state.findMinus(ignoredServer)
		if len(minLoadServers)-1 != 0 {
			index := rand.Intn(len(minLoadServers) - 1)
			return minLoadServers[index]
		} else {
			return minLoadServers[0]
		}
	}
}

func (state *LoadBalancer) findMinus(ignored string) []string {
	state.Mutex.Lock()
	minServers := make([]string, 0)
	minLoad := math.MaxInt
	//Find min load
	for s, load := range state.NumberOfPending {
		if load < minLoad && strings.Compare(s, ignored) != 0 {
			minLoad = load
		}
	}
	//group by min load
	for s, load := range state.NumberOfPending {
		if load == minLoad && strings.Compare(s, ignored) != 0 {
			minServers = append(minServers, s)
		}
	}
	state.Mutex.Unlock()
	return minServers
}

func (state *LoadBalancer) connect(serverName string) *rpc.Client {
	state.Mutex.Lock()
	server, err := rpc.Dial("tcp", serverName)
	if err != nil {
		fmt.Printf("An error occurred %s on the serverName %s", err, serverName)
		//Delete the server from the balancing list
		state.lockedDeleteItem(serverName)
		state.Mutex.Unlock()
	}
	state.Mutex.Unlock()
	return server
}

func (state *LoadBalancer) lockedIncrementPending(server string) {
	state.Mutex.Lock()
	state.NumberOfPending[server]++
	state.Mutex.Unlock()
}

func (state *LoadBalancer) lockedDecrementPending(server string) {
	state.Mutex.Lock()
	state.NumberOfPending[server]--
	state.Mutex.Unlock()
}

func (state *LoadBalancer) lockedDeleteItem(server string) {
	state.Mutex.Lock()
	delete(state.NumberOfPending, server)
	state.Mutex.Unlock()
}

func (state *LoadBalancer) lockedAddNewItem(server string) {
	state.Mutex.Lock()
	state.NumberOfPending[server] = 0
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
