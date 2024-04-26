package services

import (
	"errors"
	"fmt"
	"math"
	rand2 "math/rand"
	"net/rpc"
	"strings"
	"time"
)

// LoadBalancer : Create a struct that maintain the state of the LoadBalancer
type LoadBalancer struct {
	ChoiceProbability       map[string]float64
	NumberOfPending         map[string]int //Maintain the number of pending request for each server
	MeanAverageResponseTime float64
	NumberOfReceivedRequest int
	Preferences             map[string]float64
}

type Args struct {
	Service string
	Input   int
}

type Result int
type Updated map[string]string
type Done bool

func (state *LoadBalancer) ServeRequest(args Args, result *Result) error {
	//Format service string
	service := fmt.Sprintf("Server.%s", args.Service)
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
				return nil
			}
		} else if len(state.NumberOfPending) > 1 {
			//If there are more than one server
			errServer1, errServer2 := state.sendRequestToTwoServer(service, args, result)
			if errServer1 == nil || errServer2 == nil {
				return nil
			}
		}
	}
}

func (state *LoadBalancer) sendRequestToOneServer(service string, args Args, result *Result) error {
	serverName := state.chooseFirstServer()
	server := state.connect(serverName)
	fmt.Printf("Send request to %s \n", serverName)
	start := time.Now()
	done := server.Go(service, args.Input, result, nil)
	done = <-done.Done
	if done.Error != nil {
		//If server fail during computation we exclude it from load balancing
		fmt.Printf("An error occurred %s\n", done.Error)
		delete(state.NumberOfPending, serverName)
		delete(state.ChoiceProbability, serverName)
		delete(state.Preferences, serverName)
		return done.Error
	}
	end := time.Now()
	responseTime := end.Sub(start)
	state.updateProbability(serverName, responseTime)
	state.printState()
	return nil
}

func (state *LoadBalancer) sendRequestToTwoServer(service string, args Args, result *Result) (error, error) {
	//Choose the two server and count the new request
	serverName1 := state.chooseFirstServer()
	state.NumberOfPending[serverName1]++
	serverName2 := state.chooseSecondServer(serverName1)
	state.NumberOfPending[serverName2]++
	fmt.Printf("Send request to %s and %s \n", serverName1, serverName2)
	server1 := state.connect(serverName1)
	server2 := state.connect(serverName2)
	start := time.Now()
	done1 := server1.Go(service, args.Input, result, nil)
	done2 := server2.Go(service, args.Input, result, nil)
	for {
		select {
		case <-done1.Done:
			if done1.Error != nil {
				//If during computation the server has got an error we remove it from scheduling
				fmt.Printf("Error: %s", done1.Error)
				delete(state.NumberOfPending, serverName1)
				delete(state.ChoiceProbability, serverName1)
				delete(state.Preferences, serverName1)
				if done2.Error != nil {
					return done1.Error, done2.Error
				} else {
					break
				}
			}
			//At this point the computation of the server was complete, and it can stop the other computation
			terminated2 := server2.Go("Server.StopComputation", true, nil, nil)
			terminated2 = <-terminated2.Done
			end := time.Now()
			responseTime := end.Sub(start)
			state.updateProbability(serverName1, responseTime)
			state.NumberOfPending[serverName1]--
			if terminated2.Reply == false {
				fmt.Printf("Computation already terminated")
			} else {
				state.NumberOfPending[serverName2]--
				fmt.Printf("Computation for the service %s, of the server %s was interrupted from the server %s \n", args.Service, serverName2, serverName1)
			}
			state.printState()
			return nil, nil
		case <-done2.Done:
			if done2.Error != nil {
				//If during computation the server has got an error we remove it from scheduling
				fmt.Printf("Error: %s", done2.Error)
				delete(state.NumberOfPending, serverName2)
				delete(state.ChoiceProbability, serverName2)
				delete(state.Preferences, serverName2)
				if done1.Error != nil {
					return done1.Error, done2.Error
				} else {
					break
				}
			}
			//At this point the computation of the server was complete, and it can stop the other computation
			terminated1 := server1.Go("Server.StopComputation", true, nil, nil)
			terminated1 = <-terminated1.Done
			end := time.Now()
			responseTime := end.Sub(start)
			state.updateProbability(serverName2, responseTime)
			state.NumberOfPending[serverName2]--
			if terminated1.Reply == false {
				fmt.Printf("Computation already terminated")
			} else {
				state.NumberOfPending[serverName1]--
				fmt.Printf("Computation of the server %s was interrupted from the server %s \n", serverName2, serverName1)
			}
			state.printState()
			return nil, nil
		}
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

func (state *LoadBalancer) UpdateAvailableServers(updated Updated, done *Done) error {
	for key := range state.NumberOfPending {
		//Check if all server is in the updated list
		if checkAvailability(key, updated, nil) == false {
			//If a server there isn't we must remove it from the list and switch the pending request
			delete(state.NumberOfPending, key)
			delete(state.ChoiceProbability, key)
			delete(state.Preferences, key)
			fmt.Printf("The server %s is failed!\n", key)
		}
	}
	for key := range updated {
		//Check if there are new servers
		if checkAvailability(key, nil, state.NumberOfPending) == false {
			//If a server is not in load balancer list but is the updated list add it in the load balancer list
			state.NumberOfPending[key] = 0
			//Mechanism for update probability vector when new server is detected
			state.addNewProbItem(key)
		}
	}
	return nil
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

func (state *LoadBalancer) chooseFirstServer() string {
	if len(state.NumberOfPending) == 1 {
		for key := range state.NumberOfPending {
			return key
		}
	}
	if len(state.NumberOfPending) == 0 {
		fmt.Printf("No server available now \n")
		return ""
	} else {
		minValue := math.MaxInt64
		secondMinValue := math.MaxInt64
		minKey := ""
		secondMinKey := ""
		for key, value := range state.NumberOfPending {
			if value < minValue {
				secondMinValue = minValue
				secondMinKey = minKey
				minValue = value
				minKey = key
			} else if value < secondMinValue {
				secondMinValue = value
				secondMinKey = key
			}
		}
		if len(minKey) > 0 && len(secondMinKey) > 0 {
			rand := rand2.Intn(3)
			if rand%2 == 0 {
				return minKey
			} else {
				return secondMinKey
			}
		} else if len(secondMinKey) == 0 {
			return minKey
		}
		return minKey
	}
}

func (state *LoadBalancer) chooseSecondServer(firstServer string) string {
	maxValue := 0.0
	maxKey := ""
	for key, value := range state.ChoiceProbability {
		if value >= maxValue && strings.Compare(firstServer, key) != 0 {
			maxValue = value
			maxKey = key
		}
	}
	return maxKey
}

func (state *LoadBalancer) updateProbability(server string, responseTime time.Duration) {
	alpha := 0.1
	sum := 0.
	//Update preferences
	state.Preferences[server] = state.Preferences[server] - alpha*(responseTime.Seconds()-state.MeanAverageResponseTime)*(1-state.ChoiceProbability[server])
	for key, value := range state.ChoiceProbability {
		if strings.Compare(key, server) != 0 {
			value = value + alpha*(responseTime.Seconds()-state.MeanAverageResponseTime)*(state.ChoiceProbability[key])
		}
	}
	for key := range state.ChoiceProbability {
		for key1 := range state.ChoiceProbability {
			sum += math.Exp(state.Preferences[key1])
		}
		state.ChoiceProbability[key] = math.Exp(state.Preferences[key]) / sum
		sum = 0
	}
	state.MeanAverageResponseTime = (state.MeanAverageResponseTime*float64(state.NumberOfReceivedRequest) + responseTime.Seconds()) / float64(state.NumberOfReceivedRequest+1)
	state.NumberOfReceivedRequest++
}

func (state *LoadBalancer) addNewProbItem(newServer string) {
	if len(state.ChoiceProbability) == 0 {
		//If there isn't other servers
		state.Preferences[newServer] = 0
		state.ChoiceProbability[newServer] = 1
	} else {
		sum := 0.
		for _, value := range state.Preferences {
			sum += value
		}
		//Calculate the mean
		sum = sum / float64(len(state.ChoiceProbability))
		//Set the preference for the new server at the mean of the other preferences
		state.Preferences[newServer] = sum
		state.ChoiceProbability[newServer] = 0
		//Update probability
		for key := range state.ChoiceProbability {
			for key1 := range state.ChoiceProbability {
				sum += math.Exp(state.Preferences[key1])
			}
			state.ChoiceProbability[key] = math.Exp(state.Preferences[key]) / sum
			sum = 0
		}
	}
}

func (state *LoadBalancer) connect(serverName string) *rpc.Client {
	server, err := rpc.Dial("tcp", serverName)
	if err != nil {
		fmt.Printf("An error occurred %s on the serverName %s", err, serverName)
		//Delete the server from the balancing list
		delete(state.NumberOfPending, serverName)
		delete(state.ChoiceProbability, serverName)
	}
	return server
}

func (state *LoadBalancer) printState() {
	fmt.Printf("------------------------------------------------\n")
	fmt.Printf("Number of pending request for each server: \n")
	for s, i := range state.NumberOfPending {
		fmt.Printf("Server %s : %d\n", s, i)
	}
	fmt.Printf("Choice probability for each server: \n")
	for s, f := range state.ChoiceProbability {
		fmt.Printf("Server %s : %f\n", s, f)
	}
	fmt.Printf("------------------------------------------------\n")
}
