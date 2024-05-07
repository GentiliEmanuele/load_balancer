package main

import (
	"fmt"
	"loadbalancer1/services"
	"net"
	"net/rpc"
	"os"
	"sync"
)

func main() {
	port := os.Getenv("PORT")
	playOnLoadBalancer(
		port,
	)
	select {}
}

func playOnLoadBalancer(port string) {
	loadBalancer := rpc.NewServer()
	//Create a state for loadBalancer using the list of services
	loadBalancerState := services.LoadBalancer{
		NumberOfPending: make(map[string]int),
		Mutex:           sync.RWMutex{},
		History:         make(map[string]int),
	}
	//Wait request from client and update from registry
	err := loadBalancer.RegisterName("LoadBalancer", &loadBalancerState)
	if err != nil {
		fmt.Printf("An error occurred %s", err)
	}
	addr := fmt.Sprintf(":%s", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Printf("An error occurred %s\n", err)
	}
	go loadBalancer.Accept(lis)
	select {}
}
