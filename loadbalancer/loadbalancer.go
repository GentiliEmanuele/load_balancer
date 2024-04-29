package main

import (
	"fmt"
	"loadbalancer1/services"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
)

func main() {
	port := os.Getenv("PORT")
	playOnLoadBalancer(
		GetOutboundIP().String(),
		port,
	)
	select {}
}

func playOnLoadBalancer(ip string, port string) {
	var listOfServices map[string]string
	//Get the listOfServices from registry
	registryAddress := os.Getenv("REGISTRY")
	//For all services offered send they to the service registry
	registry, err := rpc.Dial("tcp", registryAddress)
	if err != nil {
		fmt.Printf("An error occurred %s", err)
	}
	ip = fmt.Sprintf("%s:%s", ip, port)
	err = registry.Call("Registry.GetServices", ip, &listOfServices)
	for key, value := range listOfServices {
		fmt.Printf(key + " " + value + "\n")
	}
	//Create a state for loadBalancer using the list of services
	loadBalancerState := services.LoadBalancer{
		ChoiceProbability:       initServerProb(listOfServices),
		MeanAverageResponseTime: float64(0),
		NumberOfReceivedRequest: 0,
		Preferences:             initServerProb(listOfServices),
		NumberOfPending:         initNumberOfPending(listOfServices),
		Mutex:                   sync.RWMutex{},
	}
	//Wait request from client and update from registry
	loadBalancer := rpc.NewServer()
	err = loadBalancer.RegisterName("LoadBalancer", &loadBalancerState)
	if err != nil {
		fmt.Printf("An error occurred %s", err)
	}

	fmt.Printf("The load balancer listening on the port %s\n", port)
	addr := fmt.Sprintf(":%s", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Printf("An error occurred %s\n", err)
	}
	go loadBalancer.Accept(lis)
	select {}
}

func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer func(conn net.Conn) {
		_ = conn.Close()
	}(conn)
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP
}

func initServerProb(servers map[string]string) map[string]float64 {
	state := make(map[string]float64)
	for key := range servers {
		state[key] = 1.0 / float64(len(servers))
	}
	return state
}

func initNumberOfPending(servers map[string]string) map[string]int {
	pending := make(map[string]int)
	for key := range servers {
		pending[key] = 0
	}
	return pending
}
