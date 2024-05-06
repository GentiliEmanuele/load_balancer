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
	var listOfServer []string
	//Get the listOfServer from registry
	registryAddress := os.Getenv("REGISTRY")
	//For all services offered send they to the service registry
	registry, err := rpc.Dial("tcp", registryAddress)
	if err != nil {
		fmt.Printf("An error occurred %s\n", err)
	}
	ip = fmt.Sprintf("%s:%s", ip, port)
	err = registry.Call("Registry.GetServices", ip, &listOfServer)
	//Create a state for loadBalancer using the list of services
	loadBalancerState := services.LoadBalancer{
		NumberOfPending: initNumberOfPending(listOfServer),
		Mutex:           sync.RWMutex{},
		History:         initNumberOfPending(listOfServer),
	}
	//Wait request from client and update from registry
	loadBalancer := rpc.NewServer()
	err = loadBalancer.RegisterName("LoadBalancer", &loadBalancerState)
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

func initNumberOfPending(servers []string) map[string]int {
	pending := make(map[string]int)
	for _, address := range servers {
		pending[address] = 0
	}
	return pending
}
