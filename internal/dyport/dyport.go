package dyport

import (
	"math/rand"
	"net"
	"sync"
	"time"
)

const (
	minPort    = 10000
	endPort    = 65535
	countPorts = 1024
	maxBlocks  = 16
	attempts   = 3
)

var (
	port     int
	initPort int
	once     sync.Once
	mu       sync.Mutex
)

func AllocatePorts(count int) ([]int, error) {
	if count > countPorts-1 {
		count = countPorts - 1
	}

	mu.Lock()
	defer mu.Unlock()

	ports := make([]int, 0)

	once.Do(func() {
		source := rand.NewSource(time.Now().UnixNano())
		rng := rand.New(source)
		for i := 0; i < attempts; i++ {
			initPort = minPort + rng.Intn(maxBlocks)*countPorts
			lockLn, err := listener(initPort)
			if err != nil {
				continue
			}
			lockLn.Close()
			return
		}
		panic("failed to allocate port block")
	})

	for len(ports) < count {
		port += 1

		if port < initPort+1 || port >= initPort+countPorts {
			port = initPort + 1
		}

		ln, err := listener(port)
		if err != nil {
			continue
		}

		ln.Close()
		ports = append(ports, port)
	}

	return ports, nil
}

func listener(port int) (*net.TCPListener, error) {
	return net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: port})
}
