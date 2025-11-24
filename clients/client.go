package main

import (
	proto "ReplicationService/grpc"
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ClientConnection struct {
	serverAddrs  []string
	currentIdx   int
	client       proto.ReplicationServiceClient
	conn         *grpc.ClientConn
	mu           sync.RWMutex
	reconnecting bool
	clientID     string
}

func NewClientConnection(clientID string, serverAddrs []string) *ClientConnection {
	return &ClientConnection{
		serverAddrs: serverAddrs,
		currentIdx:  0,
		clientID:    clientID,
	}
}

// Connect establishes initial connection to the first available server
func (cc *ClientConnection) Connect() error {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	for i := 0; i < len(cc.serverAddrs); i++ {
		addr := cc.serverAddrs[cc.currentIdx]
		log.Printf("Attempting to connect to server at %s...", addr)

		dialCtx, dialCancel := context.WithTimeout(context.Background(), 3*time.Second)
		conn, err := grpc.DialContext(dialCtx, addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		dialCancel()

		if err != nil {
			log.Printf("Failed to connect to %s: %v", addr, err)
			cc.currentIdx = (cc.currentIdx + 1) % len(cc.serverAddrs)
			continue
		}

		cc.conn = conn
		cc.client = proto.NewReplicationServiceClient(conn)
		log.Printf("Successfully connected to server at %s", addr)
		return nil
	}

	return fmt.Errorf("failed to connect to any server")
}

// Reconnect tries to connect to the next available server
func (cc *ClientConnection) Reconnect() error {
	cc.mu.Lock()
	if cc.reconnecting {
		cc.mu.Unlock()
		return fmt.Errorf("already reconnecting")
	}
	cc.reconnecting = true

	// Close old connection if exists
	if cc.conn != nil {
		cc.conn.Close()
		cc.conn = nil
		cc.client = nil
	}
	cc.mu.Unlock()

	// Keep trying forever until we connect
	for {
		cc.mu.Lock()
		cc.currentIdx = (cc.currentIdx + 1) % len(cc.serverAddrs)
		addr := cc.serverAddrs[cc.currentIdx]
		cc.mu.Unlock()

		log.Printf("Reconnecting to server at %s...", addr)

		dialCtx, dialCancel := context.WithTimeout(context.Background(), 3*time.Second)
		conn, err := grpc.DialContext(dialCtx, addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		dialCancel()

		if err != nil {
			log.Printf("Failed to reconnect to %s: %v", addr, err)
			time.Sleep(time.Second) // Wait before trying next server
			continue
		}

		cc.mu.Lock()
		cc.conn = conn
		cc.client = proto.NewReplicationServiceClient(conn)
		cc.reconnecting = false
		cc.mu.Unlock()

		log.Printf("Successfully reconnected to server at %s", addr)
		return nil
	}
}

// GetClient returns the current client, reconnecting if necessary
func (cc *ClientConnection) GetClient() proto.ReplicationServiceClient {
	cc.mu.RLock()
	client := cc.client
	cc.mu.RUnlock()

	if client == nil {
		cc.Reconnect()
		cc.mu.RLock()
		client = cc.client
		cc.mu.RUnlock()
	}

	return client
}

// Close closes the connection
func (cc *ClientConnection) Close() {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	if cc.conn != nil {
		cc.conn.Close()
	}
}

func main() {
	var clientID string

	flag.StringVar(&clientID, "id", "", "Client ID (required)")
	flag.Parse()

	// List all server addresses - add more as needed
	serverAddrs := []string{
		"localhost:50051",
		"localhost:50052",
		"localhost:50053",
	}

	if clientID == "" {
		fmt.Fprintln(os.Stderr, "client id is required: -id <name>")
		os.Exit(2)
	}

	clientConn := NewClientConnection(clientID, serverAddrs)

	if err := clientConn.Connect(); err != nil {
		log.Fatalf("Failed to establish initial connection: %v", err)
	}
	defer clientConn.Close()

	stdin := bufio.NewScanner(os.Stdin)
	fmt.Println("Commands:")
	fmt.Println("  bid <amount>   - place a bid, e.g. 'bid 100'")
	fmt.Println("  result         - request highest bid")
	fmt.Println("  quit           - exit client")

	for stdin.Scan() {
		line := strings.TrimSpace(stdin.Text())
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "quit", "exit":
			fmt.Println("exiting")
			return

		case "bid":
			var amount int64
			if len(parts) >= 2 {
				v, err := strconv.ParseInt(parts[1], 10, 64)
				if err != nil {
					log.Printf("invalid amount: %v", err)
					continue
				}
				amount = v
			} else {
				fmt.Print("Enter bid amount: ")
				if !stdin.Scan() {
					continue
				}
				v, err := strconv.ParseInt(strings.TrimSpace(stdin.Text()), 10, 64)
				if err != nil {
					log.Printf("invalid amount: %v", err)
					continue
				}
				amount = v
			}

			// Keep trying until successful
			for {
				client := clientConn.GetClient()
				rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 3*time.Second)
				resp, err := client.Bid(rpcCtx, &proto.BidRequest{Amount: amount, Id: clientID})
				rpcCancel()

				if err != nil {
					log.Printf("BID RPC error: %v - reconnecting...", err)
					clientConn.Reconnect()
					continue
				}

				log.Printf("Bid sent: id=%s amount=%d ack=%v", clientID, amount, resp.GetAck())
				break
			}

		case "result":
			// Keep trying until successful
			for {
				client := clientConn.GetClient()
				rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 3*time.Second)
				resp, err := client.Result(rpcCtx, &proto.Empty{})
				rpcCancel()

				if err != nil {
					log.Printf("RESULT RPC error: %v - reconnecting...", err)
					clientConn.Reconnect()
					continue
				}

				log.Printf("RESULT: highest bid=%d by client=%s", resp.GetResult(), resp.GetHighestBidderId())
				break
			}

		default:
			fmt.Println("unknown command; use 'bid <amount>', 'result', or 'quit'")
		}
	}

	if err := stdin.Err(); err != nil {
		log.Printf("stdin error: %v", err)
	}
}
