package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"coursework-networking-sockets/internal/app"
	"coursework-networking-sockets/internal/tcp"
)

func main() {
	mode := flag.String("mode", "server", "server|client|uber-server|uber-client")
	addr := flag.String("addr", "127.0.0.1:9000", "ip:porta")
	maxClients := flag.Int("max", 2, "max clients (server)")
	readTO := flag.Duration("rto", 0*time.Second, "read timeout por operacao (0=sem)")
	writeTO := flag.Duration("wto", 0*time.Second, "write timeout por operacao (0=sem)")

	// opções do client de teste original
	n := flag.Int("n", 1, "numero de clients concorrentes (client mode)")
	msg := flag.String("msg", "ping", "mensagem para enviar (client mode)")
	every := flag.Duration("every", 1*time.Second, "intervalo entre envios (client mode)")
	count := flag.Int("count", 5, "quantidade de mensagens por client (client mode)")

	flag.Parse()

	// contexto global com cancelamento por sinal (Ctrl+C)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		log.Println("[main] sinal recebido, encerrando...")
		cancel()
	}()

	switch *mode {
	case "uber-server":
		if err := app.RunUberServer(ctx, *addr); err != nil {
			log.Fatalf("uber-server: %v", err)
		}

	case "uber-client":
		if err := app.RunUberClient(ctx, *addr); err != nil {
			log.Fatalf("uber-client: %v", err)
		}

	case "server":
		runServer(ctx, *addr, *maxClients, *readTO, *writeTO)

	case "client":
		runClient(*addr, *n, *msg, *every, *count, *readTO, *writeTO)

	default:
		log.Fatalf("invalid mode: %s (use server|client|uber-server|uber-client)", *mode)
	}
}

// ─── Modos originais de teste (sem alteração) ────────────────────────────────

func runServer(ctx context.Context, addr string, maxClients int, rto, wto time.Duration) {
	srv, err := tcp.NewServer(ctx, addr, maxClients)
	if err != nil {
		log.Fatalf("NewServer: %v", err)
	}
	defer func() { _ = srv.Close() }()

	log.Printf("[server] listening on %s (maxClients=%d)", addr, maxClients)
	if rto > 0 || wto > 0 {
		log.Printf("[server] received flags: rto=%s wto=%s", rto, wto)
	}

	go func() {
		for err := range srv.Errors() {
			log.Printf("[server] error: %v", err)
		}
	}()

	t := time.NewTicker(5 * time.Second)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			payload := []byte("server broadcast " + time.Now().Format(time.RFC3339))
			srv.BroadcastFrame(payload)
			log.Printf("[server] broadcasted %q", payload)
		}
	}
}

func runClient(addr string, n int, msg string, every time.Duration, count int, rto, wto time.Duration) {
	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()

			conn, err := net.Dial("tcp", addr)
			if err != nil {
				log.Printf("[client %d] dial error: %v", i, err)
				return
			}

			c := tcp.NewClient(conn)
			c.SetReadTimeout(rto)
			c.SetWriteTimeout(wto)

			log.Printf("[client %d] connected -> %s (rto=%s wto=%s)", i, addr, rto, wto)

			done := make(chan struct{})
			var mu sync.Mutex
			serverFull := false

			go func() {
				defer close(done)
				for {
					p, err := c.ReadFrame()
					if err != nil {
						if isExpectedClientReadClose(err) {
							return
						}
						mu.Lock()
						sf := serverFull
						mu.Unlock()
						if sf && isExpectedServerRefusalClose(err) {
							return
						}
						log.Printf("[client %d] read error: %v", i, err)
						return
					}
					log.Printf("[client %d] <- %q", i, string(p))
					if string(p) == string(tcp.ErrServerFull) {
						mu.Lock()
						serverFull = true
						mu.Unlock()
					}
				}
			}()

			for k := 0; k < count; k++ {
				mu.Lock()
				sf := serverFull
				mu.Unlock()
				if sf {
					break
				}

				payload := []byte(fmt.Sprintf("%s (client=%d seq=%d at=%s)", msg, i, k, time.Now().Format(time.RFC3339)))
				if err := c.SendFrame(payload); err != nil {
					if isExpectedClientWriteClose(err) {
						break
					}
					mu.Lock()
					sf = serverFull
					mu.Unlock()
					if sf && isExpectedServerRefusalWrite(err) {
						break
					}
					log.Printf("[client %d] send error: %v", i, err)
					break
				}

				log.Printf("[client %d] -> %q", i, string(payload))
				time.Sleep(every)
			}

			_ = c.Close()
			<-done
			log.Printf("[client %d] done", i)
		}()
	}

	wg.Wait()
}

// ─── Helpers de erro (sem alteração) ─────────────────────────────────────────

func isExpectedClientReadClose(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
		return true
	}
	return strings.Contains(err.Error(), "use of closed network connection")
}

func isExpectedServerRefusalClose(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "connection reset by peer")
}

func isExpectedClientWriteClose(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	return strings.Contains(err.Error(), "use of closed network connection")
}

func isExpectedServerRefusalWrite(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection reset by peer")
}
