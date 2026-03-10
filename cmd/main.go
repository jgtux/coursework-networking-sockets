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

	"coursework-networking-sockets/internal/tcp"
)

func main() {
	mode := flag.String("mode", "server", "server|client")
	addr := flag.String("addr", "127.0.0.1:9000", "ip:porta")
	maxClients := flag.Int("max", 2, "max clients (server)")
	readTO := flag.Duration("rto", 0*time.Second, "read timeout por operacao (0=sem)")
	writeTO := flag.Duration("wto", 0*time.Second, "write timeout por operacao (0=sem)")

	// client options
	n := flag.Int("n", 1, "numero de clients concorrentes (client mode)")
	msg := flag.String("msg", "ping", "mensagem para enviar (client mode)")
	every := flag.Duration("every", 1*time.Second, "intervalo entre envios (client mode)")
	count := flag.Int("count", 5, "quantidade de mensagens por client (client mode)")

	flag.Parse()

	switch *mode {
	case "server":
		runServer(*addr, *maxClients, *readTO, *writeTO)
	case "client":
		runClient(*addr, *n, *msg, *every, *count, *readTO, *writeTO)
	default:
		log.Fatalf("invalid mode: %s (use server|client)", *mode)
	}
}

func runServer(addr string, maxClients int, rto, wto time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv, err := tcp.NewServer(ctx, addr, maxClients)
	if err != nil {
		log.Fatalf("NewServer: %v", err)
	}
	defer func() { _ = srv.Close() }()

	log.Printf("[server] listening on %s (maxClients=%d)", addr, maxClients)
	if rto > 0 || wto > 0 {
		log.Printf("[server] received flags: rto=%s wto=%s", rto, wto)
	}

	// shutdown por sinal
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		log.Printf("[server] shutting down...")
		cancel()
		_ = srv.Close()
	}()

	// coleta erros do acceptLoop
	go func() {
		for err := range srv.Errors() {
			log.Printf("[server] error: %v", err)
		}
	}()

	// teste de broadcast a cada 5s
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

			// goroutine pra ler frames do server (broadcast / server_full / etc)
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

						// se o servidor avisou SERVER_FULL e logo depois fechou/resetou, isso é esperado
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

			// envia frames periodicamente
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
					sf := serverFull
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

			// fecha a conexao e espera o leitor terminar sem poluir o log
			_ = c.Close()
			<-done
			log.Printf("[client %d] done", i)
		}()
	}

	wg.Wait()
}

// helper para erros esperados quando o proprio client fecha a conexao
func isExpectedClientReadClose(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
		return true
	}

	msg := err.Error()
	return strings.Contains(msg, "use of closed network connection")
}

// helper para erros esperados quando o servidor recusa e fecha/reset a conexao
func isExpectedServerRefusalClose(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, io.EOF) {
		return true
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection reset by peer")
}

// helper para erros esperados de escrita quando a conexao ja foi fechada pelo proprio client
func isExpectedClientWriteClose(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, net.ErrClosed) {
		return true
	}

	msg := err.Error()
	return strings.Contains(msg, "use of closed network connection")
}

// helper para erros esperados de escrita quando o servidor recusou a conexao
func isExpectedServerRefusalWrite(err error) bool {
	if err == nil {
		return false
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection reset by peer")
}
