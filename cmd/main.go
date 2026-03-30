package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"coursework-networking-sockets/internal/app"
)

func main() {
	mode := flag.String("mode", "server", "server|client|uber-server|uber-client")
	addr := flag.String("addr", "127.0.0.1:9000", "ip:porta")
	maxClients := flag.Int("max", 5, "max motoristas simultâneos (uber-server)")


	flag.Parse()

	// contexto global cancelado quando o programa recebe Ctrl+C
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
		if err := app.RunUberServer(ctx, *addr, *maxClients); err != nil {
			log.Fatalf("uber-server: %v", err)
		}

	case "uber-client":
		if err := app.RunUberClient(ctx, *addr); err != nil {
			log.Fatalf("uber-client: %v", err)
		}

	default:
		log.Fatalf("invalid mode: %s (use uber-server|uber-client)", *mode)
	}
}


