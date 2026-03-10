package app

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"strings"

	"coursework-networking-sockets/internal/tcp"
)

// RunUberClient é o ponto de entrada do cliente chamado pelo main
func RunUberClient(ctx context.Context, addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("erro ao conectar em %s: %w", addr, err)
	}

	client := tcp.NewClient(conn)
	defer client.Close()

	fmt.Printf("[CLIENT] Conectado ao servidor %s\n", addr)

	// contexto para encerrar as duas threads juntas
	sessCtx, sessCancel := context.WithCancel(ctx)
	defer sessCancel()

	// Thread 2 recebe mensagens do servidor e imprime na tela
	go thread2Receiver(sessCtx, client, sessCancel)

	// Thread 1 lê comandos do teclado e envia ao servidor
	thread1Input(sessCtx, client, sessCancel)

	return nil
}

// Thread 1 lê do teclado e envia comandos pelo socket
func thread1Input(ctx context.Context, client *tcp.Client, cancel context.CancelFunc) {
	scanner := bufio.NewScanner(os.Stdin)

	printHelp()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		fmt.Print("> ")

		// bloqueia até o usuário pressionar Enter
		if !scanner.Scan() {
			cancel()
			return
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// valida o comando digitado
		cmd := parseCommand(line)
		if cmd == "" {
			fmt.Println("[ERRO] Comando inválido. Use :accept, :cancel, :status ou :quit")
			continue
		}

		// envia o comando ao servidor
		if err := client.SendFrame([]byte(cmd)); err != nil {
			fmt.Printf("[ERRO] Falha ao enviar comando: %v\n", err)
			cancel()
			return
		}

		// encerra o cliente localmente após :quit
		if cmd == ":quit" {
			cancel()
			return
		}
	}
}

// Thread 2 recebe frames do servidor e imprime na tela
func thread2Receiver(ctx context.Context, client *tcp.Client, cancel context.CancelFunc) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		frame, err := client.ReadFrame()
		if err != nil {
			select {
			case <-ctx.Done():
				// encerramento esperado
			default:
				fmt.Printf("\n[SERVIDOR DESCONECTOU] %v\n", err)
				cancel()
			}
			return
		}

		fmt.Printf("\n%s\n", string(frame))
	}
}

// Valida o comando digitado ou string vazia se for inválido
func parseCommand(input string) string {
	lower := strings.ToLower(strings.TrimSpace(input))

	switch lower {
	case ":accept", ":cancel", ":status", ":quit":
		return lower
	default:
		return ""
	}
}

func printHelp() {
	fmt.Println("─────────────────────────────────────────")
	fmt.Println("  SISTEMA DE CORRIDAS — MODO MOTORISTA")
	fmt.Println("─────────────────────────────────────────")
	fmt.Println("  :accept  → Aceita a última chamada recebida")
	fmt.Println("  :cancel  → Cancela a última corrida aceita")
	fmt.Println("  :status  → Exibe seu status atual")
	fmt.Println("  :quit    → Desconecta do sistema")
	fmt.Println("─────────────────────────────────────────")
}
