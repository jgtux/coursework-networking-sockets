package app

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"

	"coursework-networking-sockets/internal/tcp"
)

func RunUberClient(ctx context.Context, addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("erro ao conectar em %s: %w", addr, err)
	}

	client := tcp.NewClient(conn)
	defer client.Close()

	fmt.Printf("[CLIENT] Conectado ao servidor %s\n", addr)

	if err := login(client); err != nil {
		if errors.Is(err, tcp.ErrServerFull) {
			return fmt.Errorf("servidor lotado")
		}
		return err
	}

	sessCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go recvLoop(sessCtx, client, cancel)
	readInput(sessCtx, client, cancel)

	return nil
}

func login(client *tcp.Client) error {
	frame, err := client.ReadFrame()
	if err != nil {
		return fmt.Errorf("erro ao receber mensagem inicial: %w", err)
	}

	msg := strings.TrimSpace(string(frame))
	if msg == string(tcp.ErrServerFull) {
		return tcp.ErrServerFull
	}

	fmt.Println(msg)

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("Nome de usuário: ")
	if !scanner.Scan() {
		return fmt.Errorf("entrada encerrada")
	}

	name := strings.TrimSpace(scanner.Text())
	if name == "" {
		return fmt.Errorf("nome não pode ser vazio")
	}

	if err := client.SendFrame([]byte(name)); err != nil {
		return fmt.Errorf("falha ao enviar nome: %w", err)
	}

	printHelp()
	return nil
}

func readInput(ctx context.Context, client *tcp.Client, cancel context.CancelFunc) {
	scanner := bufio.NewScanner(os.Stdin)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		fmt.Print("> ")

		if !scanner.Scan() {
			cancel()
			return
		}

		cmd := parseCommand(scanner.Text())
		if cmd == "" {
			fmt.Println("[ERRO] Comando inválido. Use :accept, :start, :finish, :cancel, :status ou :quit")
			continue
		}

		if err := client.SendFrame([]byte(cmd)); err != nil {
			fmt.Printf("[ERRO] Falha ao enviar comando: %v\n", err)
			cancel()
			return
		}

		if cmd == ":quit" {
			cancel()
			return
		}
	}
}

func recvLoop(ctx context.Context, client *tcp.Client, cancel context.CancelFunc) {
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
			default:
				fmt.Printf("\n[SERVIDOR DESCONECTOU] %v\n", err)
				cancel()
			}
			return
		}

		fmt.Printf("\n%s\n", string(frame))
	}
}

func parseCommand(input string) string {
	switch strings.ToLower(strings.TrimSpace(input)) {
	case ":accept", ":start", ":finish", ":cancel", ":status", ":quit":
		return strings.ToLower(strings.TrimSpace(input))
	default:
		return ""
	}
}

func printHelp() {
	fmt.Println("─────────────────────────────────────────")
	fmt.Println("  SISTEMA DE CORRIDAS — MODO MOTORISTA")
	fmt.Println("─────────────────────────────────────────")
	fmt.Println("  :accept  → Aceita a última chamada recebida")
	fmt.Println("  :start   → Inicia a corrida aceita")
	fmt.Println("  :finish  → Finaliza a corrida em andamento")
	fmt.Println("  :cancel  → Cancela a corrida aceita")
	fmt.Println("  :status  → Exibe seu status e faturamento")
	fmt.Println("  :quit    → Desconecta do sistema")
	fmt.Println("─────────────────────────────────────────")
}
