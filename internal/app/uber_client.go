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

// RunUberClient é o ponto de entrada do cliente, chamado pelo main.
// Conecta ao servidor e gerencia as duas threads da sessão.
func RunUberClient(ctx context.Context, addr string) error {
	// net.Dial abre a conexão TCP com o servidor
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("erro ao conectar em %s: %w", addr, err)
	}

	// tcp.NewClient envolve a conexão com a camada de framing do pacote tcp
	client := tcp.NewClient(conn)
	defer client.Close()

	fmt.Printf("[CLIENT] Conectado ao servidor %s\n", addr)

	// contexto para encerrar as duas threads juntas quando necessário
	sessCtx, sessCancel := context.WithCancel(ctx)
	defer sessCancel()

	// Thread 2: recebe mensagens do servidor e imprime na tela de forma assíncrona
	go thread2Receiver(sessCtx, client, sessCancel)

	// Thread 1: lê comandos do teclado e os envia ao servidor
	// o primeiro envio é sempre o nome do usuário
	thread1Input(sessCtx, client, sessCancel)

	return nil
}

// thread1Input lê o teclado e envia dados ao servidor em loop infinito.
// O primeiro dado enviado é sempre o nome do motorista (identificação).
func thread1Input(ctx context.Context, client *tcp.Client, cancel context.CancelFunc) {
	scanner := bufio.NewScanner(os.Stdin)

	// passo 1: identifica o motorista antes de mostrar os comandos
	fmt.Print("Nome de usuário: ")
	if !scanner.Scan() {
		cancel()
		return
	}

	name := strings.TrimSpace(scanner.Text())
	if name == "" {
		fmt.Println("[ERRO] Nome não pode ser vazio.")
		cancel()
		return
	}

	// envia o nome ao servidor para identificação e verificação de duplicidade
	if err := client.SendFrame([]byte(name)); err != nil {
		fmt.Printf("[ERRO] Falha ao enviar nome: %v\n", err)
		cancel()
		return
	}

	// exibe o menu de comandos disponíveis
	printHelp()

	// passo 2: loop normal de comandos
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

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		cmd := parseCommand(line)
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

// thread2Receiver recebe mensagens do servidor e as imprime na tela de forma assíncrona.
// Permite que notificações de novas chamadas apareçam mesmo enquanto o motorista
// não está digitando nada.
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
				// encerramento esperado — não loga erro
			default:
				fmt.Printf("\n[SERVIDOR DESCONECTOU] %v\n", err)
				cancel()
			}
			return
		}

		// \n antes garante que a mensagem não sobreponha o prompt "> "
		fmt.Printf("\n%s\n", string(frame))
	}
}

// parseCommand valida o comando digitado.
// Retorna o comando em minúsculas ou string vazia se inválido.
func parseCommand(input string) string {
	lower := strings.ToLower(strings.TrimSpace(input))

	switch lower {
	case ":accept", ":start", ":finish", ":cancel", ":status", ":quit":
		return lower
	default:
		return ""
	}
}

// printHelp exibe o menu de comandos disponíveis.
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
