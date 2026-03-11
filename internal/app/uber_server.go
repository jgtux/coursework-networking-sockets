package app

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"coursework-networking-sockets/internal/tcp"
)

// ─── Tipos e estado compartilhado

// DriverStatus motorista livre ou em corrida. (0 = livre / 1 = em corrida)
type DriverStatus int

const (
	StatusFree   DriverStatus = iota // motorista livre
	StatusOnRide                     // motorista em corrida
)

// RideCall é uma chamada de passageiro gerada pelo server
type RideCall struct {
	ID           int
	DistToPickup float64 // km até o passageiro
	RideDistance float64 // km da corrida
	Value        float64 // valor pago
	ExpiresAt    time.Time
	Accepted     bool
	Cancelled    bool
}

// SharedState é a memória compartilhada do servidor
// O sync.Mutex garante que apenas uma thread acesse os dados por vez
type SharedState struct {
	mu            sync.Mutex
	status        DriverStatus
	lastCall      *RideCall
	callCounter   int
	currentRideID int
}

// Estado inicial do servidor
func NewSharedState() *SharedState {
	return &SharedState{status: StatusFree}
}

// Retorna o status atual do motorista
func (s *SharedState) GetStatus() DriverStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

// Atualiza o status do motorista
func (s *SharedState) SetStatus(st DriverStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = st
}

func (s *SharedState) SetLastCall(call *RideCall) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastCall = call
}

func (s *SharedState) GetLastCall() *RideCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastCall
}

func (s *SharedState) NextCallID() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.callCounter++
	return s.callCounter
}

// ─── UberServer

type UberServer struct {
	state *SharedState
}

// RunUberServer ponto de entrada chamado pelo main
func RunUberServer(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("erro ao abrir listener: %w", err)
	}
	defer ln.Close()

	// encerra o listener quando o contexto for cancelado
	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	fmt.Printf("[SERVER] Aguardando conexão em %s...\n", addr)

	// aceita apenas 1 cliente
	conn, err := ln.Accept()
	if err != nil {
		select {
		case <-ctx.Done():
			return nil // encerramento esperado
		default:
			return fmt.Errorf("erro ao aceitar conexão: %w", err)
		}
	}

	client := tcp.NewClient(conn)
	defer client.Close()

	fmt.Printf("[SERVER] Motorista conectado! [%s]\n", time.Now().Format("15:04:05"))

	us := &UberServer{state: NewSharedState()}

	// envia mensagem confirmando conexao
	now := time.Now().Format("15:04:05")
	sendMsg(client, fmt.Sprintf("[%s]: CONECTADO!!", now))
	sendMsg(client, "[INFO] Bem-vindo ao sistema de corridas. Aguardando chamadas...")

	// encerra as duas threads quando o contexto for cancelado
	sessCtx, sessCancel := context.WithCancel(ctx)
	defer sessCancel()

	// Thread 1 do servidor recebe e processa comandos do motorista
	go us.thread1Commands(sessCtx, client, sessCancel)

	// Thread 2 do servidor gerador de eventos/chamadas de passageiros
	go us.thread2EventGenerator(sessCtx, client)

	<-sessCtx.Done()
	fmt.Println("[SERVER] Sessão encerrada.")
	return nil
}

// Thread 1 recebe comandos do motorista e processa
func (us *UberServer) thread1Commands(ctx context.Context, client *tcp.Client, cancel context.CancelFunc) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		frame, err := client.ReadFrame()
		if err != nil {
			fmt.Println("[SERVER] Motorista desconectou.")
			cancel()
			return
		}

		us.handleCommand(string(frame), client, cancel)
	}
}

// Processa cada comando recebido do motorista
func (us *UberServer) handleCommand(cmd string, client *tcp.Client, cancel context.CancelFunc) {
	switch cmd {
	case ":accept":
		call := us.state.GetLastCall()
		if call == nil {
			sendMsg(client, "[RESPOSTA] Nenhuma chamada pendente para aceitar.")
			return
		}
		if call.Accepted {
			sendMsg(client, "[RESPOSTA] Você já aceitou esta corrida.")
			return
		}
		if call.Cancelled {
			sendMsg(client, "[RESPOSTA] Esta chamada já foi cancelada.")
			return
		}
		if time.Now().After(call.ExpiresAt) {
			sendMsg(client, "[RESPOSTA] Tempo esgotado para aceitar esta chamada.")
			return
		}

		call.Accepted = true
		us.state.SetStatus(StatusOnRide)
		us.state.currentRideID = call.ID

		sendMsg(client, fmt.Sprintf("[CONFIRMAÇÃO] Você executou: ACEITAR CORRIDA #%d", call.ID))
		sendMsg(client, fmt.Sprintf("[INFO] Corrida aceita! Dirija %.1f km até o passageiro. Corrida: %.1f km | Valor: R$ %.2f",
			call.DistToPickup, call.RideDistance, call.Value))

	case ":cancel":
		call := us.state.GetLastCall()
		if call == nil || !call.Accepted {
			sendMsg(client, "[RESPOSTA] Nenhuma corrida aceita para cancelar.")
			return
		}
		if call.Cancelled {
			sendMsg(client, "[RESPOSTA] Esta corrida já foi cancelada.")
			return
		}

		call.Cancelled = true
		us.state.SetStatus(StatusFree)
		us.state.SetLastCall(nil)

		sendMsg(client, fmt.Sprintf("[CONFIRMAÇÃO] Você executou: CANCELAR CORRIDA #%d", call.ID))
		sendMsg(client, "[INFO] Corrida cancelada. Você está livre para novas chamadas.")

	case ":status":
		st := us.state.GetStatus()
		call := us.state.GetLastCall()

		statusStr := "LIVRE"
		extra := ""
		if st == StatusOnRide && call != nil {
			statusStr = "EM CORRIDA"
			extra = fmt.Sprintf(" | Corrida #%d | Distância: %.1f km | Valor: R$ %.2f",
				call.ID, call.RideDistance, call.Value)
		}

		sendMsg(client, fmt.Sprintf("[RESPOSTA] Status atual: %s%s", statusStr, extra))

	case ":quit":
		sendMsg(client, "[INFO] Encerrando conexão. Até logo, motorista!")
		cancel()

	default:
		sendMsg(client, fmt.Sprintf("[RESPOSTA] Comando desconhecido: %q. Use :accept, :cancel, :status ou :quit", cmd))
	}
}

// Thread 2 gerador de eventos cria chamadas e  expirações
func (us *UberServer) thread2EventGenerator(ctx context.Context, client *tcp.Client) {
	// intervalo chamadas entre 15 e 20 segundos
	callTimer := time.NewTimer(randomDuration(15, 20))
	defer callTimer.Stop()

	// verifica expiração de chamadas pendentes
	checkTicker := time.NewTicker(1 * time.Second)
	defer checkTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-callTimer.C:
			// só gera nova chamada se o motorista estiver livre
			if us.state.GetStatus() == StatusFree {
				call := us.generateCall()
				us.state.SetLastCall(call)

				sendMsg(client, fmt.Sprintf(
					"\n[NOVA CHAMADA #%d] Passageiro a %.1f km | Corrida: %.1f km | Valor: R$ %.2f | Tempo para aceitar: 15s",
					call.ID, call.DistToPickup, call.RideDistance, call.Value,
				))
			}

			// agenda próxima chamada
			callTimer.Reset(randomDuration(15, 20))

		case <-checkTicker.C:
			us.checkCallExpiry(client)
		}
	}
}

// Verifica se uma chamada pendente expirou e randomiza se deve cancelar ou aumentar o valor e renovar o tempo
func (us *UberServer) checkCallExpiry(client *tcp.Client) {
	call := us.state.GetLastCall()
	if call == nil || call.Accepted || call.Cancelled {
		return
	}

	if !time.Now().After(call.ExpiresAt) {
		return
	}

	// Cancelar ou aumentar o valor e renovar o tempo
	if rand.Intn(2) == 0 {
		call.Cancelled = true
		us.state.SetLastCall(nil)
		sendMsg(client, fmt.Sprintf("[AVISO] CHAMADA #%d: Tempo esgotado. Corrida cancelada.", call.ID))
	} else {
		bonus := rand.Float64()*5 + 2 // bônus entre R$2 e R$7
		call.Value += bonus
		call.ExpiresAt = time.Now().Add(15 * time.Second)
		sendMsg(client, fmt.Sprintf(
			"[VALOR AUMENTADO] CHAMADA #%d: Valor aumentado para R$ %.2f. Mais 15s para aceitar.",
			call.ID, call.Value,
		))
	}
}

// Gera uma nova chamada com distâncias e valor aleatórios
func (us *UberServer) generateCall() *RideCall {
	id := us.state.NextCallID()
	distToPickup := randomFloat(0.5, 8.0)
	rideDistance := randomFloat(2.0, 25.0)
	value := rideDistance*1.5 + randomFloat(2.0, 8.0)

	return &RideCall{
		ID:           id,
		DistToPickup: distToPickup,
		RideDistance: rideDistance,
		Value:        value,
		ExpiresAt:    time.Now().Add(15 * time.Second),
	}
}

// Envia uma mensagem de texto para o cliente via frame TCP
func sendMsg(client *tcp.Client, msg string) {
	_ = client.SendFrame([]byte(msg))
}

func randomFloat(min, max float64) float64 {
	return min + rand.Float64()*(max-min)
}

func randomDuration(minSec, maxSec int) time.Duration {
	sec := minSec + rand.Intn(maxSec-minSec+1)
	return time.Duration(sec) * time.Second
}
