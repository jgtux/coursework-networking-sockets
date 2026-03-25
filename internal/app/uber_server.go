package app

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"coursework-networking-sockets/internal/tcp"
)

// FSM: Estados da sessão do motorista

// SessionState representa cada estado possível na máquina de estados da sessão.
type SessionState int

const (
	// StateWaitingName: conexão aberta, aguardando o motorista digitar o nome
	StateWaitingName SessionState = iota

	// StateIdle: motorista livre, sem chamada ativa
	StateIdle

	// StateCallPending: uma chamada foi enviada ao motorista, aguardando :accept
	StateCallPending

	// StateAccepted: motorista aceitou a chamada, aguardando :start
	StateAccepted

	// StateStarted: corrida em andamento, aguardando :finish
	StateStarted
)

func (s SessionState) String() string {
	switch s {
	case StateWaitingName:
		return "AGUARDANDO IDENTIFICAÇÃO"
	case StateIdle:
		return "LIVRE"
	case StateCallPending:
		return "CHAMADA PENDENTE"
	case StateAccepted:
		return "CORRIDA ACEITA (aguardando :start)"
	case StateStarted:
		return "EM CORRIDA"
	default:
		return "DESCONHECIDO"
	}
}

// Salvando dados do motoristas em JSON

const dataDir = "data"
const dataFile = "data/drivers.json"

type DriverRecord struct {
	Name        string    `json:"name"`
	TotalEarned float64   `json:"total_earned"`
	LastSeen    time.Time `json:"last_seen"`
}

type DriverStore struct {
	mu      sync.Mutex
	records map[string]*DriverRecord
}

func loadStore() (*DriverStore, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("erro ao criar pasta data: %w", err)
	}

	store := &DriverStore{records: make(map[string]*DriverRecord)}

	data, err := os.ReadFile(dataFile)
	if os.IsNotExist(err) {
		return store, nil
	}
	if err != nil {
		return nil, fmt.Errorf("erro ao ler arquivo de dados: %w", err)
	}

	if err := json.Unmarshal(data, &store.records); err != nil {
		return nil, fmt.Errorf("erro ao interpretar JSON: %w", err)
	}

	return store, nil
}

func (ds *DriverStore) save() error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	data, err := json.MarshalIndent(ds.records, "", "  ")
	if err != nil {
		return fmt.Errorf("erro ao serializar JSON: %w", err)
	}

	return os.WriteFile(dataFile, data, 0644)
}

func (ds *DriverStore) getOrCreate(name string) *DriverRecord {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	record, exists := ds.records[name]
	if !exists {
		record = &DriverRecord{Name: name, TotalEarned: 0, LastSeen: time.Now()}
		ds.records[name] = record
	}
	return record
}

func (ds *DriverStore) addEarnings(name string, value float64) error {
	ds.mu.Lock()
	record, exists := ds.records[name]
	if !exists {
		ds.mu.Unlock()
		return fmt.Errorf("motorista %q não encontrado", name)
	}
	record.TotalEarned += value
	record.LastSeen = time.Now()
	ds.mu.Unlock()

	return ds.save()
}

// Chamada global de passageiro

// GlobalCall representa uma chamada ativa no sistema.
// É compartilhada entre todos os motoristas — apenas um pode aceitar.
type GlobalCall struct {
	mu           sync.Mutex
	ID           int
	DistToPickup float64
	RideDistance float64
	Value        float64
	ExpiresAt    time.Time
	// takenBy guarda o nome do motorista que aceitou.
	// string vazia = ninguém aceitou ainda.
	takenBy string
}

// tryTake tenta reservar a corrida para o motorista informado.
// Retorna true se conseguiu (foi o primeiro a aceitar).
// Retorna false se outro motorista já tinha aceitado.
func (gc *GlobalCall) tryTake(driverName string) bool {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	if gc.takenBy != "" {
		// já foi aceita por outro motorista
		return false
	}

	gc.takenBy = driverName
	return true
}

func (gc *GlobalCall) isTaken() bool {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	return gc.takenBy != ""
}

// Sessão do motorista (FSM)

type DriverSession struct {
	mu     sync.Mutex
	state  SessionState
	name   string
	record *DriverRecord
	client *tcp.Client
	// currentCall aponta para a GlobalCall que está associada a esta sessão.
	// É nil quando o motorista está em StateIdle ou StateWaitingName.
	currentCall *GlobalCall
}

func newDriverSession(client *tcp.Client) *DriverSession {
	return &DriverSession{
		state:  StateWaitingName,
		client: client,
	}
}

func (ds *DriverSession) transition(next SessionState) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.state = next
}

func (ds *DriverSession) getState() SessionState {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.state
}

func (ds *DriverSession) getCurrentCall() *GlobalCall {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.currentCall
}

func (ds *DriverSession) setCurrentCall(call *GlobalCall) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.currentCall = call
}

func (ds *DriverSession) getTotalEarned() float64 {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.record.TotalEarned
}

//UberServer

// UberServer gerencia todas as sessões e a Thread 2 global.
type UberServer struct {
	store       *DriverStore
	sessions    sync.Map // nome → *DriverSession
	counterMu   sync.Mutex
	callCounter int

	// activeCall é a chamada global atual sendo disputada pelos motoristas.
	// É nil quando não há chamada ativa no momento.
	activeCallMu sync.Mutex
	activeCall   *GlobalCall
}

// RunUberServer é o ponto de entrada do servidor.
func RunUberServer(ctx context.Context, addr string, maxClients int) error {
	store, err := loadStore()
	if err != nil {
		return fmt.Errorf("erro ao carregar dados: %w", err)
	}

	sem := make(chan struct{}, maxClients)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("erro ao abrir listener: %w", err)
	}
	defer ln.Close()

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	us := &UberServer{store: store}

	// Thread 2 global: go routine que gera e distribui chamadas para todos os motoristas livres simultaneamente
	go us.thread2GlobalEventGenerator(ctx)

	fmt.Printf("[SERVER] Aguardando conexões em %s (max %d motoristas)...\n", addr, maxClients)

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				return fmt.Errorf("erro ao aceitar conexão: %w", err)
			}
		}

		select {
		case sem <- struct{}{}:
			go func() {
				defer func() { <-sem }()
				client := tcp.NewClient(conn)
				us.handleSession(ctx, client)
			}()
		default:
			tmp := tcp.NewClient(conn)
			sendMsg(tmp, "[SERVIDOR] Limite de motoristas atingido. Tente novamente mais tarde.")
			tmp.Close()
		}
	}
}

// handleSession gerencia o ciclo de vida de um motorista conectado.
func (us *UberServer) handleSession(ctx context.Context, client *tcp.Client) {
	defer client.Close()

	session := newDriverSession(client)

	// Transição: StateWaitingName para StateIdle
	sendMsg(client, "Digite seu nome de usuário:")

	nameFrame, err := client.ReadFrame()
	if err != nil {
		return
	}

	name := string(nameFrame)
	if name == "" {
		sendMsg(client, "[ERRO] Nome inválido. Desconectando.")
		return
	}

	if _, taken := us.sessions.Load(name); taken {
		sendMsg(client, fmt.Sprintf("[ERRO] O nome %q já está em uso. Desconectando.", name))
		return
	}

	session.name = name
	session.record = us.store.getOrCreate(name)

	us.sessions.Store(name, session)
	defer us.sessions.Delete(name)

	session.transition(StateIdle)

	fmt.Printf("[SERVER] Motorista %q conectado! [%s]\n", name, time.Now().Format("15:04:05"))

	now := time.Now().Format("15:04:05")
	sendMsg(client, fmt.Sprintf("[%s]: CONECTADO!!", now))
	if session.record.TotalEarned == 0 {
		sendMsg(client, fmt.Sprintf("[INFO] Bem-vindo, %s! Saldo inicial: R$ 0,00", name))
	} else {
		sendMsg(client, fmt.Sprintf("[INFO] Bem-vindo de volta, %s! Faturamento total: R$ %.2f",
			name, session.record.TotalEarned))
	}

	sessCtx, sessCancel := context.WithCancel(ctx)
	defer sessCancel()

	// Thread 1 dedicada ao motorista: processa seus comandos
	go us.thread1Commands(sessCtx, client, session, sessCancel)

	<-sessCtx.Done()
	fmt.Printf("[SERVER] Motorista %q desconectou.\n", name)
}

// thread1Commands processa comandos do motorista e dispara transições na FSM.
func (us *UberServer) thread1Commands(ctx context.Context, client *tcp.Client, session *DriverSession, cancel context.CancelFunc) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		frame, err := client.ReadFrame()
		if err != nil {
			fmt.Printf("[SERVER] Motorista %q desconectou inesperadamente.\n", session.name)
			cancel()
			return
		}

		us.handleCommand(string(frame), client, session, cancel)
	}
}

// handleCommand executa o comando respeitando as transições válidas da FSM.
func (us *UberServer) handleCommand(cmd string, client *tcp.Client, session *DriverSession, cancel context.CancelFunc) {
	state := session.getState()

	switch cmd {

	case ":accept":
		// transição válida: StateCallPending → StateAccepted
		// mas só se conseguir reservar a corrida (tryTake)
		if state != StateCallPending {
			sendMsg(client, "[RESPOSTA] Nenhuma chamada pendente para aceitar.")
			return
		}

		call := session.getCurrentCall()
		if call == nil {
			// chamada foi cancelada por timeout antes de aceitar
			session.transition(StateIdle)
			sendMsg(client, "[RESPOSTA] A chamada expirou antes de ser aceita.")
			return
		}

		if time.Now().After(call.ExpiresAt) {
			session.transition(StateIdle)
			session.setCurrentCall(nil)
			sendMsg(client, "[RESPOSTA] Tempo esgotado para aceitar esta chamada.")
			return
		}

		// disputa: tenta ser o primeiro a aceitar
		if !call.tryTake(session.name) {
			// outro motorista aceitou primeiro — volta para IDLE silenciosamente
			session.setCurrentCall(nil)
			session.transition(StateIdle)
			return
		}

		// conseguiu — avança para StateAccepted
		session.transition(StateAccepted)

		sendMsg(client, fmt.Sprintf("[CONFIRMAÇÃO] Você executou: ACEITAR CORRIDA #%d", call.ID))
		sendMsg(client, fmt.Sprintf("[INFO] Corrida aceita! Dirija %.1f km até o passageiro. Corrida: %.1f km | Valor: R$ %.2f",
			call.DistToPickup, call.RideDistance, call.Value))
		sendMsg(client, "[INFO] Use :start quando chegar ao passageiro.")

	case ":start":
		// transição válida: StateAccepted → StateStarted
		if state != StateAccepted {
			sendMsg(client, "[RESPOSTA] Nenhuma corrida aceita para iniciar. Use :accept primeiro.")
			return
		}

		call := session.getCurrentCall()
		session.transition(StateStarted)

		sendMsg(client, fmt.Sprintf("[CONFIRMAÇÃO] Você executou: INICIAR CORRIDA #%d", call.ID))
		sendMsg(client, fmt.Sprintf("[INFO] Corrida iniciada! Percurso de %.1f km. Use :finish ao chegar ao destino.", call.RideDistance))

	case ":finish":
		// transição válida: StateStarted → StateIdle
		if state != StateStarted {
			sendMsg(client, "[RESPOSTA] Nenhuma corrida em andamento. Use :start primeiro.")
			return
		}

		call := session.getCurrentCall()

		if err := us.store.addEarnings(session.name, call.Value); err != nil {
			sendMsg(client, "[ERRO] Falha ao registrar ganhos. Tente novamente.")
			return
		}

		session.setCurrentCall(nil)
		session.transition(StateIdle)

		sendMsg(client, fmt.Sprintf("[CONFIRMAÇÃO] Você executou: FINALIZAR CORRIDA #%d", call.ID))
		sendMsg(client, fmt.Sprintf("[INFO] Corrida finalizada! Você ganhou R$ %.2f. Faturamento total: R$ %.2f",
			call.Value, session.getTotalEarned()))
		sendMsg(client, "[INFO] Você está livre para novas chamadas.")

	case ":cancel":
		// transição válida: StateAccepted → StateIdle
		if state != StateAccepted {
			if state == StateStarted {
				sendMsg(client, "[RESPOSTA] Corrida já iniciada. Use :finish para finalizar.")
			} else {
				sendMsg(client, "[RESPOSTA] Nenhuma corrida aceita para cancelar.")
			}
			return
		}

		call := session.getCurrentCall()
		session.setCurrentCall(nil)
		session.transition(StateIdle)

		sendMsg(client, fmt.Sprintf("[CONFIRMAÇÃO] Você executou: CANCELAR CORRIDA #%d", call.ID))
		sendMsg(client, "[INFO] Corrida cancelada. Você está livre para novas chamadas.")

	case ":status":
		call := session.getCurrentCall()
		extra := ""
		if call != nil {
			extra = fmt.Sprintf(" | Corrida #%d | Distância: %.1f km | Valor: R$ %.2f",
				call.ID, call.RideDistance, call.Value)
		}
		sendMsg(client, fmt.Sprintf("[RESPOSTA] Status: %s%s | Faturamento total: R$ %.2f",
			state.String(), extra, session.getTotalEarned()))

	case ":quit":
		sendMsg(client, "[INFO] Encerrando conexão. Até logo, motorista!")
		cancel()

	default:
		sendMsg(client, fmt.Sprintf("[RESPOSTA] Comando desconhecido: %q. Use :accept, :start, :finish, :cancel, :status ou :quit", cmd))
	}
}

// thread2GlobalEventGenerator é a única goroutine que gera chamadas.
// Distribui cada chamada para TODOS os motoristas em StateIdle simultaneamente.
// Quando um aceita, os outros voltam para StateIdle silenciosamente via tryTake.
func (us *UberServer) thread2GlobalEventGenerator(ctx context.Context) {
	callTimer := time.NewTimer(randomDuration(15, 20))
	defer callTimer.Stop()

	checkTicker := time.NewTicker(1 * time.Second)
	defer checkTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-callTimer.C:
			us.generateAndDispatch()
			callTimer.Reset(randomDuration(15, 20))

		case <-checkTicker.C:
			us.checkActiveCallExpiry()
		}
	}
}

// generateAndDispatch gera uma nova chamada e a envia para todos os motoristas livres.
// Só gera se não houver chamada ativa no momento.
func (us *UberServer) generateAndDispatch() {
	us.activeCallMu.Lock()
	// se já há uma chamada ativa (pendente), não gera outra
	if us.activeCall != nil && !us.activeCall.isTaken() {
		us.activeCallMu.Unlock()
		return
	}

	// gera nova chamada global
	us.counterMu.Lock()
	us.callCounter++
	id := us.callCounter
	us.counterMu.Unlock()

	call := &GlobalCall{
		ID:           id,
		DistToPickup: randomFloat(0.5, 8.0),
		RideDistance: randomFloat(2.0, 25.0),
		ExpiresAt:    time.Now().Add(15 * time.Second),
	}
	call.Value = call.RideDistance*1.5 + randomFloat(2.0, 8.0)
	us.activeCall = call
	us.activeCallMu.Unlock()

	// distribui para todos os motoristas em StateIdle
	count := 0
	us.sessions.Range(func(_, val any) bool {
		session := val.(*DriverSession)
		if session.getState() == StateIdle {
			// transição: StateIdle → StateCallPending
			session.setCurrentCall(call)
			session.transition(StateCallPending)

			sendMsg(session.client, fmt.Sprintf(
				"\n[NOVA CHAMADA #%d] Passageiro a %.1f km | Corrida: %.1f km | Valor: R$ %.2f | Tempo para aceitar: 15s",
				call.ID, call.DistToPickup, call.RideDistance, call.Value,
			))
			count++
		}
		return true
	})

	if count == 0 {
		// nenhum motorista livre — descarta a chamada
		us.activeCallMu.Lock()
		us.activeCall = nil
		us.activeCallMu.Unlock()
	}
}

// checkActiveCallExpiry verifica se a chamada ativa expirou.
// Retorna os motoristas em StateCallPending para StateIdle e decide se cancela ou aumenta o valor.

func (us *UberServer) checkActiveCallExpiry() {
	us.activeCallMu.Lock()
	call := us.activeCall
	us.activeCallMu.Unlock()

	if call == nil || call.isTaken() {
		return
	}

	if !time.Now().After(call.ExpiresAt) {
		return
	}

	// 50% cancela, 50% aumenta o valor e renova o prazo
	if rand.Intn(2) == 0 {
		// cancela: volta todos os motoristas em StateCallPending para StateIdle
		us.sessions.Range(func(_, val any) bool {
			session := val.(*DriverSession)
			if session.getState() == StateCallPending {
				call := session.getCurrentCall()
				if call != nil {
					sendMsg(session.client, fmt.Sprintf("[AVISO] CHAMADA #%d: Tempo esgotado. Corrida cancelada.", call.ID))
				}
				session.setCurrentCall(nil)
				session.transition(StateIdle)
			}
			return true
		})

		us.activeCallMu.Lock()
		us.activeCall = nil
		us.activeCallMu.Unlock()

	} else {
		// aumenta o valor e renova o prazo para todos ainda pendentes
		bonus := rand.Float64()*5 + 2
		call.mu.Lock()
		call.Value += bonus
		call.ExpiresAt = time.Now().Add(15 * time.Second)
		newValue := call.Value
		call.mu.Unlock()

		us.sessions.Range(func(_, val any) bool {
			session := val.(*DriverSession)
			if session.getState() == StateCallPending {
				sendMsg(session.client, fmt.Sprintf(
					"[VALOR AUMENTADO] CHAMADA #%d: Valor aumentado para R$ %.2f. Mais 15s para aceitar.",
					call.ID, newValue,
				))
			}
			return true
		})
	}
}

// Helpers

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
