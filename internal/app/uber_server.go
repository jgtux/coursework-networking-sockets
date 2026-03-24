package app

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"coursework-networking-sockets/internal/tcp"
)

// ─── Persistência em JSON ─────────────────────────────────────────────────────

const dataDir = "data"
const dataFile = "data/drivers.json"

// DriverRecord é o registro de um motorista salvo em disco.
// Guarda nome, saldo total acumulado e quando foi a última vez que conectou.
type DriverRecord struct {
	Name        string    `json:"name"`
	TotalEarned float64   `json:"total_earned"`
	LastSeen    time.Time `json:"last_seen"`
}

// DriverStore gerencia a leitura e escrita do arquivo JSON.
// O mutex garante que dois motoristas não salvem ao mesmo tempo,
// o que poderia corromper o arquivo.
type DriverStore struct {
	mu      sync.Mutex
	records map[string]*DriverRecord // chave = nome do motorista
}

// loadStore lê o arquivo JSON do disco e carrega os dados na memória.
// Se o arquivo não existir ainda, começa com um mapa vazio.
func loadStore() (*DriverStore, error) {
	// cria a pasta /data se não existir
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("erro ao criar pasta data: %w", err)
	}

	store := &DriverStore{
		records: make(map[string]*DriverRecord),
	}

	// tenta abrir o arquivo; se não existir, retorna o store vazio
	data, err := os.ReadFile(dataFile)
	if os.IsNotExist(err) {
		return store, nil
	}
	if err != nil {
		return nil, fmt.Errorf("erro ao ler arquivo de dados: %w", err)
	}

	// converte o JSON para o mapa de registros
	if err := json.Unmarshal(data, &store.records); err != nil {
		return nil, fmt.Errorf("erro ao interpretar JSON: %w", err)
	}

	return store, nil
}

// save grava o estado atual da memória no arquivo JSON.
// Chamado toda vez que o saldo de um motorista muda.
func (ds *DriverStore) save() error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	data, err := json.MarshalIndent(ds.records, "", "  ")
	if err != nil {
		return fmt.Errorf("erro ao serializar JSON: %w", err)
	}

	// escreve no arquivo (cria ou sobrescreve)
	return os.WriteFile(dataFile, data, 0644)
}

// getOrCreate retorna o registro de um motorista pelo nome.
// Se o motorista for novo, cria um registro com saldo zero.
func (ds *DriverStore) getOrCreate(name string) *DriverRecord {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	record, exists := ds.records[name]
	if !exists {
		// motorista novo — começa com saldo zero
		record = &DriverRecord{
			Name:        name,
			TotalEarned: 0,
			LastSeen:    time.Now(),
		}
		ds.records[name] = record
	}
	return record
}

// addEarnings soma o valor de uma corrida ao saldo do motorista e salva em disco.
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

// isNameTaken verifica se já existe um motorista conectado com esse nome.
func isNameTaken(name string, sessions *sync.Map) bool {
	_, taken := sessions.Load(name)
	return taken
}

// ─── Estado da sessão de cada motorista ──────────────────────────────────────

// RideStatus representa as etapas de uma corrida.
type RideStatus int

const (
	RideStatusNone     RideStatus = iota // sem corrida ativa
	RideStatusAccepted                   // corrida aceita, ainda não iniciada
	RideStatusStarted                    // corrida em andamento
)

// DriverSession é a "memória viva" de um motorista conectado.
// Cada motorista tem sua própria sessão, isolada das demais.
// O mutex garante acesso seguro entre as duas threads do motorista.
type DriverSession struct {
	mu         sync.Mutex
	name       string
	record     *DriverRecord
	lastCall   *RideCall
	rideStatus RideStatus
}

func NewDriverSession(name string, record *DriverRecord) *DriverSession {
	return &DriverSession{
		name:       name,
		record:     record,
		rideStatus: RideStatusNone,
	}
}

func (ds *DriverSession) GetLastCall() *RideCall {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.lastCall
}

func (ds *DriverSession) SetLastCall(call *RideCall) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.lastCall = call
}

func (ds *DriverSession) GetRideStatus() RideStatus {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.rideStatus
}

func (ds *DriverSession) SetRideStatus(s RideStatus) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.rideStatus = s
}

func (ds *DriverSession) GetTotalEarned() float64 {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.record.TotalEarned
}

// ─── Chamada de passageiro ────────────────────────────────────────────────────

// RideCall representa uma chamada de passageiro gerada pelo servidor.
type RideCall struct {
	ID           int
	DistToPickup float64   // km até o passageiro
	RideDistance float64   // km da corrida
	Value        float64   // valor da corrida
	ExpiresAt    time.Time // prazo para aceitar
	Accepted     bool
	Cancelled    bool
}

// ─── UberServer ──────────────────────────────────────────────────────────────

// UberServer gerencia todas as conexões simultâneas.
// store = dados persistidos em disco
// sessions = motoristas atualmente conectados (sync.Map é seguro para leitura/escrita concorrente)
// callCounter = contador global de chamadas (compartilhado entre todos os motoristas)
type UberServer struct {
	store       *DriverStore
	sessions    sync.Map // chave = nome do motorista, valor = *DriverSession
	counterMu   sync.Mutex
	callCounter int
}

// RunUberServer é o ponto de entrada do servidor, chamado pelo main.
// maxClients define quantos motoristas podem se conectar ao mesmo tempo.
func RunUberServer(ctx context.Context, addr string, maxClients int) error {
	// carrega dados salvos do disco (ou inicia vazio se for a primeira vez)
	store, err := loadStore()
	if err != nil {
		return fmt.Errorf("erro ao carregar dados: %w", err)
	}

	us := &UberServer{store: store}

	// abre a porta TCP para escutar conexões
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("erro ao abrir listener: %w", err)
	}
	defer ln.Close()

	// fecha o listener quando o programa for encerrado (ex: Ctrl+C)
	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	// semáforo: limita o número de motoristas conectados ao mesmo tempo
	// cada slot liberado = uma conexão permitida
	sem := make(chan struct{}, maxClients)

	fmt.Printf("[SERVER] Aguardando conexões em %s (max %d motoristas)...\n", addr, maxClients)

	// loop principal: aceita novas conexões indefinidamente
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil // encerramento esperado
			default:
				return fmt.Errorf("erro ao aceitar conexão: %w", err)
			}
		}

		// tenta ocupar um slot no semáforo
		select {
		case sem <- struct{}{}:
			// há vaga — inicia a sessão do motorista em uma goroutine separada
			go func() {
				defer func() { <-sem }() // libera o slot ao desconectar
				us.handleSession(ctx, conn)
			}()
		default:
			// servidor lotado — avisa e fecha a conexão
			client := tcp.NewClient(conn)
			sendMsg(client, "[SERVIDOR] Limite de motoristas atingido. Tente novamente mais tarde.")
			client.Close()
		}
	}
}

// handleSession gerencia toda a sessão de um motorista conectado.
// Roda em uma goroutine separada por motorista.
func (us *UberServer) handleSession(ctx context.Context, conn net.Conn) {
	client := tcp.NewClient(conn)
	defer client.Close()

	// passo 1: pede o nome de usuário antes de qualquer outra coisa
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

	// verifica se o nome já está em uso por outro motorista conectado
	if isNameTaken(name, &us.sessions) {
		sendMsg(client, fmt.Sprintf("[ERRO] O nome %q já está em uso. Desconectando.", name))
		return
	}

	// busca ou cria o registro do motorista no store
	record := us.store.getOrCreate(name)

	// cria a sessão em memória para este motorista
	session := NewDriverSession(name, record)

	// registra o motorista como conectado
	us.sessions.Store(name, session)
	defer us.sessions.Delete(name) // remove ao desconectar

	fmt.Printf("[SERVER] Motorista %q conectado! [%s]\n", name, time.Now().Format("15:04:05"))

	// envia mensagem de boas-vindas com saldo atual
	now := time.Now().Format("15:04:05")
	sendMsg(client, fmt.Sprintf("[%s]: CONECTADO!!", now))

	if record.TotalEarned == 0 {
		sendMsg(client, fmt.Sprintf("[INFO] Bem-vindo, %s! Saldo inicial: R$ 0,00", name))
	} else {
		sendMsg(client, fmt.Sprintf("[INFO] Bem-vindo de volta, %s! Faturamento total: R$ %.2f", name, record.TotalEarned))
	}

	// contexto para encerrar as duas threads desta sessão juntas
	sessCtx, sessCancel := context.WithCancel(ctx)
	defer sessCancel()

	// Thread 1: recebe e processa comandos do motorista
	go us.thread1Commands(sessCtx, client, session, sessCancel)

	// Thread 2: gera chamadas de passageiros para este motorista
	go us.thread2EventGenerator(sessCtx, client, session)

	// aguarda o encerramento da sessão
	<-sessCtx.Done()
	fmt.Printf("[SERVER] Motorista %q desconectou.\n", name)
}

// thread1Commands processa os comandos enviados pelo motorista.
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

// handleCommand executa a ação correspondente ao comando recebido.
func (us *UberServer) handleCommand(cmd string, client *tcp.Client, session *DriverSession, cancel context.CancelFunc) {
	switch cmd {

	case ":accept":
		// aceita a última chamada recebida — só funciona se houver chamada pendente
		call := session.GetLastCall()
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
		session.SetRideStatus(RideStatusAccepted)

		sendMsg(client, fmt.Sprintf("[CONFIRMAÇÃO] Você executou: ACEITAR CORRIDA #%d", call.ID))
		sendMsg(client, fmt.Sprintf("[INFO] Corrida aceita! Dirija %.1f km até o passageiro. Corrida: %.1f km | Valor: R$ %.2f",
			call.DistToPickup, call.RideDistance, call.Value))
		sendMsg(client, "[INFO] Use :start quando chegar ao passageiro para iniciar a corrida.")

	case ":start":
		// inicia a corrida — só funciona se a corrida foi aceita
		if session.GetRideStatus() != RideStatusAccepted {
			sendMsg(client, "[RESPOSTA] Nenhuma corrida aceita para iniciar. Use :accept primeiro.")
			return
		}

		call := session.GetLastCall()
		session.SetRideStatus(RideStatusStarted)

		sendMsg(client, fmt.Sprintf("[CONFIRMAÇÃO] Você executou: INICIAR CORRIDA #%d", call.ID))
		sendMsg(client, fmt.Sprintf("[INFO] Corrida iniciada! Percurso de %.1f km. Use :finish ao chegar ao destino.", call.RideDistance))

	case ":finish":
		// finaliza a corrida — só funciona se a corrida foi iniciada
		if session.GetRideStatus() != RideStatusStarted {
			sendMsg(client, "[RESPOSTA] Nenhuma corrida em andamento. Use :start primeiro.")
			return
		}

		call := session.GetLastCall()

		// soma o valor ao faturamento do motorista e salva em disco
		if err := us.store.addEarnings(session.name, call.Value); err != nil {
			sendMsg(client, "[ERRO] Falha ao registrar ganhos. Tente novamente.")
			return
		}

		// libera o motorista para novas corridas
		call.Cancelled = true
		session.SetLastCall(nil)
		session.SetRideStatus(RideStatusNone)

		sendMsg(client, fmt.Sprintf("[CONFIRMAÇÃO] Você executou: FINALIZAR CORRIDA #%d", call.ID))
		sendMsg(client, fmt.Sprintf("[INFO] Corrida finalizada! Você ganhou R$ %.2f. Faturamento total: R$ %.2f",
			call.Value, session.GetTotalEarned()))
		sendMsg(client, "[INFO] Você está livre para novas chamadas.")

	case ":cancel":
		// cancela a corrida aceita (antes de iniciar)
		status := session.GetRideStatus()
		if status == RideStatusNone {
			sendMsg(client, "[RESPOSTA] Nenhuma corrida aceita para cancelar.")
			return
		}
		if status == RideStatusStarted {
			sendMsg(client, "[RESPOSTA] Corrida já iniciada. Use :finish para finalizar.")
			return
		}

		call := session.GetLastCall()
		call.Cancelled = true
		session.SetLastCall(nil)
		session.SetRideStatus(RideStatusNone)

		sendMsg(client, fmt.Sprintf("[CONFIRMAÇÃO] Você executou: CANCELAR CORRIDA #%d", call.ID))
		sendMsg(client, "[INFO] Corrida cancelada. Você está livre para novas chamadas.")

	case ":status":
		// mostra status atual do motorista
		status := session.GetRideStatus()
		call := session.GetLastCall()

		var statusStr, extra string
		switch status {
		case RideStatusNone:
			statusStr = "LIVRE"
		case RideStatusAccepted:
			statusStr = "CORRIDA ACEITA (aguardando :start)"
			if call != nil {
				extra = fmt.Sprintf(" | Corrida #%d | Distância: %.1f km | Valor: R$ %.2f",
					call.ID, call.RideDistance, call.Value)
			}
		case RideStatusStarted:
			statusStr = "EM CORRIDA"
			if call != nil {
				extra = fmt.Sprintf(" | Corrida #%d | Distância: %.1f km | Valor: R$ %.2f",
					call.ID, call.RideDistance, call.Value)
			}
		}

		sendMsg(client, fmt.Sprintf("[RESPOSTA] Status: %s%s | Faturamento total: R$ %.2f",
			statusStr, extra, session.GetTotalEarned()))

	case ":quit":
		sendMsg(client, "[INFO] Encerrando conexão. Até logo, motorista!")
		cancel()

	default:
		sendMsg(client, fmt.Sprintf("[RESPOSTA] Comando desconhecido: %q. Use :accept, :start, :finish, :cancel, :status ou :quit", cmd))
	}
}

// thread2EventGenerator gera chamadas de passageiros periodicamente para um motorista.
func (us *UberServer) thread2EventGenerator(ctx context.Context, client *tcp.Client, session *DriverSession) {
	// gera a primeira chamada entre 15 e 20 segundos após conectar
	callTimer := time.NewTimer(randomDuration(15, 20))
	defer callTimer.Stop()

	// verifica expiração de chamadas a cada 1 segundo
	checkTicker := time.NewTicker(1 * time.Second)
	defer checkTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-callTimer.C:
			// só gera chamada se o motorista estiver livre (sem corrida ativa)
			if session.GetRideStatus() == RideStatusNone && session.GetLastCall() == nil {
				call := us.generateCall()
				session.SetLastCall(call)

				sendMsg(client, fmt.Sprintf(
					"\n[NOVA CHAMADA #%d] Passageiro a %.1f km | Corrida: %.1f km | Valor: R$ %.2f | Tempo para aceitar: 15s",
					call.ID, call.DistToPickup, call.RideDistance, call.Value,
				))
			}

			callTimer.Reset(randomDuration(15, 20))

		case <-checkTicker.C:
			us.checkCallExpiry(client, session)
		}
	}
}

// checkCallExpiry verifica se o prazo de uma chamada pendente expirou.
// Se sim, decide aleatoriamente entre cancelar ou aumentar o valor.
func (us *UberServer) checkCallExpiry(client *tcp.Client, session *DriverSession) {
	call := session.GetLastCall()

	// só verifica chamadas não aceitas e não canceladas
	if call == nil || call.Accepted || call.Cancelled {
		return
	}

	if !time.Now().After(call.ExpiresAt) {
		return
	}

	// 50% cancela, 50% aumenta o valor e renova o prazo
	if rand.Intn(2) == 0 {
		call.Cancelled = true
		session.SetLastCall(nil)
		sendMsg(client, fmt.Sprintf("[AVISO] CHAMADA #%d: Tempo esgotado. Corrida cancelada.", call.ID))
	} else {
		bonus := rand.Float64()*5 + 2 // entre R$2 e R$7 de bônus
		call.Value += bonus
		call.ExpiresAt = time.Now().Add(15 * time.Second)
		sendMsg(client, fmt.Sprintf(
			"[VALOR AUMENTADO] CHAMADA #%d: Valor aumentado para R$ %.2f. Mais 15s para aceitar.",
			call.ID, call.Value,
		))
	}
}

// generateCall cria uma nova chamada com valores aleatórios.
func (us *UberServer) generateCall() *RideCall {
	us.counterMu.Lock()
	us.callCounter++
	id := us.callCounter
	us.counterMu.Unlock()

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

// ─── Helpers ─────────────────────────────────────────────────────────────────

// sendMsg envia uma mensagem de texto ao cliente via frame TCP.
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

// pathExists verifica se um caminho existe no sistema de arquivos.
func pathExists(path string) bool {
	_, err := os.Stat(filepath.Clean(path))
	return !os.IsNotExist(err)
}
