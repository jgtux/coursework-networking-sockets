package app

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"coursework-networking-sockets/internal/tcp"
)

// Maquina de estados finita (FSM)
type SessionState int

const (
	StateIdle SessionState = iota
	StateCallPending
	StateAccepted
	StateStarted
)

func (s SessionState) String() string {
	switch s {
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

// Armazenamento dos dados dos motoristas com sqlite
const (
	dataDir  = "data"
	dataFile = "data/drivers.db"
)

type DriverRecord struct {
	Name        string
	TotalEarned float64
	LastSeen    time.Time
}

type DriverStore struct {
	db *sql.DB
}

func loadStore() (*DriverStore, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("erro ao criar pasta data: %w", err)
	}

	db, err := sql.Open("sqlite", dataFile) //abre e cria os data drivers
	if err != nil {
		return nil, fmt.Errorf("erro ao abrir banco sqlite: %w", err)
	}

	ds := &DriverStore{db: db}

	if err := ds.init(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return ds, nil
}

func (ds *DriverStore) init() error {
	const schema = `
	CREATE TABLE IF NOT EXISTS drivers (
		name TEXT PRIMARY KEY,
		total_earned REAL NOT NULL DEFAULT 0,
		last_seen TEXT NOT NULL
	);`

	if _, err := ds.db.Exec(schema); err != nil {
		return fmt.Errorf("erro ao criar schema sqlite: %w", err)
	}

	return nil
}

func (ds *DriverStore) Close() error {
	if ds == nil || ds.db == nil {
		return nil
	}
	return ds.db.Close()
}

func (ds *DriverStore) getDriver(name string) (DriverRecord, error) {
	var rec DriverRecord
	var lastSeenRaw string

	err := ds.db.QueryRow(`
		SELECT name, total_earned, last_seen
		FROM drivers
		WHERE name = ?
	`, name).Scan(&rec.Name, &rec.TotalEarned, &lastSeenRaw)
	if err != nil {
		if err == sql.ErrNoRows {
			return DriverRecord{}, fmt.Errorf("motorista %q não encontrado", name)
		}
		return DriverRecord{}, fmt.Errorf("erro ao buscar motorista: %w", err)
	}

	t, err := time.Parse(time.RFC3339Nano, lastSeenRaw)
	if err != nil {
		return DriverRecord{}, fmt.Errorf("erro ao converter last_seen: %w", err)
	}
	rec.LastSeen = t

	return rec, nil
}

func (ds *DriverStore) ensureDriver(name string) (DriverRecord, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)

	_, err := ds.db.Exec(`
		INSERT INTO drivers (name, total_earned, last_seen)
		VALUES (?, 0, ?)
		ON CONFLICT(name) DO NOTHING
	`, name, now)
	if err != nil {
		return DriverRecord{}, fmt.Errorf("erro ao garantir motorista: %w", err)
	}

	return ds.getDriver(name)
}

func (ds *DriverStore) totalEarned(name string) float64 {
	var total float64

	err := ds.db.QueryRow(`
		SELECT total_earned
		FROM drivers
		WHERE name = ?
	`, name).Scan(&total)
	if err != nil {
		return 0
	}

	return total
}

func (ds *DriverStore) addEarnings(name string, value float64) (float64, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)

	res, err := ds.db.Exec(`
		UPDATE drivers
		SET total_earned = total_earned + ?, last_seen = ?
		WHERE name = ?
	`, value, now, name)
	if err != nil {
		return 0, fmt.Errorf("erro ao atualizar ganhos: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("erro ao verificar linhas afetadas: %w", err)
	}
	if rows == 0 {
		return 0, fmt.Errorf("motorista %q não encontrado", name)
	}

	var total float64
	err = ds.db.QueryRow(`
		SELECT total_earned
		FROM drivers
		WHERE name = ?
	`, name).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("erro ao buscar faturamento atualizado: %w", err)
	}

	return total, nil
}

// Chamada pra corrida
type Call struct {
	ID           int
	DistToPickup float64
	RideDistance float64
	Value        float64
	ExpiresAt    time.Time
	ValueBumped  bool
}

// Sessao de um motorista
type DriverSession struct {
	serverKey   string
	name        string
	state       SessionState
	client      *tcp.Client
	currentCall *Call
}

// Server struct
type UberServer struct {
	mu         sync.Mutex
	store      *DriverStore
	maxClients int
	sessions   map[string]*DriverSession
	nextCallID int
	activeCall *Call
}

// Funções auxiliares para terminal administrativo/testes
type sessionSnapshot struct {
	Name  string
	State string
	Call  *Call
}

// Mostra nome dos motoristas
func (us *UberServer) connectedNames() []string {
	us.mu.Lock()
	defer us.mu.Unlock()

	names := make([]string, 0, len(us.sessions))
	for name := range us.sessions {
		names = append(names, name)
	}
	return names
}

// Numero de sessões ativas
func (us *UberServer) connectedCount() int {
	us.mu.Lock()
	defer us.mu.Unlock()
	return len(us.sessions)
}

// Vaga livre
func (us *UberServer) availableSlots() int {
	used := us.connectedCount()
	free := us.maxClients - used
	if free < 0 {
		return 0
	}
	return free
}

// Estado das sessões
func (us *UberServer) sessionSnapshots() []sessionSnapshot {
	us.mu.Lock()
	defer us.mu.Unlock()

	out := make([]sessionSnapshot, 0, len(us.sessions))
	for _, s := range us.sessions {
		snap := sessionSnapshot{
			Name:  s.name,
			State: s.state.String(),
		}
		if s.currentCall != nil {
			c := *s.currentCall
			snap.Call = &c
		}
		out = append(out, snap)
	}
	return out
}

// Chamada ativa
func (us *UberServer) activeCallSnapshot() *Call {
	us.mu.Lock()
	defer us.mu.Unlock()

	if us.activeCall == nil {
		return nil
	}
	c := *us.activeCall
	return &c
}

// Terminal administrativo para monitorar o servidor
func (us *UberServer) adminConsole(ctx context.Context) {
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("[ADMIN] Console iniciado. Use :help.")

	for {
		select {
		case <-ctx.Done():
			fmt.Println("[ADMIN] Console encerrado.")
			return
		default:
		}

		fmt.Print("[ADMIN] > ")
		if !scanner.Scan() {
			fmt.Println("[ADMIN] Entrada encerrada.")
			return
		}

		cmd := strings.TrimSpace(strings.ToLower(scanner.Text()))

		switch cmd {
		case ":help":
			fmt.Println("Comandos:")
			fmt.Println("  :clients   -> lista motoristas conectados")
			fmt.Println("  :slots     -> mostra vagas usadas/livres")
			fmt.Println("  :state     -> mostra estado das sessões")
			fmt.Println("  :active    -> mostra a chamada ativa")
			fmt.Println("  :quitadmin -> encerra o console admin")

		case ":clients":
			names := us.connectedNames()
			if len(names) == 0 {
				fmt.Println("[ADMIN] Nenhum motorista conectado.")
				continue
			}
			fmt.Printf("[ADMIN] Motoristas conectados (%d):\n", len(names))
			for _, name := range names {
				fmt.Printf(" - %s\n", name)
			}

		case ":slots":
			used := us.connectedCount()
			free := us.availableSlots()
			fmt.Printf("[ADMIN] Conectados: %d | Livres: %d | Máximo: %d\n",
				used, free, us.maxClients)

		case ":state":
			snaps := us.sessionSnapshots()
			if len(snaps) == 0 {
				fmt.Println("[ADMIN] Nenhum motorista conectado.")
				continue
			}
			fmt.Println("[ADMIN] Estado das sessões:")
			for _, s := range snaps {
				if s.Call != nil {
					fmt.Printf(" - %s -> %s | Corrida #%d | Valor: R$ %.2f\n",
						s.Name, s.State, s.Call.ID, s.Call.Value)
				} else {
					fmt.Printf(" - %s -> %s\n", s.Name, s.State)
				}
			}

		case ":active":
			call := us.activeCallSnapshot()
			if call == nil {
				fmt.Println("[ADMIN] Nenhuma chamada ativa no momento.")
				continue
			}
			remaining := time.Until(call.ExpiresAt).Round(time.Second)
			if remaining < 0 {
				remaining = 0
			}
			fmt.Printf("[ADMIN] Chamada ativa #%d | Passageiro: %.1f km | Corrida: %.1f km | Valor: R$ %.2f | Expira em: %s\n",
				call.ID, call.DistToPickup, call.RideDistance, call.Value, remaining)

		case ":quitadmin":
			fmt.Println("[ADMIN] Encerrando console admin.")
			return

		default:
			fmt.Println("[ADMIN] Comando desconhecido. Use :help")
		}
	}
}

// Init Uber server
func RunUberServer(ctx context.Context, addr string, maxClients int) error {
	store, err := loadStore()
	if err != nil {
		return fmt.Errorf("erro ao carregar dados: %w", err)
	}
	defer store.Close()

	srv, err := tcp.NewServer(ctx, addr, maxClients)
	if err != nil {
		return fmt.Errorf("erro ao iniciar servidor TCP: %w", err)
	}
	defer srv.Close()

	us := &UberServer{
		store:      store,
		maxClients: maxClients,
		sessions:   make(map[string]*DriverSession),
	}

	go us.eventLoop(ctx)    //Goroutine para cuidar da geração de chamadas e expiração
	go us.adminConsole(ctx) //Goroutine para console administrativo

	fmt.Printf("[SERVER] Aguardando conexões em %s (max %d motoristas)...\n", addr, maxClients)

	errs := srv.Errors()
	accepted := srv.Accepted()

	for {
		select {
		case <-ctx.Done():
			return nil

		case err, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			if err != nil {
				return fmt.Errorf("erro interno do servidor TCP: %w", err)
			}

		case ac, ok := <-accepted:
			if !ok {
				accepted = nil
				continue
			}
			go us.handleSession(ctx, srv, ac.Key, ac.Client)
		}
	}
}

// Func para cuidar de cada sessao de um motorista
func (us *UberServer) handleSession(ctx context.Context, srv *tcp.Server, key string, client *tcp.Client) {
	defer srv.RemoveClient(key)

	sendMsg(client, "Digite seu nome de usuário:")

	frame, err := client.ReadFrame()
	if err != nil {
		return
	}

	name := strings.TrimSpace(string(frame))
	if name == "" {
		sendMsg(client, "[ERRO] Nome inválido. Desconectando.")
		return
	}

	rec, err := us.store.ensureDriver(name)
	if err != nil {
		sendMsg(client, "[ERRO] Falha ao carregar dados do motorista.")
		return
	}

	session := &DriverSession{
		serverKey: key,
		name:      name,
		state:     StateIdle,
		client:    client,
	}

	us.mu.Lock()
	if _, exists := us.sessions[name]; exists {
		us.mu.Unlock()
		sendMsg(client, fmt.Sprintf("[ERRO] O nome %q já está em uso. Desconectando.", name))
		return
	}

	client.SetReadTimeout(0)

	us.sessions[name] = session
	us.mu.Unlock()

	defer us.removeSession(session)

	fmt.Printf("[SERVER] Motorista %q conectado de %s [%s]\n",
		name, client.RemoteAddr(), time.Now().Format("15:04:05"))

	now := time.Now().Format("15:04:05")
	sendMsg(client, fmt.Sprintf("[%s]: CONECTADO!!", now))
	if rec.TotalEarned == 0 {
		sendMsg(client, fmt.Sprintf("[INFO] Bem-vindo, %s! Saldo inicial: R$ 0,00", name))
	} else {
		sendMsg(client, fmt.Sprintf("[INFO] Bem-vindo de volta, %s! Faturamento total: R$ %.2f",
			name, rec.TotalEarned))
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		frame, err := client.ReadFrame()
		if err != nil {
			fmt.Printf("[SERVER] Motorista %q saiu por erro: %v\n", name, err)
			return
		}

		cmd := strings.TrimSpace(string(frame))
		if keep := us.handleCommand(session, cmd); !keep {
			fmt.Printf("[SERVER] Motorista %q desconectou.\n", name)
			return
		}
	}
}

// Remove uma sessao quando o motorista desconecta
func (us *UberServer) removeSession(session *DriverSession) {
	us.mu.Lock()
	defer us.mu.Unlock()

	delete(us.sessions, session.name)

	if us.activeCall == nil {
		return
	}

	stillPending := false
	for _, s := range us.sessions {
		if s.state == StateCallPending && s.currentCall == us.activeCall {
			stillPending = true
			break
		}
	}

	if !stillPending {
		us.activeCall = nil
	}
}

// Cuida dos casos de comandos de sessao
func (us *UberServer) handleCommand(session *DriverSession, cmd string) bool {
	switch cmd {
	case ":accept":
		call, msg := us.acceptCall(session)
		if msg != "" {
			sendMsg(session.client, "[RESPOSTA] "+msg)
			return true
		}
		sendMsg(session.client, fmt.Sprintf("[CONFIRMAÇÃO] Você executou: ACEITAR CORRIDA #%d", call.ID))
		sendMsg(session.client, fmt.Sprintf(
			"[INFO] Corrida aceita! Dirija %.1f km até o passageiro. Corrida: %.1f km | Valor: R$ %.2f",
			call.DistToPickup, call.RideDistance, call.Value,
		))
		sendMsg(session.client, "[INFO] Use :start quando chegar ao passageiro.")
		return true

	case ":start":
		call, msg := us.startRide(session)
		if msg != "" {
			sendMsg(session.client, "[RESPOSTA] "+msg)
			return true
		}
		sendMsg(session.client, fmt.Sprintf("[CONFIRMAÇÃO] Você executou: INICIAR CORRIDA #%d", call.ID))
		sendMsg(session.client, fmt.Sprintf(
			"[INFO] Corrida iniciada! Percurso de %.1f km. Use :finish ao chegar ao destino.",
			call.RideDistance,
		))
		return true

	case ":finish":
		call, total, msg := us.finishRide(session)
		if msg != "" {
			if strings.HasPrefix(msg, "[ERRO]") {
				sendMsg(session.client, msg)
			} else {
				sendMsg(session.client, "[RESPOSTA] "+msg)
			}
			return true
		}
		sendMsg(session.client, fmt.Sprintf("[CONFIRMAÇÃO] Você executou: FINALIZAR CORRIDA #%d", call.ID))
		sendMsg(session.client, fmt.Sprintf(
			"[INFO] Corrida finalizada! Você ganhou R$ %.2f. Faturamento total: R$ %.2f",
			call.Value, total,
		))
		sendMsg(session.client, "[INFO] Você está livre para novas chamadas.")
		return true

	case ":cancel":
		call, msg := us.cancelRide(session)
		if msg != "" {
			sendMsg(session.client, "[RESPOSTA] "+msg)
			return true
		}
		sendMsg(session.client, fmt.Sprintf("[CONFIRMAÇÃO] Você executou: CANCELAR CORRIDA #%d", call.ID))
		sendMsg(session.client, "[INFO] Corrida cancelada. Você está livre para novas chamadas.")
		return true

	case ":status":
		state, call, total := us.statusSnapshot(session)
		extra := ""
		if call != nil {
			extra = fmt.Sprintf(" | Corrida #%d | Distância: %.1f km | Valor: R$ %.2f",
				call.ID, call.RideDistance, call.Value)
		}
		sendMsg(session.client, fmt.Sprintf("[RESPOSTA] Status: %s%s | Faturamento total: R$ %.2f",
			state.String(), extra, total))
		return true

	case ":quit":
		sendMsg(session.client, "[INFO] Encerrando conexão. Até logo, motorista!")
		return false

	default:
		sendMsg(session.client, fmt.Sprintf(
			"[RESPOSTA] Comando desconhecido: %q. Use :accept, :start, :finish, :cancel, :status ou :quit",
			cmd,
		))
		return true
	}
}

// Comandos motorista
func (us *UberServer) acceptCall(session *DriverSession) (*Call, string) {
	us.mu.Lock()
	defer us.mu.Unlock()

	if session.state != StateCallPending || session.currentCall == nil {
		return nil, "Nenhuma chamada pendente para aceitar."
	}

	call := session.currentCall

	if us.activeCall != call {
		session.currentCall = nil
		session.state = StateIdle
		return nil, "A chamada já foi aceita por outro motorista."
	}

	if time.Now().After(call.ExpiresAt) {
		for _, other := range us.sessions {
			if other.state == StateCallPending && other.currentCall == call {
				other.currentCall = nil
				other.state = StateIdle
			}
		}
		us.activeCall = nil
		return nil, "Tempo esgotado para aceitar esta chamada."
	}

	session.state = StateAccepted
	us.activeCall = nil

	for _, other := range us.sessions {
		if other != session && other.state == StateCallPending && other.currentCall == call {
			other.currentCall = nil
			other.state = StateIdle
		}
	}

	return call, ""
}

// O motorista só pode iniciar a corrida se tiver aceitado uma chamada
func (us *UberServer) startRide(session *DriverSession) (*Call, string) {
	us.mu.Lock()
	defer us.mu.Unlock()

	if session.state != StateAccepted || session.currentCall == nil {
		return nil, "Nenhuma corrida aceita para iniciar. Use :accept primeiro."
	}

	session.state = StateStarted
	return session.currentCall, ""
}

// O motorista só pode finalizar a corrida se tiver iniciado uma chamada
func (us *UberServer) finishRide(session *DriverSession) (*Call, float64, string) {
	us.mu.Lock()
	if session.state != StateStarted || session.currentCall == nil {
		us.mu.Unlock()
		return nil, 0, "Nenhuma corrida em andamento. Use :start primeiro."
	}
	call := session.currentCall
	us.mu.Unlock()

	total, err := us.store.addEarnings(session.name, call.Value)
	if err != nil {
		return nil, 0, "[ERRO] Falha ao registrar ganhos. Tente novamente."
	}

	us.mu.Lock()
	session.currentCall = nil
	session.state = StateIdle
	us.mu.Unlock()

	return call, total, ""
}

// O motorista pode cancelar uma chamada se aceita e antes de iniciar a corrida
func (us *UberServer) cancelRide(session *DriverSession) (*Call, string) {
	us.mu.Lock()
	defer us.mu.Unlock()

	if session.state == StateStarted {
		return nil, "Corrida já iniciada. Use :finish para finalizar."
	}
	if session.state != StateAccepted || session.currentCall == nil {
		return nil, "Nenhuma corrida aceita para cancelar."
	}

	call := session.currentCall
	session.currentCall = nil
	session.state = StateIdle

	return call, ""
}

// O motorista pode verificar o status atual
func (us *UberServer) statusSnapshot(session *DriverSession) (SessionState, *Call, float64) {
	us.mu.Lock()
	state := session.state
	var callCopy *Call
	if session.currentCall != nil {
		c := *session.currentCall
		callCopy = &c
	}
	us.mu.Unlock()

	total := us.store.totalEarned(session.name)
	return state, callCopy, total
}

// Gerencia o loop de eventos do servidor, como geração de chamadas e expiração
func (us *UberServer) eventLoop(ctx context.Context) {
	callTimer := time.NewTimer(10 * time.Second)
	defer callTimer.Stop()
	checkTicker := time.NewTicker(1 * time.Second)
	defer checkTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-callTimer.C:
			us.generateAndDispatch()
			callTimer.Reset(10 * time.Second)
		case <-checkTicker.C:
			us.checkActiveCallExpiry()
		}
	}
}

// Gera uma nova chamada e notifica os motoristas disponíveis
func (us *UberServer) generateAndDispatch() {
	call := &Call{
		DistToPickup: randomFloat(0.5, 8.0),
		RideDistance: randomFloat(2.0, 25.0),
		ExpiresAt:    time.Now().Add(12 * time.Second),
	}
	call.Value = call.RideDistance*1.5 + randomFloat(2.0, 8.0)

	var recipients []*tcp.Client

	us.mu.Lock()
	if us.activeCall != nil {
		us.mu.Unlock()
		return
	}

	us.nextCallID++
	call.ID = us.nextCallID
	us.activeCall = call

	for _, session := range us.sessions {
		if session.state == StateIdle {
			session.state = StateCallPending
			session.currentCall = call
			recipients = append(recipients, session.client)
		}
	}

	if len(recipients) == 0 {
		us.activeCall = nil
	}
	us.mu.Unlock()

	for _, client := range recipients {
		sendMsg(client, fmt.Sprintf(
			"\n[NOVA CHAMADA #%d] Passageiro a %.1f km | Corrida: %.1f km | Valor: R$ %.2f | Tempo para aceitar: 12s",
			call.ID, call.DistToPickup, call.RideDistance, call.Value,
		))
	}
}

// Verifica se a chamada ativa expirou e notifica os motoristas
func (us *UberServer) checkActiveCallExpiry() {
	type outbound struct {
		client *tcp.Client
		msg    string
	}

	var outs []outbound

	us.mu.Lock()
	call := us.activeCall
	if call == nil || time.Now().Before(call.ExpiresAt) {
		us.mu.Unlock()
		return
	}

	//65% aumenta, 35% cancela
	shouldCancel := call.ValueBumped || rand.Intn(100) < 35

	if shouldCancel {
		for _, session := range us.sessions {
			if session.state == StateCallPending && session.currentCall == call {
				session.currentCall = nil
				session.state = StateIdle
				outs = append(outs, outbound{
					client: session.client,
					msg:    fmt.Sprintf("[AVISO] CHAMADA #%d: Tempo esgotado. Corrida cancelada.", call.ID),
				})
			}
		}
		us.activeCall = nil
		us.mu.Unlock()

		for _, out := range outs {
			sendMsg(out.client, out.msg)
		}
		return
	}

	//Se o valor foi aumentado só pode ter a opção de cancelar depois
	bonus := rand.Float64()*5 + 2
	call.Value += bonus
	call.ExpiresAt = time.Now().Add(10 * time.Second)
	call.ValueBumped = true
	newValue := call.Value

	for _, session := range us.sessions {
		if session.state == StateCallPending && session.currentCall == call {
			outs = append(outs, outbound{
				client: session.client,
				msg: fmt.Sprintf(
					"[VALOR AUMENTADO] CHAMADA #%d: Valor aumentado para R$ %.2f. Mais 10s para aceitar.",
					call.ID, newValue,
				),
			})
		}
	}
	us.mu.Unlock()

	for _, out := range outs {
		sendMsg(out.client, out.msg)
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
