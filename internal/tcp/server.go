package tcp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net"
	"sync"
	"fmt"
)

type ErrReason string

const (
	ErrServerFull ErrReason = "SERVER_FULL"
)

// para implementar a interface do error
func (e ErrReason) Error() string {
	return string(e)
}


// Clientes aceitos
type AcceptedClient struct {
	Key    string
	Client *Client
}

// struct server
type Server struct {
	addr string
	ln   net.Listener

	maxClients int // nao pode ser uint
	sem        chan struct{} // semáforo: 1 slot por cliente conectado

	clients   map[string]*Client
	clientsMu sync.RWMutex

	accepted chan AcceptedClient
	errs     chan error

	ctx    context.Context
	cancel context.CancelFunc

	once sync.Once
}

// inicializador/construtor do server
// maxClients precisa ser > 0
func NewServer(parent context.Context, addr string, maxClients int) (*Server, error) {
	if parent == nil {
		parent = context.Background()
	}
	if maxClients <= 0 {
		return nil, fmt.Errorf("Atleast 1 client slot must be available.")
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(parent)

	s := &Server{
		addr:       addr,
		ln:         ln,
		maxClients: maxClients,
		sem:        make(chan struct{}, maxClients),

		clients:  make(map[string]*Client),
		accepted: make(chan AcceptedClient, 128),
		errs:     make(chan error, 1),

		ctx:    ctx,
		cancel: cancel,
	}

	// quando o ctx cancelar, fecha o listener pra destravar o Accept()
	go func() {
		<-s.ctx.Done()
		_ = s.ln.Close()
	}()

	go s.acceptLoop()

	return s, nil
}

// loop para lidar com novas conexoes
func (s *Server) acceptLoop() {
	defer close(s.accepted)
	defer close(s.errs)

	for {
		conn, err := s.ln.Accept()
		if err != nil {
			// shutdown via ctx cancel + listener fechado
			select {
			case <-s.ctx.Done():
				return
			default:
			}

			if errors.Is(err, net.ErrClosed) {
				return
			}

			select {
			case s.errs <- err:
			default:
			}
			return
		}

		client := NewClient(conn)

		// tenta pegar 1 slot do semáforo; se cheio, recusa
		select {
		case s.sem <- struct{}{}:
			// ok, tem vaga
		default:
			_ = client.SendFrame([]byte(ErrServerFull))

			_ = client.Close()
			continue
		}

		key := s.newClientKey()
		s.clientsMu.Lock()
		s.clients[key] = client
		s.clientsMu.Unlock()

		select {
		case s.accepted <- AcceptedClient{Key: key, Client: client}:
		case <-s.ctx.Done():
			// se está fechando, remove e devolve o slot
			s.clientsMu.Lock()
			delete(s.clients, key)
			s.clientsMu.Unlock()
			_ = client.Close()
			<-s.sem
			return
		}
	}
}

// coleta todos os clientes aceitos no canal accepted
func (s *Server) Accepted() <-chan AcceptedClient { return s.accepted }

// coleta erros do canal error
func (s *Server) Errors() <-chan error { return s.errs }

// Puxar um client conectado
func (s *Server) GetClient(key string) (*Client, bool) {
	s.clientsMu.RLock()
	client, ok := s.clients[key]
	s.clientsMu.RUnlock()
	return client, ok
}

// Remocao de um client conectado (IMPORTANTE: devolve 1 slot do semáforo)
func (s *Server) RemoveClient(key string) {
	s.clientsMu.Lock()
	client, ok := s.clients[key]
	if ok {
		delete(s.clients, key)
	}
	s.clientsMu.Unlock()

	if ok {
		_ = client.Close()
		<-s.sem // devolve o slot
	}
}

// distribui um frame para todos os clientes conectados
func (s *Server) BroadcastFrame(payload []byte) {
	s.clientsMu.RLock()
	clients := make([]*Client, 0, len(s.clients))
	for _, client := range s.clients {
		clients = append(clients, client)
	}
	s.clientsMu.RUnlock()

	for _, client := range clients {
		_ = client.SendFrame(payload)
	}
}

// fecha servidor, só executa na primeira chamada (sync.Once)
func (s *Server) Close() error {
	var err error

	s.once.Do(func() {
		s.cancel()
		err = s.ln.Close()

		s.clientsMu.Lock()
		clients := make([]*Client, 0, len(s.clients))
		for key, client := range s.clients {
			_ = key
			clients = append(clients, client)
		}
		s.clients = make(map[string]*Client)
		s.clientsMu.Unlock()

		for range clients {
			// vamos fechar abaixo com índice pra liberar sem também
		}

		for _, client := range clients {
			_ = client.Close()
			// libera um slot por cliente que estava conectado
			select {
			case <-s.sem:
			default:
				// se por algum motivo já foi liberado, não trava
			}
		}
	})

	return err
}

// gera uma key aleatoria para cada cliente conectado
func (s *Server) newClientKey() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
