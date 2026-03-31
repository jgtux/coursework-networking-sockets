package tcp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

type ErrReason string

func (e ErrReason) Error() string {
	return string(e)
}

const (
	ErrServerFull   ErrReason = "SERVER_FULL"
	ErrAcceptFailed ErrReason = "ACCEPT_FAIL"
)


const (
	// timeouts padrao aplicados aos clientes aceitos pelo servidor
	DefaultAcceptedClientReadTimeout  = 30 * time.Second
	DefaultAcceptedClientWriteTimeout = 10 * time.Second

	// timeout menor para responder "server full" e fechar
	DefaultRejectWriteTimeout = 2 * time.Second
)

type AcceptedClient struct {
	Key    string
	Client *Client
}

// struct server
type Server struct {
	addr string
	ln   net.Listener

	maxClients int           // nao pode ser uint
	sem        chan struct{} // semáforo: 1 slot por cliente conectado

	clients   map[string]*Client
	clientsMu sync.RWMutex

	errs     chan error
	accepted chan AcceptedClient

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
		return nil, fmt.Errorf("Pelo menos 1 slot deve estar disponivel.")
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
		clients:    make(map[string]*Client),
		errs:       make(chan error, 1),
		accepted:   make(chan AcceptedClient, maxClients),
		ctx:        ctx,
		cancel:     cancel,
	}

	// quando o ctx cancelar, fecha o listener pra destravar o Accept()
	go func() {
		<-s.ctx.Done()
		_ = s.ln.Close()
	}()

	go s.acceptLoop()

	return s, nil
}

func (s *Server) trySendErr(err error) {
	if err == nil {
		return
	}

	select {
	case s.errs <- err:
	default:
	}
}

// loop para lidar com novas conexoes
func (s *Server) acceptLoop() {
	defer close(s.errs)
	defer close(s.accepted)

	for {
		conn, err := s.ln.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
			}

			if errors.Is(err, net.ErrClosed) {
				return
			}

			s.trySendErr(fmt.Errorf("%w: erro ao aceitar conexão: %w", ErrAcceptFailed, err))
			return
		}

		client := NewClient(conn)
		client.SetReadTimeout(DefaultAcceptedClientReadTimeout)
		client.SetWriteTimeout(DefaultAcceptedClientWriteTimeout)

		// tenta pegar 1 slot do semáforo; se cheio, recusa
		select {
		case s.sem <- struct{}{}:
			// ok, tem vaga

		default:
			client.SetWriteTimeout(DefaultRejectWriteTimeout)
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
			s.RemoveClient(key)
			return
		}
	}
}

func (s *Server) Accepted() <-chan AcceptedClient { return s.accepted }

// Errors retorna o canal de erros internos do servidor
func (s *Server) Errors() <-chan error { return s.errs }

// GetClient retorna um cliente pelo seu identificador interno
func (s *Server) GetClient(key string) (*Client, bool) {
	s.clientsMu.RLock()
	client, ok := s.clients[key]
	s.clientsMu.RUnlock()
	return client, ok
}

// RemoveClient remove um cliente do mapa e fecha sua conexão, devolvendo o slot do semáforo
func (s *Server) RemoveClient(key string) {
	s.clientsMu.Lock()
	client, ok := s.clients[key]
	if ok {
		delete(s.clients, key)
	}
	s.clientsMu.Unlock()

	if !ok {
		return
	}

	_ = client.Close()

	select {
	case <-s.sem:
	default:
	}
}

// Close encerra o servidor — só executa na primeira chamada (sync.Once)
func (s *Server) Close() error {
	var err error

	s.once.Do(func() {
		s.cancel()

		// pode ja ter sido fechado pela goroutine que escuta ctx.Done()
		if cerr := s.ln.Close(); cerr != nil && !errors.Is(cerr, net.ErrClosed) {
			err = cerr
		}

		s.clientsMu.Lock()
		clients := make([]*Client, 0, len(s.clients))
		for _, client := range s.clients {
			clients = append(clients, client)
		}
		s.clients = make(map[string]*Client)
		s.clientsMu.Unlock()

		for _, client := range clients {
			_ = client.Close()

			select {
			case <-s.sem:
			default:
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
