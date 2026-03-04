package tcp

import (
	"errors"
	"net"
	"strconv"
	"sync"
	"context"
	"crypto/rand"
)

// Clientes aceitos
type AcceptedClient struct {
	Key    string
	Client *Client
}

// dados do objeto server
type Server struct {
	addr string // endereco ip + porta logica do server
	ln net.Listener // Receptor de novas conexoes

	maxClients int // maximo de clientes conectados no server
	sem chan struct{} // semphore

	clients   map[string]*Client // cache de clientes conectados
	clientsMu sync.RWMutex // Mutex com read-lock para leitura concorrente em hashmaps

	accepted chan AcceptedClient // canal de clientes aceitos
	errs     chan error // canal de erros

	ctx context.Context
	cancel context.CancelFunc

	once   sync.Once // para fechamento do server, usado para somente fechar na primeira chamada do Close(), evitando panics
}

// inicializador/construtor do server
func NewServer(addr string) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	s := &Server{
		ln:       ln,
		clients:  make(map[string]*Client),
		accepted: make(chan AcceptedClient, 128),
		errs:     make(chan error, 1),
		done:     make(chan struct{}),
	}

	go s.acceptLoop() // goroutine para lidar com novas conexoes

	return s, nil
}

// loop para lidar com novas conexoes
func (s *Server) acceptLoop() {
	defer close(s.accepted)
	defer close(s.errs)

	for {
		conn, err := s.ln.Accept()
		if err != nil {
			select {
			case <-s.done:
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

		key := s.newClientKey()
		client := NewClient(conn)

		s.clientsMu.Lock()
		s.clients[key] = client
		s.clientsMu.Unlock()

		select {
		case s.accepted <- AcceptedClient{Key: key, Client: client}:
		case <-s.done:
			s.clientsMu.Lock()
			delete(s.clients, key)
			s.clientsMu.Unlock()
			_ = client.Close()
			return
		}
	}
}

func (s *Server) Accepted() <-chan AcceptedClient {
	return s.accepted
}

func (s *Server) Errors() <-chan error {
	return s.errs
}

// Puxar um client conectado
func (s *Server) GetClient(key string) (*Client, bool) {
	s.clientsMu.RLock()
	client, ok := s.clients[key]
	s.clientsMu.RUnlock()
	return client, ok
}

// Remocao de um client conectado
func (s *Server) RemoveClient(key string) {
	s.clientsMu.Lock()
	client, ok := s.clients[key]
	if ok {
		delete(s.clients, key)
	}
	s.clientsMu.Unlock()

	if ok {
		_ = client.Close()
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

func (s *Server) Close() error {
	var err error

	s.once.Do(func() {
		close(s.done)
		err = s.ln.Close()

		s.clientsMu.Lock()
		clients := make([]*Client, 0, len(s.clients))
		for _, client := range s.clients {
			clients = append(clients, client)
		}
		s.clients = make(map[string]*Client)
		s.clientsMu.Unlock()

		for _, client := range clients {
			_ = client.Close()
		}
	})

	return err
}

func (s *Server) newClientKey() string {
    var b [8]byte
    rand.Read(b[:])
    return hex.EncodeToString(b[:])
}
