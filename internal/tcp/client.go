package tcp

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
)

const DefaultMaxFrameSize uint32 = 1 << 20 // 1 MiB

// struct client
type Client struct {
	conn         net.Conn   // conexao tcp do cliente
	writeMu      sync.Mutex // mutex para evitar writes concorrentes no mesmo socket
	maxFrameSize uint32     // tamanho maximo permitido para um frame
}

// inicializador/construtor do client
func NewClient(conn net.Conn) *Client {
	return &Client{
		conn:         conn,
		maxFrameSize: DefaultMaxFrameSize,
	}
}

// le um frame completo da conexao
// protocolo:
// 4 bytes iniciais = tamanho do payload em big endian
// payload = dados reais do frame
func (c *Client) ReadFrame() ([]byte, error) {
	var header [4]byte

	// le exatamente os 4 bytes do cabecalho
	if _, err := io.ReadFull(c.conn, header[:]); err != nil {
		return nil, err
	}

	// converte os 4 bytes para uint32
	size := binary.BigEndian.Uint32(header[:])

	// valida o tamanho maximo permitido
	if size > c.maxFrameSize {
		return nil, fmt.Errorf("frame too large: %d > %d", size, c.maxFrameSize)
	}

	// cria buffer com o tamanho informado no cabecalho
	payload := make([]byte, size)

	// le exatamente o payload inteiro
	if _, err := io.ReadFull(c.conn, payload); err != nil {
		return nil, err
	}

	return payload, nil
}

// envia um frame completo pela conexao
// protocolo:
// 4 bytes iniciais = tamanho do payload em big endian
// payload = dados reais do frame
func (c *Client) SendFrame(payload []byte) error {
	// valida se o payload nao excede o tamanho maximo permitido
	if len(payload) > int(c.maxFrameSize) {
		return fmt.Errorf("frame too large: %d > %d", len(payload), c.maxFrameSize)
	}

	var header [4]byte

	// escreve no cabecalho o tamanho do payload
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))

	// lock para evitar intercalacao de escritas concorrentes no mesmo socket
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	// envia primeiro o cabecalho
	if err := writeFull(c.conn, header[:]); err != nil {
		return err
	}

	// depois envia o payload
	return writeFull(c.conn, payload)
}

// fecha a conexao do client
func (c *Client) Close() error {
	return c.conn.Close()
}

// retorna o endereco remoto do client conectado
func (c *Client) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// helper para garantir escrita completa no writer
// continua escrevendo ate consumir todo o buffer ou ocorrer erro
func writeFull(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)

		// avanca no buffer caso tenha escrito algo
		if n > 0 {
			p = p[n:]
		}

		// se houve erro, retorna imediatamente
		if err != nil {
			return err
		}

		// protecao contra writer que nao escreve nada e nao retorna erro
		if n == 0 {
			return io.ErrShortWrite
		}
	}

	return nil
}
