package tcp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

// timeouts padrao por operacao (0 = sem timeout)
// max tamanho de frame padrao
const (
	DefaultReadTimeout  = 0 * time.Second
	DefaultWriteTimeout = 0 * time.Second
	DefaultMaxFrameSize uint32 = 1 << 20 // 1 MiB
)

// struct client
type Client struct {
	conn         net.Conn   // conexao tcp do cliente
	writeMu      sync.Mutex // mutex para evitar writes concorrentes no mesmo socket
	maxFrameSize uint32     // tamanho maximo permitido para um frame

	readTimeout  time.Duration // timeout por operacao de leitura
	writeTimeout time.Duration // timeout por operacao de escrita
}

// inicializador/construtor do client
func NewClient(conn net.Conn) *Client {
	return &Client{
		conn:         conn,
		maxFrameSize: DefaultMaxFrameSize,
		readTimeout:  DefaultReadTimeout,
		writeTimeout: DefaultWriteTimeout,
	}
}

// define o timeout por operacao de leitura (0 = sem timeout)
func (c *Client) SetReadTimeout(d time.Duration) {
	c.readTimeout = d
}

// define o timeout por operacao de escrita (0 = sem timeout)
func (c *Client) SetWriteTimeout(d time.Duration) {
	c.writeTimeout = d
}

// le um frame completo da conexao
// protocolo:
// 4 bytes iniciais = tamanho do payload em big endian
// payload = dados reais do frame
func (c *Client) ReadFrame() ([]byte, error) {
	// aplica deadline de leitura por operacao, se configurado
	if c.readTimeout > 0 {
		_ = c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
		defer c.conn.SetReadDeadline(time.Time{})
	}

	var header [4]byte

	// le exatamente os 4 bytes do cabecalho
	if _, err := io.ReadFull(c.conn, header[:]); err != nil {
		return nil, wrapTimeout("read header", err)
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
		return nil, wrapTimeout("read payload", err)
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

	// aplica deadline de escrita por operacao, se configurado
	if c.writeTimeout > 0 {
		_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		defer c.conn.SetWriteDeadline(time.Time{})
	}

	// envia primeiro o cabecalho
	if err := writeFull(c.conn, header[:]); err != nil {
		return wrapTimeout("write header", err)
	}

	// depois envia o payload
	if err := writeFull(c.conn, payload); err != nil {
		return wrapTimeout("write payload", err)
	}

	return nil
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

// helper para identificar timeout/deadline excedido
func IsTimeout(err error) bool {
	if err == nil {
		return false
	}

	// forma comum em Go recente
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}

	// forma classica para net.Conn
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// helper para padronizar mensagem em caso de timeout
func wrapTimeout(where string, err error) error {
	if err == nil {
		return nil
	}

	if IsTimeout(err) {
		return fmt.Errorf("%s timeout: %w", where, err)
	}

	return err
}
