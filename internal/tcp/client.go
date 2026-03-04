package tcp

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
)

const DefaultMaxFrameSize uint32 = 1 << 20 // 1 MiB

type Client struct {
	conn         net.Conn
	writeMu      sync.Mutex
	maxFrameSize uint32
}

func NewClient(conn net.Conn) *Client {
	return &Client{
		conn:         conn,
		maxFrameSize: DefaultMaxFrameSize,
	}
}

func (c *Client) ReadFrame() ([]byte, error) {
	var header [4]byte

	if _, err := io.ReadFull(c.conn, header[:]); err != nil {
		return nil, err
	}

	size := binary.BigEndian.Uint32(header[:])
	if size > c.maxFrameSize {
		return nil, fmt.Errorf("frame too large: %d > %d", size, c.maxFrameSize)
	}

	payload := make([]byte, size)
	if _, err := io.ReadFull(c.conn, payload); err != nil {
		return nil, err
	}

	return payload, nil
}

func (c *Client) SendFrame(payload []byte) error {
	if len(payload) > int(c.maxFrameSize) {
		return fmt.Errorf("frame too large: %d > %d", len(payload), c.maxFrameSize)
	}

	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if err := writeFull(c.conn, header[:]); err != nil {
		return err
	}
	return writeFull(c.conn, payload)
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func writeFull(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if n > 0 {
			p = p[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
