package balancer

import (
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/getlantern/testify/assert"
)

var (
	msg = []byte("Hello world")
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestAll(t *testing.T) {
	// Start an echo server
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		log.Fatalf("Unable to listen: %s", err)
	}
	defer l.Close()
	go func() {
		for {
			c, err := l.Accept()
			if err == nil {
				go func() {
					_, err = io.Copy(c, c)
					if err != nil {
						log.Fatalf("Unable to echo: %s", err)
					}
				}()
			}
		}
	}()
	addr := l.Addr().String()

	dialedBy := 0

	dialer1 := &Dialer{
		Weight: 1,
		QOS:    10,
		Dial: func(network, addr string) (net.Conn, error) {
			dialedBy = 1
			return net.Dial(network, addr)
		},
	}
	dialer2 := &Dialer{
		Weight: 10000000,
		QOS:    1,
		Dial: func(network, addr string) (net.Conn, error) {
			dialedBy = 2
			return net.Dial(network, addr)
		},
	}
	dialer3 := &Dialer{
		Weight: 1,
		QOS:    15,
		Dial: func(network, addr string) (net.Conn, error) {
			dialedBy = 3
			return nil, fmt.Errorf("Me no dialee")
		},
	}

	// Test successful single dialer
	b := New(dialer1)
	conn, err := b.Dial("tcp", addr, 0)
	assert.NoError(t, err, "Dialing should have succeeded")
	assert.Equal(t, 1, dialedBy, "Wrong dialedBy")
	doTestConn(t, conn)

	// Test QOS
	dialedBy = 0
	b = New(dialer1, dialer2)
	conn, err = b.Dial("tcp", addr, 5)
	assert.NoError(t, err, "Dialing should have succeeded")
	assert.Equal(t, 1, dialedBy, "Wrong dialedBy")
	doTestConn(t, conn)

	// Test random selection
	dialedBy = 0
	conn, err = b.Dial("tcp", addr, 0)
	assert.NoError(t, err, "Dialing should have succeeded")
	assert.Equal(t, 2, dialedBy, "Wrong dialedBy (note this has a 1/%d chance of failing)", (dialer1.Weight + dialer2.Weight))
	doTestConn(t, conn)

	// Test success with failing dialer
	dialedBy = 0
	b = New(dialer1, dialer2, dialer3)
	conn, err = b.Dial("tcp", addr, 20)
	assert.NoError(t, err, "Dialing should have succeeded")
	assert.Equal(t, 1, dialedBy, "Wrong dialedBy")
	doTestConn(t, conn)

	// Test failure
	b = New(dialer3)
	_, err = b.Dial("tcp", addr, 0)
	assert.Error(t, err, "Dialing should have failed")
}

func doTestConn(t *testing.T, conn net.Conn) {
	defer conn.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		n, err := conn.Write(msg)
		assert.NoError(t, err, "Writing should have succeeded")
		assert.Equal(t, len(msg), n, "Should have written full message")
		wg.Done()
	}()
	go func() {
		b := make([]byte, len(msg))
		n, err := io.ReadFull(conn, b)
		assert.NoError(t, err, "Read should have succeeded")
		assert.Equal(t, len(msg), n, "Should have read full message")
		assert.Equal(t, msg, b[:n], "Read should have matched written")
		wg.Done()
	}()

	wg.Wait()
}

var (
	failed = fmt.Errorf("I failed")
)

type failingConn struct {
	conn         net.Conn
	bytesRead    int
	bytesWritten int
}

func (c *failingConn) Read(b []byte) (n int, err error) {
	n, err = c.conn.Read(b[:c.bytesRead])
	err = failed
	return
}

func (c *failingConn) Write(b []byte) (n int, err error) {
	n, err = c.conn.Write(b[:c.bytesWritten])
	err = failed
	return
}

func (c *failingConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *failingConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *failingConn) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

func (c *failingConn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *failingConn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

func (c *failingConn) Close() error {
	return c.conn.Close()
}
