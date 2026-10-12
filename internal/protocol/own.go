package protocol

// The process's own connections.
//
// The console is a Kafka client of the broker it runs beside, in the same process, and
// the Traffic page leaves the console's own requests out: the page that shows traffic
// would otherwise be mostly the page's own polling. It used to know them by their
// client id. A client id is whatever a client says it is, so any client that called
// itself "doze-kafka-console" was left out too, and could not be seen by the person
// looking for it.
//
// What a client cannot say is which connections this process itself opened. So the
// process says it, in memory, about the connection: after the console dials and before
// it sends a byte, it calls Adopt with the connection it holds, which finds the server's
// end of that same connection (by the two addresses, which both ends see) and marks
// it. Nothing about it is on the wire; a connection from anywhere else cannot be
// marked, whatever it sends.

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

// accepted is every connection a Server of this process is serving, by its two ends.
var accepted = struct {
	sync.Mutex
	byEnds map[string]*Conn
	// more is closed, and replaced, each time a connection is added: Adopt waits on it
	// for a connection it dialled and the server has not accepted yet.
	more chan struct{}
}{byEnds: map[string]*Conn{}, more: make(chan struct{})}

// ends names a connection by the address of its client end and of its server end. The
// pair is one live connection: TCP does not let two share it.
func ends(client, server net.Addr) string { return client.String() + " > " + server.String() }

// accept records that c is being served. The accept loop calls it before the
// connection's first request is read.
func (c *Conn) accept() {
	accepted.Lock()
	accepted.byEnds[ends(c.raw.RemoteAddr(), c.raw.LocalAddr())] = c
	close(accepted.more)
	accepted.more = make(chan struct{})
	accepted.Unlock()
}

// forget removes c, before its socket is closed: once the socket is closed the same
// pair of addresses may be another connection, which must not find this one's mark.
func (c *Conn) forget() {
	accepted.Lock()
	if key := ends(c.raw.RemoteAddr(), c.raw.LocalAddr()); accepted.byEnds[key] == c {
		delete(accepted.byEnds, key)
	}
	accepted.Unlock()
}

// adoptWait is how long Adopt waits for the server to accept a connection that has
// just been dialled. It is there within microseconds; this is for a loaded machine.
const adoptWait = 5 * time.Second

// ErrNotOurs is returned by Adopt for a connection no Server in this process is
// serving: one to a broker somewhere else, or through a proxy.
var ErrNotOurs = errors.New("protocol: the connection is not to a server in this process")

// Adopt marks the server's end of dialed as one of this process's own connections, so
// that an Observer is told (Observation.Own) and can leave the process's own requests
// out. Call it with a connection just dialled, before anything is sent on it. It waits
// for the server to have accepted the connection, and returns ErrNotOurs if no Server
// here does: the connection is usable all the same, and is then observed like anyone's.
func Adopt(ctx context.Context, dialed net.Conn) error {
	key := ends(dialed.LocalAddr(), dialed.RemoteAddr())
	ctx, cancel := context.WithTimeout(ctx, adoptWait)
	defer cancel()
	for {
		accepted.Lock()
		c, more := accepted.byEnds[key], accepted.more
		accepted.Unlock()
		if c != nil {
			c.own.Store(true)
			return nil
		}
		select {
		case <-more:
		case <-ctx.Done():
			return ErrNotOurs
		}
	}
}
