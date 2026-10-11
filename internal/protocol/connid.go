package protocol

// ID is the connection's number: the one the observer is told, so what is known of a
// connection in one place (the recorder) can be joined to what is known in another
// (a client's telemetry).
func (c *Conn) ID() uint64 { return c.id }

// Open reports whether the connection's serve loop is still running.
func (c *Conn) Open() bool { return !c.done.Load() }
