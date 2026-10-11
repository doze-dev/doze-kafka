package p

import "net"

// Thing is documented, which the surface ignores.
type Thing struct {
	Name   string
	hidden int
	net.Conn
	Opt *bool
}

type Doer interface {
	Do(x int) error
	quiet()
}

type Alias = map[string]int

type hiddenType struct{ X int }

const Max = 3
const lower = 4

var Default Thing

func New(n string, opts ...int) (*Thing, error) { return nil, nil }

func helper() {}

func (t *Thing) Close() error { return nil }

func (t Thing) quiet() {}

func (h hiddenType) Exported() {}
