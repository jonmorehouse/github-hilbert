package hilbert

type Point [2]int

type DisplayData struct {
	Label  string
	Points []Point
}

type Display interface {
	Init() error
	Close() error

	DataCh() chan DisplayData
	ErrCh() chan error
}
