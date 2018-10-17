package hilbert

import "fmt"

type ErrDisplayInterrupt struct{}

func (e ErrDisplayInterrupt) Error() string { return "signal caught" }

type ErrDisplayNotInitialized struct{}

func (e ErrDisplayNotInitialized) Error() string { return "display not initialized" }

type ErrDisplayTooSmall struct {
	height, width int
}

func (e ErrDisplayTooSmall) Error() string {
	return fmt.Sprintf("%vx%v display too small must be %vx%v", e.width, e.height, minDisplayHeight, minDisplayWidth)
}
