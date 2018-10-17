package hilbert

import (
	"math/rand"
	"time"

	"github.com/gdamore/tcell"
	runewidth "github.com/mattn/go-runewidth"
)

const (
	minDisplayHeight = 10
	minDisplayWidth  = 10
)

type TerminalDisplayOptions struct {
	MinRefresh  time.Duration
	BufferSize  int
	PeanoFactor int

	LabelHeight      int
	LabelTextOffset  [2]int
	LabelDefaultText string
}

func NewDefaultTerminalDisplayOptions() TerminalDisplayOptions {
	return TerminalDisplayOptions{
		MinRefresh:  time.Second,
		BufferSize:  1000,
		PeanoFactor: 5,

		LabelDefaultText: "Default Label",
		LabelHeight:      3,
		LabelTextOffset:  [2]int{5, 1},
	}
}

func NewTerminalDisplay(opts TerminalDisplayOptions) Display {
	if opts == (TerminalDisplayOptions{}) {
		opts = NewDefaultTerminalDisplayOptions()
	}

	return &terminalDisplay{
		opts: opts,

		drawDeadline: time.Time{},

		dataCh:   make(chan DisplayData, opts.BufferSize),
		errCh:    make(chan error),
		resizeCh: make(chan struct{}),
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// terminalDisplay shows a hilbert curve on the local terminal. It exposes a
// dataCh for sending new data to display and enforces a "min" refresh, which
// throttles how often the screen changes
type terminalDisplay struct {
	opts TerminalDisplayOptions

	drawDeadline time.Time
	currentData  DisplayData

	dataCh   chan DisplayData
	doneCh   chan struct{}
	errCh    chan error
	resizeCh chan struct{}
	stopCh   chan struct{}

	screen tcell.Screen
}

func (d *terminalDisplay) Init() error {
	tcell.SetEncodingFallback(tcell.EncodingFallbackASCII)

	screen, err := tcell.NewScreen()
	if err != nil {
		return err
	}

	if err := screen.Init(); err != nil {
		return err
	}
	d.screen = screen

	if err := d.checkScreenSize(); err != nil {
		return err
	}

	go d.pollLoop()
	go d.drawLoop()

	return nil
}

func (d *terminalDisplay) Close() error {
	select {
	case d.stopCh <- struct{}{}:
	default:
		return nil
	}

	<-d.doneCh
	close(d.errCh)
	return nil
}

func (d *terminalDisplay) DataCh() chan DisplayData {
	return d.dataCh
}

func (d *terminalDisplay) ErrCh() chan error {
	return d.errCh
}

// NOTE: tcell "takes over" the terminal and captures incoming keyevents and
// signals. This poller listens for those events and propagates them
// accordingly
func (d *terminalDisplay) pollLoop() {
	for {
		event := d.screen.PollEvent()
		if event == nil {
			return
		}

		switch event := event.(type) {
		case *tcell.EventKey:
			switch event.Key() {
			case tcell.KeyCtrlC, tcell.KeyEsc:
				d.sendErr(ErrDisplayInterrupt{})
			}
			d.sendErr(ErrDisplayInterrupt{})
		case *tcell.EventResize:
			d.resizeCh <- struct{}{}
		}
	}

}

func (d *terminalDisplay) drawLoop() {
	defer close(d.doneCh)
	defer d.screen.Fini()

	for {
		select {
		case <-d.stopCh:
			return
		case <-d.resizeCh:
			d.draw()
		case data, ok := <-d.dataCh:
			if !ok {
				return
			}

			d.currentData = data
			d.drawWithDeadline()
		}
	}
}

func (d *terminalDisplay) checkScreenSize() error {
	width, height := d.screen.Size()
	if width < minDisplayWidth || height < minDisplayHeight {
		return ErrDisplayTooSmall{width: width, height: height}
	}

	return nil
}

func (d *terminalDisplay) drawWithDeadline() {
	// if no min refresh is given, then draw immediately
	if d.opts.MinRefresh == time.Duration(0) {
		d.draw()
		return
	}
	defer func() {
		d.drawDeadline = time.Now().Add(d.opts.MinRefresh)
	}()

	<-time.After(d.drawDeadline.Sub(time.Now()))
	d.draw()
}

func (d *terminalDisplay) sendErr(err error) {
	select {
	case d.errCh <- err:
	default:
	}
}

func (d *terminalDisplay) draw() {
	style := tcell.StyleDefault
	if d.screen.Colors() > 256 {
		rgb := tcell.NewHexColor(int32(rand.Int() & 0xffffff))
		style = style.Background(rgb)
	}
	d.screen.SetStyle(tcell.StyleDefault.Foreground(tcell.ColorBlack).Background(tcell.ColorWhite))

	d.drawLabel(style)
	d.drawHilbert(style)

	d.screen.Show()
}

func (d *terminalDisplay) drawLabel(baseStyle tcell.Style) {
	style := baseStyle.Background(tcell.ColorLightGray).Foreground(tcell.ColorBlack)

	width, height := d.screen.Size()

	// draw label box
	startY := height - d.opts.LabelHeight
	for row := startY; row <= height; row++ {
		for col := 0; col < width; col++ {
			d.screen.SetCell(col, row, style, ' ')
		}
	}

	// draw background
	x := d.opts.LabelTextOffset[0]
	y := startY + d.opts.LabelTextOffset[1]

	label := d.currentData.Label
	if label == "" {
		label = d.opts.LabelDefaultText
	}

	i := 0
	for _, ru := range label {
		d.screen.SetContent(x+i, y, ru, []rune{}, style)
		i += runewidth.RuneWidth(ru)
	}
}

func (d *terminalDisplay) calculateSquare() ([2]int, [2]int) {
	width, height := d.screen.Size()
	height = height - d.opts.LabelHeight

	return [2]int{1, 1}, [2]int{width - 2, height - 2}
}

func (d *terminalDisplay) drawHilbert(baseStyle tcell.Style) {
	coords, size := d.calculateSquare()

	for row := coords[1]; row < coords[1]+size[1]; row++ {
		for col := coords[0]; col < coords[0]+size[0]; col++ {
			//color := tcell.Color((col - coords[0]) % d.screen.Colors())
			style := baseStyle.Background(tcell.ColorBlue)

			d.screen.SetCell(col, row, style, ' ')
		}
	}
}
