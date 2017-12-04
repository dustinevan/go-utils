package stream

import (
	"context"
	"sync"

	"github.com/pkg/errors"
)

var STAT_FULL = errors.New("max number of inflight stats messages reached")

// A monitor is used to pass stats, error, and success/failure information about a stream parts
// back to stream parent
type Monitor struct {
	success bool

	stats        chan string
	statinflight int
	statlimit    int

	errs        chan error
	errinflight int
	errlimit    int

	mu sync.Mutex

	routinewg   *sync.WaitGroup
	routinecanc context.CancelFunc
}

func NewMonitor(routinewg *sync.WaitGroup, routinecanc context.CancelFunc) *Monitor {
	m := &Monitor{
		errs:      make(chan error, 16),
		stats:     make(chan string, 16),
		statlimit: 16,
		errlimit:  16,

		routinewg:   routinewg,
		routinecanc: routinecanc,
	}

	go func() {
		m.routinewg.Wait()
		close(m.errs)
		close(m.stats)
	}()

	return m
}

// write methods are used by the thing being monitored. non-blocking
func (m *Monitor) SubmitStat(s string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.statlimit == m.statinflight {
		return STAT_FULL
	}
	m.stats <- s
	m.statinflight++
	return nil
}

// write methods are used by the thing being monitored. non-blocking
func (m *Monitor) SubmitErr(e error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.errlimit == m.errinflight {
		return STAT_FULL
	}
	m.errs <- e
	m.errinflight++
	return nil
}

// returns the stat channel
func (m *Monitor) ReadStats() <-chan string {
	return m.stats
}

func (m *Monitor) ReadErrors() <-chan error {
	return m.errs
}

func (m *Monitor) SetSuccess(bool bool) {
	m.success = bool
}

func (m *Monitor) CancelRoutine() {
	m.routinecanc()
}

func (m *Monitor) GetSuccess() bool {
	m.routinewg.Wait()
	return m.success
}
