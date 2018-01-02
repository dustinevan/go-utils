package stream

import (
	"context"
	"sync"

	"log"

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

func JoinMonitors(ms ...*Monitor) *Monitor {

	var wg sync.WaitGroup
	p := &Monitor{
		errs:         make(chan error, 1024),
		stats:        make(chan string, 1024),
		statlimit:    1024,
		errlimit:     1024,
		routinewg: &wg,
	}

	for _, m := range ms {
		p.routinewg.Add(1)
		go func() {
			defer p.routinewg.Done()
			for e := range m.ReadErrors() {
				p.SubmitErr(e)
			}
		}()
		p.routinewg.Add(1)
		go func() {
			defer p.routinewg.Done()
			for s := range m.ReadStats() {
				p.SubmitStat(s)
			}
		}()
	}

	p.routinecanc = func() {
		for _, m := range ms {
			m.routinecanc()
		}
	}

	// this wait group waits on each of the underlying monitor wgs.
	// after it completes a goroutine that checks the combined success
	// unblocks
	var internalwg sync.WaitGroup
	for _, m := range ms {
		internalwg.Add(1)
		go func() {
			defer internalwg.Done()
			m.routinewg.Wait()
		}()
	}

	// when all the underlying monitors complete, check the combined
	// success or failure.
	p.routinewg.Add(1)
	go func() {
		internalwg.Wait()
		p.success = true
		for _, m := range ms {
			p.success = p.success && m.success
		}
		p.routinewg.Done()
	}()

	go func() {
		p.routinewg.Wait()
		close(p.errs)
		close(p.stats)
	}()

	return p
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

func (m *Monitor) Log() {
	go func() {
		for s := range m.ReadStats() {
			log.Println(s)
		}
	}()
	go func() {
		for e := range m.ReadErrors() {
			log.Println(e)
		}
	}()
}

func (m *Monitor) CancelOnErr(canc func()) {
	go func() {
		for e := range m.ReadErrors() {
			log.Println(e)
			canc()
			return
		}
	}()
}
