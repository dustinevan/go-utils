package stream

import (
	"context"
	"log"
	"sync"
)

type Monitored interface {
	ListenErr() <-chan error
	ListenStats() <-chan string
	Wait()
	Cancel()
}

type MonitoringType int

const (
	LogAll MonitoringType = iota
	CancelOnErr
)

type Monitor struct {
	ctx context.Context

	cancs []func()
	canceled bool

	waits []func()

	mu sync.Mutex
}

func NewMonitor(ctx context.Context) *Monitor {
	m := &Monitor{
		ctx: ctx,
		cancs: make([]func(), 0),
	}

	go func() {
		<-ctx.Done()
		m.Cancel()
	}()

	return m
}

func (m *Monitor) Register(md Monitored, t MonitoringType) {
	if t == CancelOnErr {
		m.cancelOnErr(md)
	}
	if t== LogAll {
		go func() {
			stats := md.ListenStats()
			errs := md.ListenErr()
			for s := range stats {
				log.Println(s)
			}
			for e := range errs {
				log.Println(e)
			}
		}()
	}
}

func (m *Monitor) cancelOnErr(md Monitored) {
	m.addCancelFn(md.Cancel)
	go func() {
		errs := md.ListenErr()
		for e := range errs {
			log.Println(e.Error())
			m.Cancel()
			return
		}
	}()
	go func() {
		stats := md.ListenStats()
		for s := range stats {
			log.Println(s)
		}
	}()
}

func (m *Monitor) addCancelFn(fn func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.canceled == true {
		fn()
		return
	}
	m.cancs = append(m.cancs, fn)
}

func (m *Monitor) Cancel() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.canceled == true {
		panic("Monitor: cancel called twice" )
	}
	for _, c := range m.cancs {
		c()
	}
	m.canceled = true
}
