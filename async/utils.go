package async

import (
	"context"

	"time"
)

func TickerLog(d time.Duration, ctx context.Context, logfn func()) {
	go func() {
		ticker := time.NewTicker(d)
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				logfn()
			}
		}
	}()
}

func drainString(c <-chan string) {
	pctx, _ := context.WithTimeout(context.Background(), time.Second)
	dctx, canc := context.WithCancel(context.Background())
	go func() {
		for w := range c {
			byebyebye(w)
		}
		canc()
	}()

	select {
	case <-pctx.Done():
		panic("drainStruct didn't finish in time, missing channel close")
	case <-dctx.Done():
		return
	}
}

func drainStruct(c <-chan struct{}) {
	pctx, _ := context.WithTimeout(context.Background(), time.Second)
	dctx, canc := context.WithCancel(context.Background())
	go func() {
		for w := range c {
			byebyebye(w)
		}
		canc()
	}()

	select {
	case <-pctx.Done():
		panic("drainStruct didn't finish in time, missing channel close")
	case <-dctx.Done():
		return
	}
}

func byebyebye(n interface{}) {}
