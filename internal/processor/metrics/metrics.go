package metrics 

import(
	"sync"
	"sync/atomic"
	"time"
)

type Throughput struct {
	mu 			sync.Mutex 
	total 		atomic.Uint64 
	window 		atomic.Uint64
	windowTs 	time.Time 
}

func NewTroughput() *Throughput {
	return &Throughput{ windowTs: time.Now() }
}

func (t *Throughput) Inc() float64 {
	t.total.Add(1)
	t.window.Add(1)

	t.mu.Lock()
	defer t.mu.Unlock()

	elapsed := time.Since(t.windowTs).Seconds()
	rate := float64(t.window.Load()) / elapsed
	if elapsed < 1.0 {
		return rate 
	}
	t.window.Store(0)
	t.windowTs = time.Now()
	return rate 
}

func (t *Throughput) Total() uint64 {
	return t.total.Load()
}

type Latency struct {
	mu 		sync.Mutex 
	count 	int 
	sum 	time.Duration 
	max 	time.Duration 
}

func NewLatency() *Latency {
	return &Latency{}
}

func (l *Latency) Record(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.count++
	l.sum += d
	if d > l.max {
		l.max = d 
	}
}

func (l *Latency) Stats() (avg, max time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.count == 0 {
		return 0, 0
	}
	return l.sum / time.Duration(l.count), l.max
}