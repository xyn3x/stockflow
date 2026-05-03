package aggregation 

import "sync"

type MovingAverage struct {
	mu 			sync.Mutex
	windowSize 	int 
	windows 	map[string][]float64
	cursors 	map[string]int 
	counts		map[string]int 
}

func NewMovingAverage(WindowSize int) *MovingAverage {
	return &MovingAverage {
		windowSize: 	WindowSize, 
		windows: 		make(map[string][]float64), 
		cursors: 		make(map[string]int),
		counts: 		make(map[string]int),
	}
}

func (ma *MovingAverage) Add(key string, value float64) float64 {
	ma.mu.Lock()
	defer ma.mu.Unlock()

	if _, ok := ma.windows[key]; !ok {
		ma.windows[key] = make([]float64, ma.windowSize)
		ma.cursors[key] = 0 
		ma.counts[key] = 0 
	}

	pos := ma.cursors[key]
	ma.windows[key][pos] = value 
	ma.cursors[key] = (pos + 1) % ma.windowSize 

	if ma.counts[key] < ma.windowSize {
		ma.counts[key]++
	}

	sum := 0.00 
	for i := 0; i < ma.counts[key]; i++ {
		sum += ma.windows[key][i]
	}
	return sum / float64(ma.counts[key])
}

func (ma *MovingAverage) Get(key string) float64 {
	ma.mu.Lock()
	defer ma.mu.Unlock()

	if ma.counts[key] == 0 {
		return 0.00
	}

	sum := 0.00
	for i := 0; i < ma.counts[key]; i++ {
		sum += ma.windows[key][i]
	}
	return sum / float64(ma.counts[key])
}