package aggregation 

import(
	"sync"
	"math"
)

type welfordState struct {
	count 	int 
	mean 	float64 
	m2 		float64 
}

type Volatility struct {
	mu 		sync.Mutex 
	states 	map[string] *welfordState
}

func NewVolatility() *Volatility { 
	return &Volatility {
		states: make(map[string] *welfordState),
	}
}

func (v *Volatility) Add(key string, value float64) float64 {
	v.mu.Lock()
	defer v.mu.Unlock()

	st, ok := v.states[key]
	if !ok {
		st = &welfordState{}
		v.states[key] = st 
	}

	st.count++
	delta := value - st.mean 
	st.mean += delta / float64(st.count)
	delta2 := value - st.mean 
	st.m2 += delta * delta2

	if st.count < 2 {
		return 0
	}
	variance := st.m2 / float64(st.count - 1)
	return math.Sqrt(variance)
}

func (v *Volatility) Get(key string) float64 {
	v.mu.Lock()
	defer v.mu.Unlock()

	st, ok := v.states[key]
	if !ok || st.count < 2 {
		return 0
	}
	return math.Sqrt(st.m2 / float64(st.count - 1))
}