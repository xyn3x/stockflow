package aggregation

import(
	"math"
	"testing"
	"sync"
)

func TestVolatility_MatchesNaiveStdDev(t *testing.T) {
	values := []float64{100, 102, 99, 105, 101, 98, 103}

	v := NewVolatility()
	var last float64 
	for _, val := range values {
		last = v.Add("APPL", val)
	}

	mean := 0.0 
	for _, val := range values {
		mean += val
	}
	mean /= float64(len(values))

	sumSq := 0.0
	for _, val := range values {
		sumSq += (val - mean) * (val - mean)
	}
	wantStdDev := math.Sqrt(sumSq / float64(len(values) - 1))

	if !almostEqual(last, wantStdDev, 1e-9) {
		t.Errorf("Volatility.Add final = %v, want %v", last, wantStdDev)
	}
}

func TestVolatility_FewerThanTwoPoints(t *testing.T) {
	v := NewVolatility()
	if got := v.Add("APPL", 100); got != 0 {
		t.Errorf("First Add() should return 0, got %v", got)
	}
}


func TestVolatility_ConcurrentAdds(t *testing.T) {
	v := NewVolatility()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			v.Add("APPL", 100)
		}(i)
	}
	wg.Wait()

	result := v.Get("APPL")
	if result != 0 {
		t.Errorf("stdev of 100 same values should be 0, got %v", result)
	}
}