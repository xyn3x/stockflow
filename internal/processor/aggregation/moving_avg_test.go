package aggregation

import(
	"math"
	"testing"
	"sync"
)

func almostEqual(a, b, eps float64) bool {
	return math.Abs(a - b) < eps 
}

func TestMovingAverage_Add(t *testing.T) {
	tests := []struct {
		name		string 
		windowSize	int
		values 		[]float64 
		wantLast	float64
	}{
		{
			name: 		"fewer values than window",
			windowSize:	5, 
			values:		[]float64{10, 20, 30},
			wantLast:	20,
		},
		{
			name:		"exactly fills window",
			windowSize:	3, 
			values:		[]float64{10, 20, 30},
			wantLast:	20, 
		},
		{
			name:		"window wraps and drop oldest",
			windowSize:	3, 
			values:		[]float64{10, 20, 30, 60},
			wantLast:	36.66666666664, 
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ma := NewMovingAverage(tc.windowSize)
			var last float64 
			for _, v := range tc.values {
				last = ma.Add("APPL", v)
			}
			if !almostEqual(last, tc.wantLast, 1e-9) {
				t.Errorf("got %v, want %v", last, tc.wantLast)
			}
		})
	}
}

func TestMovingAverage_Get_EmptyKeyReturnsZero(t *testing.T) {
	ma := NewMovingAverage(3)
	if got := ma.Get("UNKNOWN"); got != 0 {
		t.Errorf("Get() on unknown key = %v, want 0", got)
	}
}

func TestMovingAverage_ConcurrentAdds(t *testing.T) {
	ma := NewMovingAverage(10)
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			ma.Add("APPL", 5)
		}(i)
	}
	wg.Wait()

	result := ma.Get("APPL")
	if result != 5 {
		t.Errorf("expected score 5, got %v", result)
	}
}
