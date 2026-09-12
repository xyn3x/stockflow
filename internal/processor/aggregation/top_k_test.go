package aggregation

import(
	"reflect"
	"testing"
	"sync"
)

func TestTopK_Add_AccumulatesAndOrders(t *testing.T) {
	tk := NewTopK(2)

	tk.Add("APPL", 100)
	tk.Add("MSFT", 300)
	got := tk.Add("GOOGL", 200)

	want := []Entry {
		{Key: "MSFT", Score: 300},
		{Key: "GOOGL", Score: 200}, 
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("Add() = %+v, want %+v", got, want)
	}
}

func TestTopK_KLargerThanEntries(t *testing.T) {
	tk := NewTopK(10)
	got := tk.Add("APPL", 42)

	if len(got) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(got))
	}
	if got[0].Score != 42 {
		t.Errorf("expected score 42, got %v", got[0].Score)
	}
}

func TestTopK_Results_DoesNotMutateState(t *testing.T) {
	tk := NewTopK(5)
	tk.Add("AAPL", 100)

	r1 := tk.Results()
	r2 := tk.Results()

	if !reflect.DeepEqual(r1, r2) {
		t.Errorf("calling Results() twice should be idempotent: %+v vs %+v", r1, r2)
	}
}

func TestTopK_Add_AccumulatesSameKey(t *testing.T) {
	tk := NewTopK(1)

	tk.Add("AAPL", 100)
	got := tk.Add("AAPL", 50)

	want := []Entry{{ Key: "AAPL", Score: 150 }}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Add() = %+v, want %+v", got, want)
	}
}

func TestTopK_ConcurrentAdds(t *testing.T) {
	tk := NewTopK(5)
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			tk.Add("APPL", 1)
		}(i)
	}
	wg.Wait()

	results := tk.Results()
	if results[0].Score != 100 {
		t.Errorf("expected accumulated score 100, got %v", results[0].Score)
	}
}
