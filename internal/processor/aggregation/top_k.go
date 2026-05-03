package aggregation

import(
	"sync"
	"sort"
)

type Entry struct {
	Key 	string 
	Score 	float64
}

type TopK struct {
	mu 		sync.Mutex 
	k 		int 
	scores 	map[string] float64
} 

func NewTopK(k int) *TopK {
	return &TopK {
		k : 	k, 
		scores: make(map[string] float64),
	}
}

func (t *TopK) Add(key string, value float64) []Entry {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.scores[key] += value 

	return t.topK()
}

func (t *TopK) Results() []Entry {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.topK()
}

func (t *TopK) topK() []Entry {
	entries := make([]Entry, 0, len(t.scores))

	for k, sc := range t.scores {
		entries = append(entries, Entry{Key: k, Score: sc})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Score > entries[j].Score
	})
	if len(entries) > t.k {
		entries = entries[:t.k]
	}
	
	return entries
}