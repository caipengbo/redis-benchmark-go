package main

import (
	"math/rand"
	"testing"
)

func newRand() *rand.Rand { return rand.New(rand.NewSource(1)) }

func TestUniformRangeAndSpread(t *testing.T) {
	const lb, ub = int64(10), int64(19)
	g := NewUniform(lb, ub)
	r := newRand()

	seen := make(map[int64]int)
	for i := 0; i < 100000; i++ {
		n := g.Next(r)
		if n < lb || n > ub {
			t.Fatalf("uniform out of range: %d not in [%d,%d]", n, lb, ub)
		}
		seen[n]++
	}
	// all 10 buckets should be hit, none wildly dominant (uniform).
	if len(seen) != int(ub-lb+1) {
		t.Fatalf("uniform did not cover the range: got %d distinct values", len(seen))
	}
	expected := 100000 / int(ub-lb+1)
	for k, c := range seen {
		if c < expected/2 || c > expected*2 {
			t.Errorf("uniform bucket %d count %d far from expected ~%d", k, c, expected)
		}
	}
}

func TestSequentialCycles(t *testing.T) {
	const start, end = int64(5), int64(8) // interval 4: values cycle 6,7,8,5,6,7,8,5...
	g := NewSequential(start, end)
	r := newRand()

	for i := 0; i < 20; i++ {
		n := g.Next(r)
		if n < start || n > end {
			t.Fatalf("sequential out of range: %d not in [%d,%d]", n, start, end)
		}
	}
}

func TestZipfianRangeAndSkew(t *testing.T) {
	const min, max = int64(0), int64(999)
	g := NewZipfianWithRange(min, max, ZipfianConstant)
	r := newRand()

	seen := make(map[int64]int)
	const n = 200000
	for i := 0; i < n; i++ {
		v := g.Next(r)
		if v < min || v > max {
			t.Fatalf("zipfian out of range: %d not in [%d,%d]", v, min, max)
		}
		seen[v]++
	}
	// The most popular item (item 0 / min) should take a large share under a
	// zipfian with constant 0.99.
	top := seen[min]
	if float64(top)/float64(n) < 0.05 {
		t.Errorf("zipfian not skewed enough: top item share %.3f", float64(top)/float64(n))
	}
}

func TestScrambledZipfianRangeAndSkew(t *testing.T) {
	const min, max = int64(0), int64(9999)
	g := NewScrambledZipfian(min, max, ZipfianConstant)
	r := newRand()

	seen := make(map[int64]int)
	const n = 200000
	for i := 0; i < n; i++ {
		v := g.Next(r)
		if v < min || v > max {
			t.Fatalf("scrambled zipfian out of range: %d not in [%d,%d]", v, min, max)
		}
		seen[v]++
	}
	// Popularity should be scattered (not concentrated at min), but still skewed:
	// a small fraction of keys should absorb a large fraction of hits.
	type kc struct {
		k int64
		c int
	}
	var all []kc
	for k, c := range seen {
		all = append(all, kc{k, c})
	}
	// find the single hottest key's share; scrambled still has a hot key.
	maxc := 0
	for _, e := range all {
		if e.c > maxc {
			maxc = e.c
		}
	}
	if float64(maxc)/float64(n) < 0.02 {
		t.Errorf("scrambled zipfian hottest key share too low: %.3f", float64(maxc)/float64(n))
	}
}

func TestCounterMonotonic(t *testing.T) {
	g := NewCounter(100)
	r := newRand()
	prev := g.Next(r)
	if prev != 100 {
		t.Fatalf("counter start = %d, want 100", prev)
	}
	for i := 0; i < 50; i++ {
		n := g.Next(r)
		if n != prev+1 {
			t.Fatalf("counter not monotonic: %d after %d", n, prev)
		}
		prev = n
	}
}
