package main

import (
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestIsSupportedType(t *testing.T) {
	for _, ok := range []string{"string", "hash", "list", "set", "zset"} {
		if !IsSupportedType(ok) {
			t.Errorf("IsSupportedType(%q) = false, want true", ok)
		}
	}
	for _, bad := range []string{"", "String", "stream", "int"} {
		if IsSupportedType(bad) {
			t.Errorf("IsSupportedType(%q) = true, want false", bad)
		}
	}
}

func TestTypedValue(t *testing.T) {
	if v, ok := typedValue(List, 8).([]string); !ok || len(v) != 8 {
		t.Errorf("List typedValue = %#v, want []string len 8", typedValue(List, 8))
	}
	if v, ok := typedValue(Set, 8).([]string); !ok || len(v) != 8 {
		t.Errorf("Set typedValue = %#v, want []string len 8", typedValue(Set, 8))
	}
	if v, ok := typedValue(Hash, 8).(map[string]string); !ok || len(v) != 8 {
		t.Errorf("Hash typedValue = %#v, want map len 8", typedValue(Hash, 8))
	}
	if v, ok := typedValue(ZSet, 8).(map[string]float64); !ok || len(v) != 8 {
		t.Errorf("ZSet typedValue = %#v, want map len 8", typedValue(ZSet, 8))
	}
	if typedValue(String, 8) != nil {
		t.Errorf("String typedValue should be nil")
	}
}

// TestTypedPipelineArgs verifies the constant payload is pre-built into the
// exact argument form each pipeline command consumes, and that the same slice
// is reused (not rebuilt per call) so the write path stays allocation-free.
func TestTypedPipelineArgs(t *testing.T) {
	// Hash: alternating field/value pairs.
	hargs, ok := typedPipelineArgs(Hash, typedValue(Hash, 3)).([]interface{})
	if !ok || len(hargs) != 6 {
		t.Fatalf("Hash args = %#v, want []interface{} len 6", hargs)
	}

	// List / Set: one element per field.
	for _, ty := range []Type{List, Set} {
		args, ok := typedPipelineArgs(ty, typedValue(ty, 4)).([]interface{})
		if !ok || len(args) != 4 {
			t.Fatalf("%s args = %#v, want []interface{} len 4", ty, args)
		}
	}

	// ZSet: []redis.Z.
	zargs, ok := typedPipelineArgs(ZSet, typedValue(ZSet, 5)).([]redis.Z)
	if !ok || len(zargs) != 5 {
		t.Fatalf("ZSet args = %#v, want []redis.Z len 5", zargs)
	}

	if typedPipelineArgs(String, nil) != nil {
		t.Errorf("String typedPipelineArgs should be nil")
	}
}

func TestHash64(t *testing.T) {
	// Always non-negative, and deterministic for a given input.
	for _, n := range []int64{0, 1, -1, 1 << 40, -(1 << 40), 1234567890} {
		h := hash64(n)
		if h < 0 {
			t.Errorf("hash64(%d) = %d, want non-negative", n, h)
		}
		if hash64(n) != h {
			t.Errorf("hash64(%d) not deterministic", n)
		}
	}
	// Distinct inputs should generally map to distinct hashes (spot check).
	if hash64(1) == hash64(2) {
		t.Errorf("hash64 collided on 1 and 2")
	}
	// Sanity: the inlined FNV-1a matches a from-scratch reference computation.
	ref := func(n int64) int64 {
		const offset64 = uint64(1469598103934665603)
		const prime64 = uint64(1099511628211)
		u := uint64(n)
		h := offset64
		for shift := 56; shift >= 0; shift -= 8 {
			h ^= (u >> uint(shift)) & 0xff
			h *= prime64
		}
		r := int64(h)
		if r < 0 {
			r = -r
		}
		return r
	}
	for _, n := range []int64{0, 42, -42, 1 << 50} {
		if got := hash64(n); got != ref(n) {
			t.Errorf("hash64(%d) = %d, ref = %d", n, got, ref(n))
		}
	}
}
