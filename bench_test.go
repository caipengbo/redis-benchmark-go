package main

import (
	"math/rand"
	"testing"
)

// BenchmarkScrambledZipfianNext measures the default Z-path key generator,
// which previously allocated an fnv hasher and wrote a shared atomic per call.
func BenchmarkScrambledZipfianNext(b *testing.B) {
	g := NewScrambledZipfian(0, 10_000_000, ZipfianConstant)
	r := rand.New(rand.NewSource(1))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = g.Next(r)
	}
}

// BenchmarkUniformNext measures the default R-path key generator.
func BenchmarkUniformNext(b *testing.B) {
	g := NewUniform(0, 10_000_000)
	r := rand.New(rand.NewSource(1))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = g.Next(r)
	}
}

// BenchmarkHash64 isolates the inlined FNV-1a.
func BenchmarkHash64(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = hash64(int64(i))
	}
}
