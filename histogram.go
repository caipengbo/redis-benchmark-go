package main

import (
	"sync"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

// latencyHist records batch (pipeline round-trip) latencies using an
// HdrHistogram, which gives high accuracy (3 significant figures) at bounded
// memory.
//
// HdrHistogram is not safe for concurrent use, so a mutex guards it. This is
// cheap here because we record once per batch (ops/pipeline times per second),
// not once per command.
type latencyHist struct {
	mu   sync.Mutex
	hist *hdrhistogram.Histogram
}

func newLatencyHist() *latencyHist {
	return &latencyHist{
		// 1µs .. 24h range, 3 significant figures.
		hist: hdrhistogram.New(1, 24*60*60*1000*1000, 3),
	}
}

func (h *latencyHist) record(d time.Duration) {
	us := d.Microseconds()
	if us < 1 {
		us = 1
	}
	h.mu.Lock()
	_ = h.hist.RecordValue(us)
	h.mu.Unlock()
}

// percentile returns the latency at percentile p (p in 0..1).
func (h *latencyHist) percentile(p float64) time.Duration {
	h.mu.Lock()
	v := h.hist.ValueAtPercentile(p * 100)
	h.mu.Unlock()
	return time.Duration(v) * time.Microsecond
}

// stats returns cumulative min / mean / max latency and the sample count.
func (h *latencyHist) stats() (min, mean, max time.Duration, count int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return time.Duration(h.hist.Min()) * time.Microsecond,
		time.Duration(int64(h.hist.Mean())) * time.Microsecond,
		time.Duration(h.hist.Max()) * time.Microsecond,
		h.hist.TotalCount()
}

// exportSnapshot returns the HdrHistogram snapshot (fixed-size bucket counts +
// config), used to serialize a mergeable histogram.
func (h *latencyHist) exportSnapshot() *hdrhistogram.Snapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.hist.Export()
}
