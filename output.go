package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

// metricJSON is the per-operation summary in the human-readable JSON output.
type metricJSON struct {
	Count  int64 `json:"count"`
	Miss   int64 `json:"miss,omitempty"`
	MinUS  int64 `json:"min_us"`
	MeanUS int64 `json:"mean_us"`
	MaxUS  int64 `json:"max_us"`
	P50US  int64 `json:"p50_us"`
	P99US  int64 `json:"p99_us"`
	P999US int64 `json:"p999_us"`
}

type runReport struct {
	Target     string                 `json:"target"`
	Start      string                 `json:"start"`
	End        string                 `json:"end"`
	ElapsedSec float64                `json:"elapsed_sec"`
	Mode       string                 `json:"mode"`
	OpsTotal   int64                  `json:"ops_total"`
	QPS        float64                `json:"qps"`
	Metrics    map[string]*metricJSON `json:"metrics"`
}

func metricFromHist(h *latencyHist, count, miss int64) *metricJSON {
	min, mean, max, _ := h.stats()
	return &metricJSON{
		Count:  count,
		Miss:   miss,
		MinUS:  min.Microseconds(),
		MeanUS: mean.Microseconds(),
		MaxUS:  max.Microseconds(),
		P50US:  h.percentile(0.50).Microseconds(),
		P99US:  h.percentile(0.99).Microseconds(),
		P999US: h.percentile(0.999).Microseconds(),
	}
}

// namedHist pairs a metric name (used as the .hlog Tag) with its histogram.
type namedHist struct {
	name string
	h    *latencyHist
	miss int64
	cnt  int64
}

func (s *Sender) metricHists() []namedHist {
	if s.cmdMode() {
		out := make([]namedHist, 0, len(s.commands))
		for _, cs := range s.commands {
			out = append(out, namedHist{name: cs.name, h: cs.hist, cnt: cs.count.Load()})
		}
		return out
	}
	out := []namedHist{{name: "WRITE", h: s.writeHist, cnt: s.writeCount.Load()}}
	if s.readCount.Load() > 0 {
		out = append(out, namedHist{name: "READ", h: s.readHist, cnt: s.readCount.Load(), miss: s.missCount.Load()})
	}
	return out
}

func (s *Sender) buildReport(start time.Time, elapsed time.Duration) *runReport {
	rep := &runReport{
		Target:     strings.Join(s.addrs, ","),
		Start:      start.UTC().Format(time.RFC3339),
		End:        start.Add(elapsed).UTC().Format(time.RFC3339),
		ElapsedSec: elapsed.Seconds(),
		OpsTotal:   s.counter.Load(),
		Metrics:    map[string]*metricJSON{},
	}
	if elapsed.Seconds() > 0 {
		rep.QPS = float64(rep.OpsTotal) / elapsed.Seconds()
	}
	if s.cmdMode() {
		rep.Mode = "command"
	} else {
		rep.Mode = "workload"
	}
	for i, m := range s.metricHists() {
		key := m.name
		if _, dup := rep.Metrics[key]; dup {
			key = fmt.Sprintf("%s#%d", m.name, i)
		}
		rep.Metrics[key] = metricFromHist(m.h, m.cnt, m.miss)
	}
	return rep
}

// APPEND-WRITERS

func writeJSONFile(path string, v any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// writeHlog writes a standard HdrHistogram interval log (.hlog): one tagged
// interval-histogram per metric, V2-encoded. This is the cross-language,
// cross-tool interchange format — any HdrHistogram library (Java/Go/Rust/
// Python/JS...) or the official plotter can read and merge it.
func (s *Sender) writeHlog(path string, start time.Time, elapsed time.Duration) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := hdrhistogram.NewHistogramLogWriter(f)
	if err := w.OutputLogFormatVersion(); err != nil {
		return err
	}
	startMs := start.UnixMilli()
	if err := w.OutputStartTime(startMs); err != nil {
		return err
	}
	if err := w.OutputLegend(); err != nil {
		return err
	}
	endMs := start.Add(elapsed).UnixMilli()

	for _, m := range s.metricHists() {
		// Import a copy so tagging/timestamps don't mutate the live histogram.
		h := hdrhistogram.Import(m.h.exportSnapshot())
		h.SetTag(m.name)
		h.SetStartTimeMs(startMs)
		h.SetEndTimeMs(endMs)
		if err := w.OutputIntervalHistogram(h); err != nil {
			return err
		}
	}
	return nil
}

func (s *Sender) writeOutputs(start time.Time, elapsed time.Duration) {
	if s.jsonOut != "" {
		if err := writeJSONFile(s.jsonOut, s.buildReport(start, elapsed)); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "write json-out: %v\n", err)
		} else {
			_, _ = fmt.Fprintf(os.Stderr, "json summary written: %s\n", s.jsonOut)
		}
	}
	if s.histOut != "" {
		if err := s.writeHlog(s.histOut, start, elapsed); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "write hist-out: %v\n", err)
		} else {
			_, _ = fmt.Fprintf(os.Stderr, "HdrHistogram .hlog written: %s\n", s.histOut)
		}
	}
}
