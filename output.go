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
	Config     *RunConfig             `json:"config,omitempty"`
	OpsTotal   int64                  `json:"ops_total"`
	QPS        float64                `json:"qps"`
	BytesTotal int64                  `json:"bytes_total"`
	MBPS       float64                `json:"mbps"`
	Metrics    map[string]*metricJSON `json:"metrics"`
}

// RunConfig captures the effective run parameters (after preset-file and flag
// resolution) so a run is self-documenting: printed to stderr at startup and
// embedded in both the text summary and the JSON report.
type RunConfig struct {
	Target     string   `json:"target"`
	Clients    int      `json:"clients"`
	Duration   string   `json:"duration"`
	Pipeline   int      `json:"pipeline"`
	Mode       string   `json:"mode"`
	DataTypes  string   `json:"data_types,omitempty"`
	Commands   []string `json:"commands,omitempty"`
	Ratio      string   `json:"ratio,omitempty"`
	KeyPattern string   `json:"key_pattern"`
	KeyMin     int64    `json:"key_min"`
	KeyMax     int64    `json:"key_max"`
	ValueSize  string   `json:"value_size"`
	Throttle   string   `json:"throttle"`
	Load       bool     `json:"load"`
	Expire     string   `json:"expire,omitempty"`
}

// lines renders the config as aligned "  key: value" rows for the text output.
func (c *RunConfig) lines() []string {
	out := []string{
		fmt.Sprintf("  target:      %s", c.Target),
		fmt.Sprintf("  clients:     %d", c.Clients),
		fmt.Sprintf("  duration:    %s", c.Duration),
		fmt.Sprintf("  pipeline:    %d", c.Pipeline),
		fmt.Sprintf("  mode:        %s", c.Mode),
	}
	if c.Mode == "command" {
		for i, cmd := range c.Commands {
			out = append(out, fmt.Sprintf("  command[%d]:  %s", i, cmd))
		}
	} else {
		out = append(out, fmt.Sprintf("  data-types:  %s", c.DataTypes))
		out = append(out, fmt.Sprintf("  ratio:       %s", c.Ratio))
	}
	out = append(out,
		fmt.Sprintf("  key-pattern: %s", c.KeyPattern),
		fmt.Sprintf("  key-range:   [%d, %d]", c.KeyMin, c.KeyMax),
		fmt.Sprintf("  value-size:  %s", c.ValueSize),
		fmt.Sprintf("  throttle:    %s", c.Throttle),
		fmt.Sprintf("  load:        %t", c.Load),
	)
	if c.Expire != "" {
		out = append(out, fmt.Sprintf("  expire:      %s", c.Expire))
	}
	return out
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
		BytesTotal: s.bytesWritten.Load(),
		Metrics:    map[string]*metricJSON{},
	}
	if elapsed.Seconds() > 0 {
		rep.QPS = float64(rep.OpsTotal) / elapsed.Seconds()
		rep.MBPS = float64(rep.BytesTotal) / elapsed.Seconds() / (1024 * 1024)
	}
	if s.cmdMode() {
		rep.Mode = "command"
	} else {
		rep.Mode = "workload"
	}
	rep.Config = s.config
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
