package main

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

func TestParseThroughput(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"1MB/s", 1024 * 1024},
		{"1MB", 1024 * 1024},
		{"500KB", 500 * 1024},
		{"1GB/s", 1024 * 1024 * 1024},
		{"1024B", 1024},
		{"2M", 2 * 1024 * 1024},
		{"2m/s", 2 * 1024 * 1024},
		{"1KB", 1024}, // 1KB == 1024, not 1000
		{"1k", 1024},
		{"1g", 1024 * 1024 * 1024},
		{"100", 100}, // bare number = bytes
		{" 1MB/s ", 1024 * 1024},
	}
	for _, c := range cases {
		got, err := parseThroughput(c.in)
		if err != nil {
			t.Errorf("parseThroughput(%q) unexpected error: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("parseThroughput(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestParseThroughputErrors(t *testing.T) {
	bad := []string{
		"",      // empty
		"   ",   // blank
		"/s",    // no number
		"-1MB",  // negative
		"0",     // zero
		"0KB",   // zero
		"abc",   // garbage
		"1XB",   // unknown unit (X leftover -> ParseInt fails)
		"1.5MB", // fractional not supported
		"MB",    // unit only
	}
	for _, in := range bad {
		if got, err := parseThroughput(in); err == nil {
			t.Errorf("parseThroughput(%q) = %d, expected error", in, got)
		}
	}
}

// newValidateCmd builds a cobra command whose flags mirror the real ones so
// tests can toggle Changed("ops") and drive validateFlags in isolation.
func newValidateCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().IntVar(&ops, "ops", 10000, "")
	return cmd
}

// resetFlagState restores the package-level flag globals validateFlags reads,
// so each subtest starts from a clean, valid baseline.
func resetFlagState() {
	dataTypes = []string{"string"}
	ops = 10000
	ratio = "1:0"
	keyPattern = "R"
	keyMin, keyMax = 0, 10000000
	keyZipfExp = ZipfianConstant
	dataSizeRange = ""
	expiryRange = ""
	commands = nil
	commandRatios = nil
	commandKeyPatterns = nil
	throughput = ""
}

func TestValidateThroughput(t *testing.T) {
	t.Run("valid string pure-write", func(t *testing.T) {
		resetFlagState()
		throughput = "1MB/s"
		cmd := newValidateCmd()
		if err := validateFlags(cmd); err != nil {
			t.Fatalf("expected valid, got %v", err)
		}
	})

	t.Run("mutually exclusive with --ops", func(t *testing.T) {
		resetFlagState()
		throughput = "1MB/s"
		cmd := newValidateCmd()
		_ = cmd.Flags().Set("ops", "5000") // marks ops as Changed
		if err := validateFlags(cmd); err == nil {
			t.Fatal("expected error for --throughput + --ops")
		}
	})

	t.Run("rejects command mode", func(t *testing.T) {
		resetFlagState()
		throughput = "1MB/s"
		commands = []string{"SET __key__ __data__"}
		cmd := newValidateCmd()
		if err := validateFlags(cmd); err == nil {
			t.Fatal("expected error for --throughput in command mode")
		}
	})

	t.Run("rejects non-string type", func(t *testing.T) {
		resetFlagState()
		throughput = "1MB/s"
		dataTypes = []string{"list"}
		cmd := newValidateCmd()
		if err := validateFlags(cmd); err == nil {
			t.Fatal("expected error for --throughput with non-string type")
		}
	})

	t.Run("rejects GET weight > 0", func(t *testing.T) {
		resetFlagState()
		throughput = "1MB/s"
		ratio = "1:9"
		cmd := newValidateCmd()
		if err := validateFlags(cmd); err == nil {
			t.Fatal("expected error for --throughput with GET weight")
		}
	})

	t.Run("rejects malformed value", func(t *testing.T) {
		resetFlagState()
		throughput = "notabyte"
		cmd := newValidateCmd()
		if err := validateFlags(cmd); err == nil {
			t.Fatal("expected error for malformed --throughput")
		}
	})
}

// TestNsPerByteDeadlineMonotonic locks the byte-rate deadline computation: for
// an increasing bytesDone sequence the target offset is monotonically
// non-decreasing and does not collapse to zero or overflow, even at very large
// byte counts (1e12+).
func TestNsPerByteDeadlineMonotonic(t *testing.T) {
	// 100 MB/s across 10 workers -> 10 MB/s/worker.
	bytesPerSec := int64(100 * 1024 * 1024)
	clientNum := 10
	perWorkerBps := float64(bytesPerSec) / float64(clientNum)
	nsPerByte := 1e9 / perWorkerBps
	if nsPerByte <= 0 {
		t.Fatalf("nsPerByte = %v, want > 0", nsPerByte)
	}

	var prev float64 = -1
	byteSeq := []int64{0, 1, 1024, 1 << 20, 1 << 30, 1e12, 5e12}
	for _, b := range byteSeq {
		offset := float64(b) * nsPerByte
		if offset < prev {
			t.Fatalf("deadline offset not monotonic at bytes=%d: %v < %v", b, offset, prev)
		}
		if b > 0 && offset <= 0 {
			t.Fatalf("deadline offset collapsed to %v at bytes=%d", offset, b)
		}
		prev = offset
	}
}

// TestFillWriteReturnsValueBytes locks the byte-accounting口径: the size returned
// by fillWrite equals the actual value bytes written (len(op.strVal)) for
// string, and 0 for non-string types. This self-guards the invariant that the
// bandwidth throttle counts real value bytes.
func TestFillWriteReturnsValueBytes(t *testing.T) {
	st := newWorkerState(1, 4096, false)

	t.Run("fixed-size string", func(t *testing.T) {
		w := NewWorkload(WorkloadConfig{
			Type: String, KeyPrefix: "k", KeyMin: 0, KeyMax: 100,
			KeyPattern: "R", ZipfExp: ZipfianConstant, SetWeight: 1, DataSize: 64,
		})
		var op Operation
		for i := 0; i < 100; i++ {
			size := w.fillWrite(st, &op)
			if size != len(op.strVal) {
				t.Fatalf("size %d != len(strVal) %d", size, len(op.strVal))
			}
			if size != 64 {
				t.Fatalf("fixed size = %d, want 64", size)
			}
		}
	})

	t.Run("range-size string", func(t *testing.T) {
		w := NewWorkload(WorkloadConfig{
			Type: String, KeyPrefix: "k", KeyMin: 0, KeyMax: 100,
			KeyPattern: "R", ZipfExp: ZipfianConstant, SetWeight: 1,
			DataSizeMin: 10, DataSizeMax: 100,
		})
		var op Operation
		for i := 0; i < 200; i++ {
			size := w.fillWrite(st, &op)
			if size != len(op.strVal) {
				t.Fatalf("size %d != len(strVal) %d", size, len(op.strVal))
			}
			if size < 10 || size > 100 {
				t.Fatalf("range size = %d, out of [10,100]", size)
			}
		}
	})

	t.Run("non-string returns zero", func(t *testing.T) {
		w := NewWorkload(WorkloadConfig{
			Type: List, KeyPrefix: "k", KeyMin: 0, KeyMax: 100,
			KeyPattern: "R", ZipfExp: ZipfianConstant, SetWeight: 1, FieldNum: 4,
		})
		var op Operation
		if size := w.fillWrite(st, &op); size != 0 {
			t.Fatalf("non-string fillWrite size = %d, want 0", size)
		}
	})
}

// TestAvgValueSize locks the expected-value-size used by the byte-rate startup
// jitter: fixed size returns itself, a range returns its midpoint, and
// non-string types report 0 (they write no value bytes).
func TestAvgValueSize(t *testing.T) {
	fixed := NewWorkload(WorkloadConfig{
		Type: String, KeyPrefix: "k", KeyMin: 0, KeyMax: 1,
		KeyPattern: "R", ZipfExp: ZipfianConstant, SetWeight: 1, DataSize: 128,
	})
	if got := fixed.avgValueSize(); got != 128 {
		t.Errorf("fixed avgValueSize = %v, want 128", got)
	}

	ranged := NewWorkload(WorkloadConfig{
		Type: String, KeyPrefix: "k", KeyMin: 0, KeyMax: 1,
		KeyPattern: "R", ZipfExp: ZipfianConstant, SetWeight: 1,
		DataSizeMin: 10, DataSizeMax: 90,
	})
	if got := ranged.avgValueSize(); got != 50 { // (10+90)/2
		t.Errorf("range avgValueSize = %v, want 50", got)
	}

	nonStr := NewWorkload(WorkloadConfig{
		Type: List, KeyPrefix: "k", KeyMin: 0, KeyMax: 1,
		KeyPattern: "R", ZipfExp: ZipfianConstant, SetWeight: 1, FieldNum: 4,
	})
	if got := nonStr.avgValueSize(); got != 0 {
		t.Errorf("non-string avgValueSize = %v, want 0", got)
	}
}

// TestCommandSpecHasData verifies __data__ detection, which gates byte
// accounting in command mode (only commands that write a value count bytes).
func TestCommandSpecHasData(t *testing.T) {
	kc := NewUniform(0, 100)
	if cs := newCommandSpec("SET __key__ __data__", 1, kc); !cs.hasData {
		t.Error("SET ... __data__ should have hasData=true")
	}
	if cs := newCommandSpec("GET __key__", 1, kc); cs.hasData {
		t.Error("GET __key__ should have hasData=false")
	}
}

// TestBuildReportBytes locks the JSON byte fields: bytes_total mirrors
// s.bytesWritten and mbps is derived with the same 1024-based unit as the flag.
func TestBuildReportBytes(t *testing.T) {
	s := &Sender{addrs: []string{"x:1"}, writeHist: newLatencyHist(), readHist: newLatencyHist()}
	s.bytesWritten.Store(2 * 1024 * 1024) // 2 MiB
	rep := s.buildReport(time.Unix(0, 0), 2*time.Second)
	if rep.BytesTotal != 2*1024*1024 {
		t.Errorf("BytesTotal = %d, want %d", rep.BytesTotal, 2*1024*1024)
	}
	if rep.MBPS != 1.0 { // 2 MiB over 2s = 1 MiB/s
		t.Errorf("MBPS = %v, want 1.0", rep.MBPS)
	}
}

// TestRunConfigLines checks the config renderer emits the expected rows per
// mode and only shows the optional expire/command rows when relevant.
func TestRunConfigLines(t *testing.T) {
	joined := func(lines []string) string { return strings.Join(lines, "\n") }

	t.Run("workload mode", func(t *testing.T) {
		c := &RunConfig{
			Target: "h:1", Clients: 4, Duration: "3s", Pipeline: 16, Mode: "workload",
			DataTypes: "string", Ratio: "1:0", KeyPattern: "R", KeyMin: 0, KeyMax: 100,
			ValueSize: "256 B (fixed)", Throttle: "throughput 512KB/s", Load: false,
		}
		got := joined(c.lines())
		for _, want := range []string{"data-types:  string", "ratio:       1:0", "throttle:    throughput 512KB/s", "value-size:  256 B (fixed)"} {
			if !strings.Contains(got, want) {
				t.Errorf("workload config missing %q in:\n%s", want, got)
			}
		}
		if strings.Contains(got, "command[") {
			t.Error("workload config should not show command rows")
		}
		if strings.Contains(got, "expire:") {
			t.Error("empty expire should be omitted")
		}
	})

	t.Run("command mode with expire", func(t *testing.T) {
		c := &RunConfig{
			Target: "h:1", Clients: 2, Duration: "2s", Pipeline: 8, Mode: "command",
			Commands:   []string{"SET __key__ __data__", "GET __key__"},
			KeyPattern: "Z", KeyMin: 1, KeyMax: 9, ValueSize: "64 B (fixed)",
			Throttle: "10000 ops/s", Load: true, Expire: "30s",
		}
		got := joined(c.lines())
		for _, want := range []string{"command[0]:  SET __key__ __data__", "command[1]:  GET __key__", "expire:      30s", "load:        true"} {
			if !strings.Contains(got, want) {
				t.Errorf("command config missing %q in:\n%s", want, got)
			}
		}
		if strings.Contains(got, "data-types:") {
			t.Error("command config should not show data-types row")
		}
	})
}
