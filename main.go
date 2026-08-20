package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

const version = "0.0.1"

var (
	versionFlag bool
	addresses   []string
	password    string
	clientsNum  int
	duration    string
	dataTypes   []string
	pipeline    int
	fieldsNum   int
	ops         int
	keyPrefix   string
	expire      string

	// workload options
	ratio         string
	keyPattern    string
	keyMin        int64
	keyMax        int64
	keyZipfExp    float64
	zeroPadding   int
	dataSize      int
	dataSizeRange string
	randomData    bool
	expiryRange   string
	loadFlag      bool
	workloadFile  string
	jsonOut       string
	histOut       string
	throughput    string

	// arbitrary command mode
	commands           []string
	commandRatios      []int
	commandKeyPatterns []string

	rootCmd = &cobra.Command{
		Use:   "redis-benchmark-go",
		Short: "redis-benchmark",
		Long:  "A redis benchmark tool",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if workloadFile != "" {
				if err := applyWorkloadFile(cmd, workloadFile); err != nil {
					return err
				}
			}
			return validateFlags(cmd)
		},
		Run: func(cmd *cobra.Command, args []string) {
			if versionFlag {
				fmt.Println(fmt.Sprintf("redis-benchmark-go v%s", version))
				os.Exit(0)
			}
			run()
		},
	}
)

// applyWorkloadFile loads internal parameters (key=value, one per line, "#"
// comments) from a preset file. A value is applied only if the matching flag
// was not set on the command line, so CLI flags always override the file.
func applyWorkloadFile(cmd *cobra.Command, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			return fmt.Errorf("workload file line %d: expected key=value, got %q", lineNo, line)
		}
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		if cmd.Flags().Lookup(k) == nil {
			return fmt.Errorf("workload file line %d: unknown parameter %q", lineNo, k)
		}
		if cmd.Flags().Changed(k) {
			continue // CLI overrides the file
		}
		if err := cmd.Flags().Set(k, v); err != nil {
			return fmt.Errorf("workload file line %d: %v", lineNo, err)
		}
	}
	return scanner.Err()
}

func validateFlags(cmd *cobra.Command) error {
	for i := 0; i < len(dataTypes); i++ {
		if !IsSupportedType(strings.ToLower(dataTypes[i])) {
			return fmt.Errorf("unsupported data type: %s", dataTypes[i])
		}
		dataTypes[i] = strings.ToLower(dataTypes[i])
	}
	if ops < 0 {
		return fmt.Errorf("invalid ops: %d", ops)
	}
	if _, _, err := parseRatio(ratio); err != nil {
		return err
	}
	switch strings.ToUpper(keyPattern) {
	case "R", "S", "Z":
	default:
		return fmt.Errorf("invalid key-pattern %q (allowed: R, S, Z)", keyPattern)
	}
	if keyMax < keyMin {
		return fmt.Errorf("key-maximum (%d) must be >= key-minimum (%d)", keyMax, keyMin)
	}
	if keyZipfExp <= 0 || keyZipfExp >= 5 {
		return errors.New("key-zipf-exp must be within (0, 5)")
	}
	if dataSizeRange != "" {
		if _, _, err := parseIntRange(dataSizeRange); err != nil {
			return fmt.Errorf("data-size-range: %v", err)
		}
	}
	if expiryRange != "" {
		if _, _, err := parseIntRange(expiryRange); err != nil {
			return fmt.Errorf("expiry-range: %v", err)
		}
	}
	if len(commands) > 0 {
		if len(commandRatios) > len(commands) {
			return fmt.Errorf("more --command-ratio (%d) than --command (%d)", len(commandRatios), len(commands))
		}
		if len(commandKeyPatterns) > len(commands) {
			return fmt.Errorf("more --command-key-pattern (%d) than --command (%d)", len(commandKeyPatterns), len(commands))
		}
		for i, c := range commands {
			if len(strings.Fields(c)) == 0 {
				return fmt.Errorf("--command %d is empty", i)
			}
		}
		for _, r := range commandRatios {
			if r < 1 {
				return fmt.Errorf("--command-ratio must be >= 1")
			}
		}
		for _, p := range commandKeyPatterns {
			switch strings.ToUpper(p) {
			case "R", "S", "Z":
			default:
				return fmt.Errorf("invalid --command-key-pattern %q (allowed: R, S, Z)", p)
			}
		}
	}
	if throughput != "" {
		if _, err := parseThroughput(throughput); err != nil {
			return fmt.Errorf("throughput: %v", err)
		}
		// --throughput throttles written value bytes; it is mutually exclusive
		// with --ops and only meaningful for string pure-write workloads.
		if cmd.Flags().Changed("ops") {
			return errors.New("--throughput is mutually exclusive with --ops")
		}
		if len(commands) > 0 {
			return errors.New("--throughput is not supported in command mode")
		}
		for _, t := range dataTypes {
			if t != "" && Type(t) != String {
				return fmt.Errorf("--throughput only supports the string type (got %q)", t)
			}
		}
		if _, getW, err := parseRatio(ratio); err == nil && getW > 0 {
			return errors.New("--throughput requires a pure-write ratio (GET weight must be 0)")
		}
	}
	return nil
}

// parseThroughput parses a byte-rate string like "1MB/s", "500KB", "2M" or
// "1024B" into bytes per second. Units are B/KB/MB/GB (case-insensitive,
// 1KB=1024); a trailing "/s" is optional and ignored (the value is always a
// per-second rate). A bare number with no unit is treated as bytes.
func parseThroughput(s string) (int64, error) {
	str := strings.TrimSpace(s)
	if str == "" {
		return 0, fmt.Errorf("empty value")
	}
	lower := strings.ToLower(str)
	lower = strings.TrimSuffix(lower, "/s")
	lower = strings.TrimSpace(lower)
	if lower == "" {
		return 0, fmt.Errorf("invalid value %q", s)
	}

	var mult int64 = 1
	switch {
	case strings.HasSuffix(lower, "gb"):
		mult, lower = 1024*1024*1024, strings.TrimSuffix(lower, "gb")
	case strings.HasSuffix(lower, "mb"):
		mult, lower = 1024*1024, strings.TrimSuffix(lower, "mb")
	case strings.HasSuffix(lower, "kb"):
		mult, lower = 1024, strings.TrimSuffix(lower, "kb")
	case strings.HasSuffix(lower, "g"):
		mult, lower = 1024*1024*1024, strings.TrimSuffix(lower, "g")
	case strings.HasSuffix(lower, "m"):
		mult, lower = 1024*1024, strings.TrimSuffix(lower, "m")
	case strings.HasSuffix(lower, "k"):
		mult, lower = 1024, strings.TrimSuffix(lower, "k")
	case strings.HasSuffix(lower, "b"):
		mult, lower = 1, strings.TrimSuffix(lower, "b")
	}
	lower = strings.TrimSpace(lower)

	n, err := strconv.ParseInt(lower, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid value %q", s)
	}
	if n <= 0 {
		return 0, fmt.Errorf("throughput must be positive, got %q", s)
	}
	bps := n * mult
	if bps <= 0 { // overflow guard
		return 0, fmt.Errorf("throughput too large: %q", s)
	}
	return bps, nil
}

func parseRatio(s string) (setW, getW int, err error) {
	a, b, ok := strings.Cut(s, ":")
	if !ok {
		return 0, 0, fmt.Errorf("invalid ratio %q (want SET:GET, e.g. 1:10)", s)
	}
	setW, err = strconv.Atoi(strings.TrimSpace(a))
	if err != nil || setW < 0 {
		return 0, 0, fmt.Errorf("invalid ratio SET part in %q", s)
	}
	getW, err = strconv.Atoi(strings.TrimSpace(b))
	if err != nil || getW < 0 {
		return 0, 0, fmt.Errorf("invalid ratio GET part in %q", s)
	}
	if setW == 0 && getW == 0 {
		return 0, 0, fmt.Errorf("invalid ratio %q (both zero)", s)
	}
	return setW, getW, nil
}

func parseIntRange(s string) (min, max int, err error) {
	a, b, ok := strings.Cut(s, "-")
	if !ok {
		return 0, 0, fmt.Errorf("expected min-max, got %q", s)
	}
	min, err = strconv.Atoi(strings.TrimSpace(a))
	if err != nil {
		return 0, 0, fmt.Errorf("invalid min in %q", s)
	}
	max, err = strconv.Atoi(strings.TrimSpace(b))
	if err != nil {
		return 0, 0, fmt.Errorf("invalid max in %q", s)
	}
	if min < 0 || max < min {
		return 0, 0, fmt.Errorf("invalid range %q", s)
	}
	return min, max, nil
}

func run() {
	// Resolve TTL: a random range takes precedence over the fixed value.
	var (
		ttlFixed       time.Duration
		ttlMin, ttlMax time.Duration
		hasTTLRange    bool
	)
	if expiryRange != "" {
		lo, hi, _ := parseIntRange(expiryRange)
		ttlMin = time.Duration(lo) * time.Second
		ttlMax = time.Duration(hi) * time.Second
		hasTTLRange = true
	} else if expire != "" {
		parsed, err := time.ParseDuration(expire)
		if err != nil || parsed < 0 {
			_, _ = fmt.Fprintf(os.Stderr, "invalid expire: %s\n", expire)
			os.Exit(1)
		}
		ttlFixed = parsed
	}

	pattern := strings.ToUpper(keyPattern)

	ds, dsMin, dsMax := dataSize, 0, 0
	if dataSizeRange != "" {
		dsMin, dsMax, _ = parseIntRange(dataSizeRange)
		ds = 0
	}

	var bytesPerSec int64
	effectiveOps := ops
	if throughput != "" {
		bytesPerSec, _ = parseThroughput(throughput)
		// --throughput is mutually exclusive with --ops (validated), but --ops
		// has a non-zero default; force it off so the byte-rate throttle (not a
		// stale default qps) governs the run.
		effectiveOps = 0
	}

	// throttleDesc summarizes the active rate-limit dimension for the config dump.
	throttleDesc := "full speed (unlimited)"
	if bytesPerSec > 0 {
		throttleDesc = fmt.Sprintf("throughput %s (%d B/s)", throughput, bytesPerSec)
	} else if effectiveOps > 0 {
		throttleDesc = fmt.Sprintf("%d ops/s", effectiveOps)
	}

	// valueSizeDesc describes how string value size is chosen.
	valueSizeDesc := fmt.Sprintf("%d B (fixed)", ds)
	if dataSizeRange != "" {
		valueSizeDesc = fmt.Sprintf("[%d, %d] B (random)", dsMin, dsMax)
	}
	if randomData {
		valueSizeDesc += ", random bytes"
	}

	expireDesc := ""
	if expiryRange != "" {
		expireDesc = "random " + expiryRange + "s"
	} else if expire != "" {
		expireDesc = expire
	}

	cfg := &RunConfig{
		Target:     strings.Join(addresses, ","),
		Clients:    clientsNum,
		Duration:   duration,
		Pipeline:   pipeline,
		KeyPattern: pattern,
		KeyMin:     keyMin,
		KeyMax:     keyMax,
		ValueSize:  valueSizeDesc,
		Throttle:   throttleDesc,
		Load:       loadFlag,
		Expire:     expireDesc,
	}
	if len(addresses) == 0 {
		cfg.Target = "(default 127.0.0.1:6379)"
	}

	// Command mode: arbitrary commands override the type-based workload and
	// --ratio.
	if len(commands) > 0 {
		specs := make([]*commandSpec, len(commands))
		for i, tmpl := range commands {
			ratio := 1
			if i < len(commandRatios) {
				ratio = commandRatios[i]
			}
			pat := pattern
			if i < len(commandKeyPatterns) {
				pat = strings.ToUpper(commandKeyPatterns[i])
			}
			kc := newKeyChooser(pat, keyMin, keyMax, keyZipfExp)
			specs[i] = newCommandSpec(tmpl, ratio, kc)
		}

		sender := NewSender(clientsNum, addresses, password, pipeline, effectiveOps, bytesPerSec, nil)
		sender.SetCommands(specs, valueSizer{fixed: ds, min: dsMin, max: dsMax}, keyPrefix, zeroPadding, randomData)
		sender.SetOutputFiles(jsonOut, histOut)
		cfg.Mode = "command"
		cfg.Commands = commands
		// In command mode value size feeds __data__ substitution.
		sender.SetConfig(cfg)
		if loadFlag {
			_, _ = fmt.Fprintln(os.Stderr, "note: --load is ignored in command mode")
		}
		ctx, cancel := newRunContext()
		defer cancel()
		sender.Run(ctx)
		return
	}

	setW, getW, _ := parseRatio(ratio)

	workloads := make([]*Workload, 0, len(dataTypes))
	for _, t := range dataTypes {
		if len(t) == 0 {
			continue
		}
		workloads = append(workloads, NewWorkload(WorkloadConfig{
			Type:        Type(t),
			KeyPrefix:   keyPrefix,
			KeyMin:      keyMin,
			KeyMax:      keyMax,
			KeyPattern:  pattern,
			ZipfExp:     keyZipfExp,
			ZeroPadding: zeroPadding,
			SetWeight:   setW,
			GetWeight:   getW,
			FieldNum:    fieldsNum,
			DataSize:    ds,
			DataSizeMin: dsMin,
			DataSizeMax: dsMax,
			RandomData:  randomData,
			TTLFixed:    ttlFixed,
			TTLMin:      ttlMin,
			TTLMax:      ttlMax,
			HasTTLRange: hasTTLRange,
		}))
	}
	if len(workloads) == 0 {
		_, _ = fmt.Fprintln(os.Stderr, "no valid data types")
		os.Exit(1)
	}

	sender := NewSender(clientsNum, addresses, password, pipeline, effectiveOps, bytesPerSec, workloads)
	sender.SetOutputFiles(jsonOut, histOut)
	cfg.Mode = "workload"
	cfg.DataTypes = strings.Join(dataTypes, ",")
	cfg.Ratio = ratio
	sender.SetConfig(cfg)

	// Load phase runs to completion (not bounded by --duration).
	if loadFlag {
		sender.Load(context.Background())
	}

	ctx, cancel := newRunContext()
	defer cancel()
	sender.Run(ctx)
}

// newRunContext returns the run-phase context bounded by --duration (or an
// unbounded context if --duration is empty).
func newRunContext() (context.Context, context.CancelFunc) {
	if len(duration) == 0 {
		return context.Background(), func() {}
	}
	runDuration, err := time.ParseDuration(duration)
	if err != nil || runDuration < 1*time.Second {
		_, _ = fmt.Fprintf(os.Stderr, "invalid duration: %s\n", duration)
		os.Exit(1)
	}
	return context.WithTimeout(context.Background(), runDuration)
}
func main() {
	rootCmd.Root().CompletionOptions.DisableDefaultCmd = true
	rootCmd.Flags().BoolVarP(&versionFlag, "version", "v", false, "print the version info")
	rootCmd.Flags().StringSliceVarP(&addresses, "address", "a", nil,
		"redis/proxy address; repeat or comma-separate to spread client connections across a proxy cluster")
	rootCmd.Flags().StringVarP(&password, "password", "p", "", "the password of redis server")
	rootCmd.Flags().IntVarP(&clientsNum, "client", "c", 50, "the number of clients (each uses one connection)")
	rootCmd.Flags().StringVarP(&duration, "duration", "d", "24h", "the duration of running(unit: s, m, h), must >= 1s")
	rootCmd.Flags().StringSliceVarP(&dataTypes, "types", "t", []string{"string"},
		"data type(use commas to separate multiple), support string, list, set, hash, zset")
	rootCmd.Flags().IntVar(&pipeline, "pipeline", 16, "the pipeline of redis client")
	rootCmd.Flags().IntVar(&fieldsNum, "fields", 8, "the fields number of hash, zset, set, list data")
	rootCmd.Flags().IntVar(&ops, "ops", 10000, "the sending speed(command per second), 0 means unlimited (full speed)")
	rootCmd.Flags().StringVar(&keyPrefix, "key-prefix", "rbg-", "prefix for keys")
	rootCmd.Flags().StringVar(&expire, "expire", "", "fixed key TTL (e.g. 30s, 5m, 1h), empty means no expiration")

	rootCmd.Flags().StringVar(&ratio, "ratio", "1:0", "SET:GET ratio (e.g. 1:10). GET only supported for string type")
	rootCmd.Flags().StringVar(&keyPattern, "key-pattern", "R", "key distribution: R=uniform, S=sequential, Z=zipfian")
	rootCmd.Flags().Int64Var(&keyMin, "key-minimum", 0, "key id minimum value")
	rootCmd.Flags().Int64Var(&keyMax, "key-maximum", 10000000, "key id maximum value")
	rootCmd.Flags().Float64Var(&keyZipfExp, "key-zipf-exp", ZipfianConstant, "zipfian exponent in (0,5); 0.99 uses the fast scrambled path")
	rootCmd.Flags().IntVar(&zeroPadding, "zero-padding", 0, "zero-pad the numeric key id to this width")
	rootCmd.Flags().IntVar(&dataSize, "data-size", 32, "value size in bytes for string writes")
	rootCmd.Flags().StringVar(&dataSizeRange, "data-size-range", "", "random value size range min-max (overrides data-size)")
	rootCmd.Flags().BoolVar(&randomData, "random-data", false, "fill string values with random bytes instead of a constant")
	rootCmd.Flags().StringVar(&expiryRange, "expiry-range", "", "random TTL in seconds, range min-max (overrides expire)")
	rootCmd.Flags().BoolVar(&loadFlag, "load", false, "pre-populate the whole key space (sequential writes) before the run")
	rootCmd.Flags().StringVarP(&workloadFile, "workload", "P", "", "load internal parameters from a preset file (CLI flags override)")

	rootCmd.Flags().StringArrayVar(&commands, "command", nil,
		"arbitrary command with __key__/__data__ placeholders (repeatable); enables command mode, ignoring --ratio/-t")
	rootCmd.Flags().IntSliceVar(&commandRatios, "command-ratio", nil, "ratio for the i-th --command (default 1)")
	rootCmd.Flags().StringArrayVar(&commandKeyPatterns, "command-key-pattern", nil, "key pattern (R/S/Z) for the i-th --command (default = --key-pattern)")

	rootCmd.Flags().StringVar(&jsonOut, "json-out", "", "write a JSON summary to FILE (default: text summary to stdout)")
	rootCmd.Flags().StringVar(&histOut, "hist-out", "", "write a standard HdrHistogram interval log (.hlog) to FILE, one tagged histogram per op (mergeable by any HdrHistogram tooling)")
	rootCmd.Flags().StringVar(&throughput, "throughput", "", "limit the written value byte rate, e.g. 1MB/s / 500KB (units B/KB/MB/GB, 1KB=1024, /s optional); mutually exclusive with --ops, string pure-write only")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
