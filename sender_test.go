package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func testAddr() string {
	if a := os.Getenv("REDIS_ADDR"); a != "" {
		return a
	}
	return "127.0.0.1:6379"
}

func dialTest(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: testAddr()})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("redis not reachable at %s: %v (set REDIS_ADDR to run e2e tests)", testAddr(), err)
	}
	return rdb
}

func baseCfg(typ Type, prefix string) WorkloadConfig {
	return WorkloadConfig{
		Type:       typ,
		KeyPrefix:  prefix,
		KeyMin:     0,
		KeyMax:     999,
		KeyPattern: "R",
		ZipfExp:    ZipfianConstant,
		SetWeight:  1,
		GetWeight:  0,
		FieldNum:   8,
		DataSize:   32,
	}
}

func newTestSender(cfgs []WorkloadConfig, clients, pipeline, ops int) *Sender {
	wls := make([]*Workload, len(cfgs))
	for i, c := range cfgs {
		wls[i] = NewWorkload(c)
	}
	return NewSender(clients, []string{testAddr()}, "", pipeline, ops, 0, wls)
}

func runFor(s *Sender, dur time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()
	s.Run(ctx)
}

func scanKeys(t *testing.T, rdb *redis.Client, match string) []string {
	t.Helper()
	ctx := context.Background()
	var keys []string
	var cursor uint64
	for {
		ks, cur, err := rdb.Scan(ctx, cursor, match, 500).Result()
		if err != nil {
			t.Fatalf("SCAN %s: %v", match, err)
		}
		keys = append(keys, ks...)
		cursor = cur
		if cursor == 0 {
			break
		}
	}
	return keys
}

func cleanup(t *testing.T, rdb *redis.Client, prefix string) {
	t.Helper()
	keys := scanKeys(t, rdb, prefix+"*")
	if len(keys) > 0 {
		if err := rdb.Del(context.Background(), keys...).Err(); err != nil {
			t.Logf("cleanup del: %v", err)
		}
	}
}

func uniquePrefix(tag string) string {
	return fmt.Sprintf("t_%s_%d:", tag, time.Now().UnixNano())
}

func TestSenderWritesStringValue(t *testing.T) {
	rdb := dialTest(t)
	defer rdb.Close()

	prefix := uniquePrefix("str")
	defer cleanup(t, rdb, prefix)

	cfg := baseCfg(String, prefix)
	cfg.DataSize = 32
	s := newTestSender([]WorkloadConfig{cfg}, 4, 8, 5000)
	runFor(s, 1200*time.Millisecond)

	if s.counter.Load() == 0 {
		t.Fatal("nothing written")
	}
	keys := scanKeys(t, rdb, prefix+"*")
	if len(keys) == 0 {
		t.Fatal("no string keys written")
	}
	// keys must be within the configured key space and prefix.
	for _, k := range keys {
		num := strings.TrimPrefix(k, prefix)
		n, err := strconv.ParseInt(num, 10, 64)
		if err != nil {
			t.Fatalf("key %q has non-numeric suffix", k)
		}
		if n < 0 || n > 999 {
			t.Errorf("key %q id %d out of [0,999]", k, n)
		}
	}
	got, err := rdb.Get(context.Background(), keys[0]).Result()
	if err != nil {
		t.Fatalf("GET %s: %v", keys[0], err)
	}
	if len(got) != 32 {
		t.Errorf("value size = %d, want 32", len(got))
	}
	if got != strings.Repeat("x", 32) {
		t.Errorf("value content unexpected: %q", got)
	}
}

func TestSenderWritesTypes(t *testing.T) {
	rdb := dialTest(t)
	defer rdb.Close()

	cases := []struct {
		typ       Type
		wantRedis string
	}{
		{List, "list"},
		{Set, "set"},
		{Hash, "hash"},
		{ZSet, "zset"},
	}
	for _, tc := range cases {
		t.Run(string(tc.typ), func(t *testing.T) {
			prefix := uniquePrefix(string(tc.typ))
			defer cleanup(t, rdb, prefix)

			s := newTestSender([]WorkloadConfig{baseCfg(tc.typ, prefix)}, 4, 8, 4000)
			runFor(s, 1*time.Second)

			keys := scanKeys(t, rdb, prefix+"*")
			if len(keys) == 0 {
				t.Fatalf("no %s keys written", tc.typ)
			}
			rt, err := rdb.Type(context.Background(), keys[0]).Result()
			if err != nil {
				t.Fatalf("TYPE %s: %v", keys[0], err)
			}
			if rt != tc.wantRedis {
				t.Errorf("key %s redis type = %q, want %q", keys[0], rt, tc.wantRedis)
			}
		})
	}
}

func TestSenderDataSizeRange(t *testing.T) {
	rdb := dialTest(t)
	defer rdb.Close()

	prefix := uniquePrefix("dsr")
	defer cleanup(t, rdb, prefix)

	cfg := baseCfg(String, prefix)
	cfg.DataSize = 0
	cfg.DataSizeMin = 10
	cfg.DataSizeMax = 100
	s := newTestSender([]WorkloadConfig{cfg}, 4, 8, 5000)
	runFor(s, 1200*time.Millisecond)

	keys := scanKeys(t, rdb, prefix+"*")
	if len(keys) == 0 {
		t.Fatal("no keys written")
	}
	ctx := context.Background()
	for _, k := range keys {
		n, err := rdb.StrLen(ctx, k).Result()
		if err != nil {
			t.Fatalf("STRLEN %s: %v", k, err)
		}
		if n < 10 || n > 100 {
			t.Errorf("value size %d out of [10,100]", n)
		}
	}
}

func TestSenderTTL(t *testing.T) {
	rdb := dialTest(t)
	defer rdb.Close()

	prefix := uniquePrefix("ttl")
	defer cleanup(t, rdb, prefix)

	ttl := 100 * time.Second
	strCfg := baseCfg(String, prefix)
	strCfg.TTLFixed = ttl
	hashCfg := baseCfg(Hash, prefix+"h:")
	hashCfg.TTLFixed = ttl

	s := newTestSender([]WorkloadConfig{strCfg, hashCfg}, 4, 8, 3000)
	runFor(s, 1200*time.Millisecond)
	defer cleanup(t, rdb, prefix+"h:")

	keys := scanKeys(t, rdb, prefix+"*")
	if len(keys) == 0 {
		t.Fatal("no keys written")
	}
	ctx := context.Background()
	for _, k := range keys {
		d, err := rdb.TTL(ctx, k).Result()
		if err != nil {
			t.Fatalf("TTL %s: %v", k, err)
		}
		if d <= 0 || d > ttl {
			t.Errorf("key %s ttl = %v, want in (0, %v]", k, d, ttl)
		}
	}
}

func TestSenderReadWriteWithLoad(t *testing.T) {
	rdb := dialTest(t)
	defer rdb.Close()

	prefix := uniquePrefix("rw")
	defer cleanup(t, rdb, prefix)

	cfg := baseCfg(String, prefix)
	cfg.KeyMax = 499 // small keyspace, fully loaded so reads hit
	cfg.SetWeight = 1
	cfg.GetWeight = 1
	s := newTestSender([]WorkloadConfig{cfg}, 4, 8, 20000)

	// Load pre-populates the whole keyspace so GETs hit.
	s.Load(context.Background())
	runFor(s, 1500*time.Millisecond)

	if s.readCount.Load() == 0 {
		t.Fatal("no reads issued despite ratio 1:1")
	}
	// Every key was loaded with no TTL, so reads must not miss.
	if miss := s.missCount.Load(); miss != 0 {
		t.Errorf("read misses = %d, want 0 (keyspace was fully loaded)", miss)
	}
}

func TestSenderFullSpeed(t *testing.T) {
	rdb := dialTest(t)
	defer rdb.Close()

	prefix := uniquePrefix("fs")
	defer cleanup(t, rdb, prefix)

	s := newTestSender([]WorkloadConfig{baseCfg(String, prefix)}, 8, 32, 0)
	runFor(s, 1*time.Second)

	got := s.counter.Load()
	if got == 0 {
		t.Fatal("full-speed mode wrote nothing")
	}
	if got < 1000 {
		t.Errorf("full-speed counter = %d, unexpectedly low for 1s", got)
	}
}

func TestSenderRateLimit(t *testing.T) {
	rdb := dialTest(t)
	defer rdb.Close()

	prefix := uniquePrefix("rl")
	defer cleanup(t, rdb, prefix)

	const ops = 2000
	const dur = 2 * time.Second
	s := newTestSender([]WorkloadConfig{baseCfg(String, prefix)}, 4, 10, ops)
	runFor(s, dur)

	got := s.counter.Load()
	sec := int64(dur / time.Second)

	maxExpected := int64(ops) * (sec + 3)
	if got > maxExpected {
		t.Errorf("rate-limited counter = %d exceeds max expected %d", got, maxExpected)
	}
	if got < int64(ops) {
		t.Errorf("rate-limited counter = %d too low for %v at %d ops/s", got, dur, ops)
	}
}

// TestThrottleWait covers both branches of the reusable-timer throttle without
// needing redis: the timer firing (returns true) and ctx cancellation
// (returns false). It also reuses the same timer across calls to exercise the
// Reset path.
func TestThrottleWait(t *testing.T) {
	tm := time.NewTimer(time.Hour)
	tm.Stop()

	// Timer fires before ctx is done -> true.
	if !throttleWait(context.Background(), tm, time.Millisecond) {
		t.Fatal("throttleWait returned false when the timer should have fired")
	}

	// Reusing the same timer must still work (Reset path).
	if !throttleWait(context.Background(), tm, time.Millisecond) {
		t.Fatal("throttleWait returned false on timer reuse")
	}

	// ctx already cancelled -> false, without waiting out the long duration.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if throttleWait(ctx, tm, time.Hour) {
		t.Fatal("throttleWait returned true when ctx was cancelled")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("throttleWait blocked %v on a cancelled ctx, should return promptly", elapsed)
	}

	// Timer must be reusable after the cancellation branch stopped it.
	if !throttleWait(context.Background(), tm, time.Millisecond) {
		t.Fatal("throttleWait returned false after the cancellation branch")
	}
}

// TestSenderThroughputLimit is the byte-rate twin of TestSenderRateLimit: with
// a fixed value size and a 1MB/s cap, the total written value bytes over the
// run must stay within a tolerant upper bound (and be non-trivial).
func TestSenderThroughputLimit(t *testing.T) {
	rdb := dialTest(t)
	defer rdb.Close()

	prefix := uniquePrefix("tp")
	defer cleanup(t, rdb, prefix)

	const bytesPerSec = int64(1024 * 1024) // 1MB/s
	const dur = 2 * time.Second
	const valSize = 512

	cfg := baseCfg(String, prefix)
	cfg.DataSize = valSize
	wl := NewWorkload(cfg)
	s := NewSender(4, []string{testAddr()}, "", 10, 0, bytesPerSec, []*Workload{wl})
	runFor(s, dur)

	got := s.bytesWritten.Load()
	sec := int64(dur / time.Second)

	// Upper bound: allow a couple extra seconds' worth for startup/tail slack,
	// same tolerance shape as the ops rate-limit test.
	maxExpected := bytesPerSec * (sec + 3)
	if got > maxExpected {
		t.Errorf("throughput-limited bytes = %d exceeds max expected %d", got, maxExpected)
	}
	// Lower bound: should have made real progress (at least ~1s worth).
	if got < bytesPerSec {
		t.Errorf("throughput-limited bytes = %d too low for %v at %d B/s", got, dur, bytesPerSec)
	}
}
