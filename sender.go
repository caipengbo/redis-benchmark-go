package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/redis/go-redis/v9"
)

type Sender struct {
	counter    atomic.Int64
	readCount  atomic.Int64
	writeCount atomic.Int64
	missCount  atomic.Int64

	writeHist *latencyHist
	readHist  *latencyHist

	workloads  []*Workload
	clients    []*client
	pipeline   int
	maxValSize int
	randomData bool

	// perOpTickNs is the target ns-per-command for a single worker
	// (clientNum*1e9/ops). 0 means full speed (no throttling).
	perOpTickNs int64

	// command mode (arbitrary commands). When non-empty, workers run these
	// instead of the type-based workloads.
	commands   []*commandSpec
	cmdTotal   int
	cmdSizer   valueSizer
	cmdPrefix  string
	cmdZeroPad int
}

// SetCommands switches the sender into arbitrary-command mode.
func (s *Sender) SetCommands(specs []*commandSpec, sizer valueSizer, prefix string, zeroPad int, randomData bool) {
	s.commands = specs
	s.cmdSizer = sizer
	s.cmdPrefix = prefix
	s.cmdZeroPad = zeroPad
	s.randomData = randomData
	total := 0
	for _, cs := range specs {
		total += cs.ratio
	}
	s.cmdTotal = total
}

func (s *Sender) cmdMode() bool { return len(s.commands) > 0 }

func (s *Sender) pickCommand(st *workerState) *commandSpec {
	if len(s.commands) == 1 {
		return s.commands[0]
	}
	x := st.r.Intn(s.cmdTotal)
	acc := 0
	for _, cs := range s.commands {
		acc += cs.ratio
		if x < acc {
			return cs
		}
	}
	return s.commands[len(s.commands)-1]
}

type client struct {
	rdb *redis.Client
}

func NewSender(clientNum int, addrs []string, password string, pipeline, ops int, workloads []*Workload) *Sender {
	if len(addrs) == 0 {
		addrs = []string{""}
	}

	clients := make([]*client, 0, clientNum)
	perAddr := make([]int, len(addrs))
	for i := 0; i < clientNum; i++ {
		addr := addrs[i%len(addrs)]
		perAddr[i%len(addrs)]++
		clients = append(clients, &client{
			rdb: redis.NewClient(&redis.Options{
				Addr:     addr,
				Password: password,
				PoolSize: 1,
			}),
		})
	}

	if len(addrs) > 1 {
		for i, a := range addrs {
			_, _ = fmt.Fprintf(os.Stderr, "address %s: %d clients\n", a, perAddr[i])
		}
	}

	var perOpTickNs int64
	if ops > 0 && clientNum > 0 {
		perWorkerOps := float64(ops) / float64(clientNum)
		if perWorkerOps > 0 {
			perOpTickNs = int64(1e9 / perWorkerOps)
		}
	}

	maxVal := 1
	randomData := false
	for _, wl := range workloads {
		if v := wl.maxValueSize(); v > maxVal {
			maxVal = v
		}
		if wl.randomData {
			randomData = true
		}
	}

	return &Sender{
		workloads:   workloads,
		clients:     clients,
		pipeline:    pipeline,
		maxValSize:  maxVal,
		randomData:  randomData,
		perOpTickNs: perOpTickNs,
		writeHist:   newLatencyHist(),
		readHist:    newLatencyHist(),
	}
}

// Load pre-populates the whole key space for each workload once (sequential
// writes), so a subsequent read/write Run can hit existing keys. It runs at
// full speed using all clients in parallel, one workload at a time.
func (s *Sender) Load(ctx context.Context) {
	for _, wl := range s.workloads {
		total := wl.keyMax - wl.keyMin + 1
		if total <= 0 {
			continue
		}
		var next atomic.Int64

		var wg sync.WaitGroup
		for i, c := range s.clients {
			wg.Add(1)
			go func(id int, c *client, wl *Workload) {
				defer wg.Done()
				st := newWorkerState(int64(id)+1, s.maxValSize, s.randomData)
				batch := make([]*Operation, s.pipeline)
				for j := range batch {
					batch[j] = &Operation{}
				}
				for {
					if ctx.Err() != nil {
						return
					}
					start := next.Add(int64(s.pipeline)) - int64(s.pipeline)
					if start >= total {
						return
					}
					n := s.pipeline
					if start+int64(n) > total {
						n = int(total - start)
					}
					pipe := c.rdb.Pipeline()
					for j := 0; j < n; j++ {
						wl.fillWriteAt(st, batch[j], wl.keyMin+start+int64(j))
					}
					addToPipeline(ctx, pipe, wl.t, batch[:n])
					if _, err := pipe.Exec(ctx); err != nil {
						if ctx.Err() != nil {
							return
						}
						log.Printf("load exec failed: %s\n", err.Error())
						continue
					}
					s.counter.Add(int64(n))
				}
			}(i, c, wl)
		}
		wg.Wait()
	}
	_, _ = fmt.Fprintf(os.Stderr, "load finished: %d keys written\n", s.counter.Load())
}

func (s *Sender) Run(ctx context.Context) {
	go s.report(ctx)

	var wg sync.WaitGroup
	for i, c := range s.clients {
		wg.Add(1)
		go func(id int, c *client) {
			defer wg.Done()
			s.worker(ctx, id, c)
		}(i, c)
	}
	wg.Wait()

	for _, c := range s.clients {
		c.close()
	}
}

func (s *Sender) worker(ctx context.Context, id int, c *client) {
	if s.cmdMode() {
		s.workerCommand(ctx, id, c)
		return
	}

	st := newWorkerState(int64(id)+1, s.maxValSize, s.randomData)

	batch := make([]*Operation, s.pipeline)
	for i := range batch {
		batch[i] = &Operation{}
	}
	getCmds := make([]*redis.StringCmd, s.pipeline)
	round := id

	if s.perOpTickNs > 0 {
		jitter := time.Duration(st.r.Int63n(s.perOpTickNs*int64(s.pipeline) + 1))
		select {
		case <-ctx.Done():
			return
		case <-time.After(jitter):
		}
	}

	startTime := time.Now()
	var opsDone int64

	throttleTimer := time.NewTimer(time.Hour)
	throttleTimer.Stop()

	for {
		if ctx.Err() != nil {
			return
		}

		wl := s.workloads[round%len(s.workloads)]
		round++
		isRead := wl.pickRead(st)

		pipe := c.rdb.Pipeline()
		if isRead {
			for j := 0; j < s.pipeline; j++ {
				wl.fillKey(st, batch[j])
				getCmds[j] = pipe.Get(ctx, batch[j].key)
			}
		} else {
			for j := 0; j < s.pipeline; j++ {
				wl.fillWrite(st, batch[j])
			}
			addToPipeline(ctx, pipe, wl.t, batch)
		}

		execStart := time.Now()
		_, err := pipe.Exec(ctx)
		lat := time.Since(execStart)

		if err != nil && err != redis.Nil {
			// redis.Nil is expected for read misses; only real errors here.
			if ctx.Err() != nil {
				return
			}
			log.Printf("exec failed: %s\n", err.Error())
			continue
		}

		s.counter.Add(int64(s.pipeline))
		opsDone += int64(s.pipeline)

		if isRead {
			s.readHist.record(lat)
			s.readCount.Add(int64(s.pipeline))
			miss := int64(0)
			for j := 0; j < s.pipeline; j++ {
				if getCmds[j].Err() == redis.Nil {
					miss++
				}
			}
			if miss > 0 {
				s.missCount.Add(miss)
			}
		} else {
			s.writeHist.record(lat)
			s.writeCount.Add(int64(s.pipeline))
		}

		if s.perOpTickNs > 0 {
			deadline := startTime.Add(time.Duration(opsDone * s.perOpTickNs))
			if d := time.Until(deadline); d > 0 {
				if !throttleWait(ctx, throttleTimer, d) {
					return
				}
			}
		}
	}
}

// workerCommand runs arbitrary-command mode: each batch is a single command
// (chosen by weighted ratio), filled pipeline times with per-op key/value
// substitution, timed, and recorded into that command's own histogram.
func (s *Sender) workerCommand(ctx context.Context, id int, c *client) {
	st := newWorkerState(int64(id)+1, s.cmdSizer.maxSize(), s.randomData)

	if s.perOpTickNs > 0 {
		jitter := time.Duration(st.r.Int63n(s.perOpTickNs*int64(s.pipeline) + 1))
		select {
		case <-ctx.Done():
			return
		case <-time.After(jitter):
		}
	}

	startTime := time.Now()
	var opsDone int64

	throttleTimer := time.NewTimer(time.Hour)
	throttleTimer.Stop()

	for {
		if ctx.Err() != nil {
			return
		}

		spec := s.pickCommand(st)
		pipe := c.rdb.Pipeline()
		for j := 0; j < s.pipeline; j++ {
			val := st.valBuf[:s.cmdSizer.size(st)]
			args := spec.buildArgs(st, s.cmdPrefix, s.cmdZeroPad, val)
			pipe.Do(ctx, args...)
		}

		execStart := time.Now()
		_, err := pipe.Exec(ctx)
		lat := time.Since(execStart)

		if err != nil && err != redis.Nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("command exec failed: %s\n", err.Error())
			continue
		}

		s.counter.Add(int64(s.pipeline))
		spec.count.Add(int64(s.pipeline))
		spec.hist.record(lat)
		opsDone += int64(s.pipeline)

		if s.perOpTickNs > 0 {
			deadline := startTime.Add(time.Duration(opsDone * s.perOpTickNs))
			if d := time.Until(deadline); d > 0 {
				if !throttleWait(ctx, throttleTimer, d) {
					return
				}
			}
		}
	}
}

func (s *Sender) report(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	const window = 5
	rates := make([]float64, 0, window)

	lastReportTime := time.Now()
	lastCounter := s.counter.Load()

	printStatus := func(final bool) {
		now := time.Now()
		cur := s.counter.Load()
		elapsed := now.Sub(lastReportTime).Seconds()

		instant := 0.0
		if elapsed > 0 {
			instant = float64(cur-lastCounter) / elapsed
		}
		if !final {
			if len(rates) == window {
				rates = rates[1:]
			}
			rates = append(rates, instant)
		}
		avg := instant
		if len(rates) > 0 {
			var sum float64
			for _, r := range rates {
				sum += r
			}
			avg = sum / float64(len(rates))
		}
		shownOps := instant
		if final && elapsed < 0.5 {
			shownOps = avg
		}

		if s.cmdMode() {
			var sb strings.Builder
			for _, cs := range s.commands {
				fmt.Fprintf(&sb, "\t%s_p99: %s", cs.name, cs.hist.percentile(0.99))
			}
			_, _ = fmt.Fprintf(w, "%s\tcounter: %d\tops: %d\tavg(%ds): %d%s\n",
				now.Format("2006-01-02 15:04:05"), cur, int(math.Round(shownOps)), window, int(math.Round(avg)), sb.String())
		} else {
			_, _ = fmt.Fprintf(w, "%s\tcounter: %d\tops: %d\tavg(%ds): %d\twrite_p99: %s\tread_p99: %s\n",
				now.Format("2006-01-02 15:04:05"), cur, int(math.Round(shownOps)), window, int(math.Round(avg)),
				s.writeHist.percentile(0.99), s.readHist.percentile(0.99))
		}
		_ = w.Flush()
	}

	for {
		select {
		case <-ctx.Done():
			printStatus(true)
			s.printSummary(w)
			return
		case <-ticker.C:
			printStatus(false)
			lastReportTime = time.Now()
			lastCounter = s.counter.Load()
		}
	}
}

func (s *Sender) printSummary(w *tabwriter.Writer) {
	if s.cmdMode() {
		for _, cs := range s.commands {
			cmin, cmean, cmax, _ := cs.hist.stats()
			_, _ = fmt.Fprintf(w,
				"SUMMARY-%s\tcount: %d\tmin: %s\tmean: %s\tmax: %s\tp50: %s\tp99: %s\tp999: %s\n",
				cs.name, cs.count.Load(), cmin, cmean, cmax,
				cs.hist.percentile(0.50), cs.hist.percentile(0.99), cs.hist.percentile(0.999))
		}
		_ = w.Flush()
		return
	}

	reads := s.readCount.Load()
	writes := s.writeCount.Load()
	miss := s.missCount.Load()

	missRate := 0.0
	if reads > 0 {
		missRate = float64(miss) / float64(reads) * 100
	}

	wmin, wmean, wmax, _ := s.writeHist.stats()
	_, _ = fmt.Fprintf(w,
		"SUMMARY-WRITE\tcount: %d\tmin: %s\tmean: %s\tmax: %s\tp50: %s\tp99: %s\tp999: %s\n",
		writes, wmin, wmean, wmax,
		s.writeHist.percentile(0.50), s.writeHist.percentile(0.99), s.writeHist.percentile(0.999))

	if reads > 0 {
		rmin, rmean, rmax, _ := s.readHist.stats()
		_, _ = fmt.Fprintf(w,
			"SUMMARY-READ\tcount: %d\tmiss: %d (%.2f%%)\tmin: %s\tmean: %s\tmax: %s\tp50: %s\tp99: %s\tp999: %s\n",
			reads, miss, missRate, rmin, rmean, rmax,
			s.readHist.percentile(0.50), s.readHist.percentile(0.99), s.readHist.percentile(0.999))
	}
	_ = w.Flush()
}

func (c *client) close() {
	if c.rdb != nil {
		_ = c.rdb.Close()
	}
}

// throttleWait sleeps until d elapses using a reusable timer (avoiding a
// per-call time.After allocation), returning false if ctx is cancelled first.
func throttleWait(ctx context.Context, t *time.Timer, d time.Duration) bool {
	t.Reset(d)
	select {
	case <-ctx.Done():
		t.Stop()
		return false
	case <-t.C:
		return true
	}
}

func addToPipeline(ctx context.Context, pipe redis.Pipeliner, t Type, ops []*Operation) {
	for _, op := range ops {
		switch t {
		case String:
			pipe.Set(ctx, op.key, op.strVal, op.ttl)
		case Hash:
			// typedVal is a pre-built, read-only []interface{} of alternating
			// field/value pairs, shared across every write (go-redis only reads
			// the args), so there is no per-op allocation here.
			pipe.HSet(ctx, op.key, op.typedVal.([]interface{})...)
		case List:
			pipe.LPush(ctx, op.key, op.typedVal.([]interface{})...)
		case Set:
			pipe.SAdd(ctx, op.key, op.typedVal.([]interface{})...)
		case ZSet:
			pipe.ZAdd(ctx, op.key, op.typedVal.([]redis.Z)...)
		}

		if op.ttl > 0 && t != String {
			pipe.Expire(ctx, op.key, op.ttl)
		}
	}
}

// typedPipelineArgs converts a type's constant payload into the exact argument
// form its pipeline command expects. It is computed once per workload and
// shared read-only across every write, keeping the non-string write path
// allocation-free.
func typedPipelineArgs(t Type, v any) any {
	switch t {
	case Hash:
		m := v.(map[string]string)
		kvs := make([]interface{}, 0, len(m)*2)
		for k, val := range m {
			kvs = append(kvs, k, val)
		}
		return kvs
	case List, Set:
		vals := v.([]string)
		args := make([]interface{}, len(vals))
		for i, val := range vals {
			args[i] = val
		}
		return args
	case ZSet:
		m := v.(map[string]float64)
		members := make([]redis.Z, 0, len(m))
		for member, score := range m {
			members = append(members, redis.Z{Score: score, Member: member})
		}
		return members
	default:
		return nil
	}
}
