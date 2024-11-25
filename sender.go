package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"
)

type Sender struct {
	dataCh  chan *Data
	counter atomic.Int64
	ops     int

	clients  []*client
	pipeline int
}

type client struct {
	rdb *redis.Client
}

func NewSender(clientNum int, addr, password string, pipeline, ops int, dataCh chan *Data) *Sender {
	clients := make([]*client, 0, clientNum)
	for i := 0; i < clientNum; i++ {
		c := &client{
			rdb: redis.NewClient(&redis.Options{
				Addr:     addr,
				Password: password,
			}),
		}
		clients = append(clients, c)
	}

	return &Sender{
		dataCh:   dataCh,
		ops:      ops,
		clients:  clients,
		pipeline: pipeline,
	}
}

func (s *Sender) Run(ctx context.Context) {
	pendingSendCh := make(chan []*Data)
	for _, c := range s.clients {
		go func(c *client) {
			c.run(ctx, pendingSendCh)
		}(c)
	}

	defer func() {
		close(pendingSendCh)
		for _, c := range s.clients {
			c.close()
		}
	}()

	limiter := rate.NewLimiter(rate.Limit(s.ops), ops)

	batchData := make([]*Data, 0, s.pipeline)

	go s.report(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-s.dataCh:
			if !ok {
				return
			}

			// Rate limit
			if !limiter.Allow() && len(batchData) > 0 {
				// Send last pending data
				pendingSendCh <- batchData
				batchData = make([]*Data, 0, s.pipeline)
			}

			if err := limiter.Wait(ctx); err != nil {
				continue
			}

			s.counter.Add(1)
			batchData = append(batchData, data)

			if len(batchData) >= s.pipeline {
				pendingSendCh <- batchData
				batchData = make([]*Data, 0, s.pipeline)
			}
		}
	}
}

func (s *Sender) report(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	w := tabwriter.NewWriter(
		os.Stdout,
		0,
		0,
		2,
		' ',
		0,
	)

	lastReportTime := time.Now()
	lastCounter := s.counter.Load()

	printStatus := func() {
		ops := float64(s.counter.Load()-lastCounter) / (float64(time.Since(lastReportTime).Milliseconds()) / 1000.0)
		_, _ = fmt.Fprintf(w, "%s\tcounter: %d\tops: %d\n",
			time.Now().Format("2006-01-02 15:04:05"), s.counter.Load(), int(math.Round(ops)))
		_ = w.Flush()
	}

	for {
		select {
		case <-ctx.Done():
			printStatus()
			return
		case <-ticker.C:
			printStatus()
			lastReportTime = time.Now()
			lastCounter = s.counter.Load()
		}
	}
}

func (c *client) close() {
	if c.rdb != nil {
		_ = c.rdb.Close()
	}
}

func (c *client) run(ctx context.Context, dataCh chan []*Data) {
	for data := range dataCh {
		pipe := c.rdb.Pipeline()
		addToPipeline(ctx, pipe, data)
		_, err := pipe.Exec(ctx)
		if err != nil {
			log.Printf("send data failed: %s\n", err.Error())
		}
	}
}

func addToPipeline(ctx context.Context, pipe redis.Pipeliner, data []*Data) {
	for _, d := range data {
		switch d.t {
		case String:
			pipe.Set(ctx, d.Key, d.Value, 0)
		case Hash:
			kvs := make([]interface{}, 0, len(d.Value.(map[string]string))*2)
			for k, v := range d.Value.(map[string]string) {
				kvs = append(kvs, k, v)
			}
			pipe.HSet(ctx, d.Key, kvs...)
		case List, Set:
			args := make([]interface{}, len(d.Value.([]string)))
			for i, v := range d.Value.([]string) {
				args[i] = v
			}
			if d.t == List {
				pipe.LPush(ctx, d.Key, args...)
			} else {
				pipe.SAdd(ctx, d.Key, args...)
			}
		case ZSet:
			m := d.Value.(map[string]float64)
			members := make([]redis.Z, 0, len(m))
			for member, score := range m {
				members = append(members, redis.Z{Score: score, Member: member})
			}
			pipe.ZAdd(ctx, d.Key, members...)
		}
	}
}
