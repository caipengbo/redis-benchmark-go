package main

import (
	"encoding/binary"
	"hash/fnv"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// This file provides the distribution generators used by the workload layer to
// pick keys following Uniform / Sequential / Zipfian distributions. The
// generators are safe to share across workers, each passing its own *rand.Rand.

// Generator generates a sequence of int64 values following some distribution.
type Generator interface {
	Next(r *rand.Rand) int64
	Last() int64
}

// hash64 returns a positive FNV-1a hash of the big-endian int64.
func hash64(n int64) int64 {
	var b [8]byte
	binary.BigEndian.PutUint64(b[0:8], uint64(n))
	h := fnv.New64a()
	_, _ = h.Write(b[0:8])
	result := int64(h.Sum64())
	if result < 0 {
		return -result
	}
	return result
}

// number is the common base holding the last generated value. lastValue is
// accessed atomically because a single generator instance is shared across all
// workers (each passing its own *rand.Rand).
type number struct {
	lastValue int64
}

func (n *number) setLastValue(v int64) { atomic.StoreInt64(&n.lastValue, v) }
func (n *number) Last() int64          { return atomic.LoadInt64(&n.lastValue) }

// Counter generates a monotonically increasing sequence [start, start+1, ...].
type Counter struct {
	counter int64
}

func NewCounter(start int64) *Counter { return &Counter{counter: start} }

func (c *Counter) Next(_ *rand.Rand) int64 { return atomic.AddInt64(&c.counter, 1) - 1 }
func (c *Counter) Last() int64             { return atomic.LoadInt64(&c.counter) - 1 }

// Sequential cycles through [start, start+interval) repeatedly.
type Sequential struct {
	counter  int64
	interval int64
	start    int64
}

func NewSequential(countStart, countEnd int64) *Sequential {
	return &Sequential{start: countStart, interval: countEnd - countStart + 1}
}

func (s *Sequential) Next(_ *rand.Rand) int64 {
	return s.start + atomic.AddInt64(&s.counter, 1)%s.interval
}
func (s *Sequential) Last() int64 { return atomic.LoadInt64(&s.counter) + 1 }

// Uniform generates integers uniformly at random in [lb, ub].
type Uniform struct {
	number
	lb       int64
	interval int64
}

func NewUniform(lb, ub int64) *Uniform {
	return &Uniform{lb: lb, interval: ub - lb + 1}
}

func (u *Uniform) Next(r *rand.Rand) int64 {
	n := r.Int63n(u.interval) + u.lb
	u.setLastValue(n)
	return n
}

// ZipfianConstant is the default zipfian skew constant.
const ZipfianConstant = float64(0.99)

// Zipfian generates a zipfian distribution: item 0 is most popular, then 1, etc.
// Algorithm from "Quickly Generating Billion-Record Synthetic Databases",
// Jim Gray et al, SIGMOD 1994.
type Zipfian struct {
	number

	lock sync.Mutex

	items int64
	base  int64

	zipfianConstant float64

	alpha      float64
	zetan      float64
	theta      float64
	eta        float64
	zeta2Theta float64

	countForZeta int64

	allowItemCountDecrease bool
}

func NewZipfianWithRange(min, max int64, zipfianConstant float64) *Zipfian {
	return newZipfian(min, max, zipfianConstant, zetaStatic(0, max-min+1, zipfianConstant, 0))
}

func newZipfian(min, max int64, zipfianConstant, zetan float64) *Zipfian {
	items := max - min + 1
	z := new(Zipfian)
	z.items = items
	z.base = min
	z.zipfianConstant = zipfianConstant
	theta := z.zipfianConstant
	z.theta = theta
	z.zeta2Theta = z.zeta(0, 2, theta, 0)
	z.alpha = 1.0 / (1.0 - theta)
	z.zetan = zetan
	z.countForZeta = items
	z.eta = (1 - math.Pow(2.0/float64(items), 1-theta)) / (1 - z.zeta2Theta/z.zetan)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	z.Next(r)
	return z
}

func (z *Zipfian) zeta(st, n int64, thetaVal, initialSum float64) float64 {
	z.countForZeta = n
	return zetaStatic(st, n, thetaVal, initialSum)
}

func zetaStatic(st, n int64, theta, initialSum float64) float64 {
	sum := initialSum
	for i := st; i < n; i++ {
		sum += 1 / math.Pow(float64(i+1), theta)
	}
	return sum
}

func (z *Zipfian) next(r *rand.Rand, itemCount int64) int64 {
	if itemCount != z.countForZeta {
		z.lock.Lock()
		if itemCount > z.countForZeta {
			z.zetan = z.zeta(z.countForZeta, itemCount, z.theta, z.zetan)
			z.eta = (1 - math.Pow(2.0/float64(z.items), 1-z.theta)) / (1 - z.zeta2Theta/z.zetan)
		} else if itemCount < z.countForZeta && z.allowItemCountDecrease {
			z.zetan = z.zeta(0, itemCount, z.theta, 0)
			z.eta = (1 - math.Pow(2.0/float64(z.items), 1-z.theta)) / (1 - z.zeta2Theta/z.zetan)
		}
		z.lock.Unlock()
	}

	u := r.Float64()
	uz := u * z.zetan
	if uz < 1.0 {
		return z.base
	}
	if uz < 1.0+math.Pow(0.5, z.theta) {
		return z.base + 1
	}
	ret := z.base + int64(float64(itemCount)*math.Pow(z.eta*u-z.eta+1, z.alpha))
	z.setLastValue(ret)
	return ret
}

func (z *Zipfian) Next(r *rand.Rand) int64 { return z.next(r, z.items) }

// ScrambledZipfian scatters the popular items across the whole [min,max] range
// (item popularity no longer clusters by adjacency).
type ScrambledZipfian struct {
	number
	gen       *Zipfian
	min       int64
	itemCount int64
}

func NewScrambledZipfian(min, max int64, zipfianConstant float64) *ScrambledZipfian {
	const (
		zetan               = float64(26.46902820178302)
		usedZipfianConstant = float64(0.99)
		itemCount           = int64(10000000000)
	)

	s := new(ScrambledZipfian)
	s.min = min
	s.itemCount = max - min + 1
	if zipfianConstant == usedZipfianConstant {
		s.gen = newZipfian(0, itemCount, zipfianConstant, zetan)
	} else {
		s.gen = NewZipfianWithRange(0, itemCount, zipfianConstant)
	}
	return s
}

func (s *ScrambledZipfian) Next(r *rand.Rand) int64 {
	n := s.gen.Next(r)
	n = s.min + hash64(n)%s.itemCount
	s.setLastValue(n)
	return n
}
