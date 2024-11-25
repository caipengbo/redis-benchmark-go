package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

type Type string

const (
	String Type = "string"
	Hash   Type = "hash"
	List   Type = "list"
	Set    Type = "set"
	ZSet   Type = "zset"
)

type Data struct {
	t     Type
	Key   string
	Value any
}

type DataGenerator struct {
	t         Type
	fieldNum  int
	valueSize int
}

var keyIndex atomic.Int64

func IsSupportedType(t string) bool {
	switch Type(t) {
	case String, Hash, List, Set, ZSet:
		return true
	default:
		return false
	}
}

func NewDataGenerator(t Type, fieldNum, valueSize int) *DataGenerator {
	return &DataGenerator{
		t:         t,
		fieldNum:  fieldNum,
		valueSize: valueSize,
	}
}

func RunDataGenerators(ctx context.Context, generators ...*DataGenerator) chan *Data {
	out := make(chan *Data, 128)

	var wg sync.WaitGroup

	for _, g := range generators {
		ch := g.Run(ctx)

		wg.Add(1)
		go func() {
			defer wg.Done()

			for data := range ch {
				out <- data
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func (g *DataGenerator) Run(ctx context.Context) chan *Data {
	ch := make(chan *Data, 64)

	go func() {
		defer close(ch)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				ch <- g.nextData()
			}
		}
	}()

	return ch
}

func (g *DataGenerator) nextData() *Data {
	data := &Data{
		t:     g.t,
		Key:   g.nextKey(),
		Value: nil,
	}
	keyIndex.Add(1)

	switch g.t {
	case String:
		data.Value = g.nextValue()
	case List, Set:
		data.Value = g.nextValues(g.fieldNum)
	case Hash:
		data.Value = g.nextHashValues(g.fieldNum)
	case ZSet:
		data.Value = g.nextZSetValues(g.fieldNum)
	default:
		log.Panicf("unknown type, %s", g.t)
	}
	return data
}

func (g *DataGenerator) nextKey() string {
	// TODO: 按照指定分布生成key
	return fmt.Sprintf("%s_key_%d", g.t, keyIndex.Load())
}

func (g *DataGenerator) nextValue() string {
	return "value"
}

func (g *DataGenerator) nextValues(n int) []string {
	values := make([]string, 0, n)
	for i := 0; i < n; i++ {
		values = append(values, fmt.Sprintf("value_%d", i))
	}
	return values
}

func (g *DataGenerator) nextHashValues(n int) map[string]string {
	data := make(map[string]string, n)
	for i := 0; i < n; i++ {
		data[fmt.Sprintf("field%d", i)] = fmt.Sprintf("value%d", i)
	}
	return data
}

func (g *DataGenerator) nextZSetValues(n int) map[string]float64 {
	data := make(map[string]float64, n)
	for i := 0; i < n; i++ {
		data[fmt.Sprintf("member%d", i)] = float64(i)
	}
	return data
}
