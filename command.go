package main

import (
	"strings"
	"sync/atomic"
)

// valueSizer decides the size of the value written for __data__ (command mode).
type valueSizer struct {
	fixed int
	min   int
	max   int
}

func (v valueSizer) size(st *workerState) int {
	s := v.fixed
	if s == 0 {
		s = v.min + st.r.Intn(v.max-v.min+1)
	}
	if s > len(st.valBuf) {
		s = len(st.valBuf)
	}
	return s
}

func (v valueSizer) maxSize() int {
	if v.fixed > 0 {
		return v.fixed
	}
	if v.max > 0 {
		return v.max
	}
	return 1
}

// commandSpec is an arbitrary command template. Tokens are the whitespace-split
// command; the placeholders __key__ and __data__ are replaced per operation.
// Each command has its own ratio, key chooser and latency stats.
type commandSpec struct {
	name       string
	tokens     []string
	keyChooser Generator
	ratio      int
	hasData    bool // true if the template contains a __data__ placeholder

	hist  *latencyHist
	count atomic.Int64
}

func newCommandSpec(template string, ratio int, keyChooser Generator) *commandSpec {
	toks := strings.Fields(template)
	name := ""
	if len(toks) > 0 {
		name = strings.ToUpper(toks[0])
	}
	if ratio < 1 {
		ratio = 1
	}
	hasData := false
	for _, t := range toks {
		if t == "__data__" {
			hasData = true
			break
		}
	}
	return &commandSpec{
		name:       name,
		tokens:     toks,
		keyChooser: keyChooser,
		ratio:      ratio,
		hasData:    hasData,
		hist:       newLatencyHist(),
	}
}

// buildArgs materializes the command args for one operation, substituting
// __key__ (a key chosen via this command's distribution) and __data__ (the
// given value buffer). A fresh args slice is returned each call because the
// pipeline retains it until Exec.
func (cs *commandSpec) buildArgs(st *workerState, prefix string, zeroPad int, val []byte) []interface{} {
	args := make([]interface{}, len(cs.tokens))
	var key string
	keySet := false
	for i, t := range cs.tokens {
		switch t {
		case "__key__":
			if !keySet {
				key = buildKeyNameStd(st, prefix, zeroPad, cs.keyChooser.Next(st.r))
				keySet = true
			}
			args[i] = key
		case "__data__":
			args[i] = val
		default:
			args[i] = t
		}
	}
	return args
}
