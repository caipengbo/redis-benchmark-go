package main

import (
	"math/rand"
	"strconv"
	"time"
)

// Operation is a single reusable command descriptor filled by a Workload.
type Operation struct {
	key      string
	strVal   []byte // value for a string SET
	typedVal any    // value for a non-string write (Hash/List/Set/ZSet)
	ttl      time.Duration
}

// Workload decides, for its data type, which key to touch (following a
// distribution over a bounded key space), what value to write, and the TTL.
// It keeps a shared, immutable config plus shared key choosers, with per-worker
// mutable state (rand + buffers) kept in workerState. Read/write mix is chosen
// per batch by the caller via pickRead.
type Workload struct {
	t           Type
	keyPrefix   string
	zeroPadding int
	keyChooser  Generator // shared; Next takes the per-worker *rand.Rand
	keyMin      int64
	keyMax      int64

	// operation weights (SET:GET). For non-string types getW is forced to 0.
	setW    int
	getW    int
	opTotal int

	// string value sizing
	dataSize    int // fixed size; 0 means use [dataSizeMin,dataSizeMax]
	dataSizeMin int
	dataSizeMax int
	randomData  bool

	// non-string constant payload, pre-built into the exact pipeline arg form
	// ([]interface{} for Hash/List/Set, []redis.Z for ZSet) and shared read-only.
	typedVal any

	// TTL: a fixed value, or a random value in [ttlMin,ttlMax] when hasTTLRange.
	ttlFixed    time.Duration
	ttlMin      time.Duration
	ttlMax      time.Duration
	hasTTLRange bool
}

// workerState is per-worker mutable state, never shared between goroutines.
type workerState struct {
	r      *rand.Rand
	valBuf []byte // preallocated value buffer sliced per write
	keyB   []byte // reusable key builder buffer
}

func newKeyChooser(pattern string, min, max int64, zipfExp float64) Generator {
	switch pattern {
	case "S", "s":
		return NewSequential(min, max)
	case "Z", "z":
		// Fast O(1) construction only for the default constant; other exponents
		// build a clustered Zipfian over the real range (O(range) once).
		if zipfExp == ZipfianConstant {
			return NewScrambledZipfian(min, max, ZipfianConstant)
		}
		return NewZipfianWithRange(min, max, zipfExp)
	default: // "R"/uniform
		return NewUniform(min, max)
	}
}

func (w *Workload) maxValueSize() int {
	if w.dataSize > 0 {
		return w.dataSize
	}
	if w.dataSizeMax > 0 {
		return w.dataSizeMax
	}
	return 1
}

// newWorkerState builds per-worker mutable state. bufSize is the max value size
// across all workloads; randomData controls whether the value buffer is filled
// with random bytes or a constant.
func newWorkerState(seed int64, bufSize int, randomData bool) *workerState {
	if bufSize < 1 {
		bufSize = 1
	}
	buf := make([]byte, bufSize)
	r := rand.New(rand.NewSource(seed))
	if randomData {
		_, _ = r.Read(buf)
	} else {
		for i := range buf {
			buf[i] = 'x'
		}
	}
	return &workerState{r: r, valBuf: buf, keyB: make([]byte, 0, 64)}
}

// pickRead reports whether the next batch should be reads, based on the SET:GET
// ratio. Kind is chosen per batch so per-batch Exec latency attributes cleanly
// to read vs write.
func (w *Workload) pickRead(st *workerState) bool {
	if w.getW == 0 {
		return false
	}
	if w.setW == 0 {
		return true
	}
	return st.r.Intn(w.opTotal) >= w.setW
}

func (w *Workload) buildKeyName(num int64, st *workerState) string {
	return buildKeyNameStd(st, w.keyPrefix, w.zeroPadding, num)
}

// buildKeyNameStd builds "{prefix}{number}" (optionally zero-padded) into the
// worker's reusable buffer. Shared by the workload and command modes.
func buildKeyNameStd(st *workerState, prefix string, zeroPad int, num int64) string {
	st.keyB = st.keyB[:0]
	st.keyB = append(st.keyB, prefix...)
	if zeroPad == 0 {
		st.keyB = strconv.AppendInt(st.keyB, num, 10)
	} else {
		s := strconv.FormatInt(num, 10)
		for i := len(s); i < zeroPad; i++ {
			st.keyB = append(st.keyB, '0')
		}
		st.keyB = append(st.keyB, s...)
	}
	return string(st.keyB)
}

func (w *Workload) pickTTL(st *workerState) time.Duration {
	if w.hasTTLRange {
		return w.ttlMin + time.Duration(st.r.Int63n(int64(w.ttlMax-w.ttlMin)+1))
	}
	return w.ttlFixed
}

func (w *Workload) pickValueSize(st *workerState) int {
	size := w.dataSize
	if size == 0 {
		size = w.dataSizeMin + st.r.Intn(w.dataSizeMax-w.dataSizeMin+1)
	}
	if size > len(st.valBuf) {
		size = len(st.valBuf)
	}
	return size
}

// fillKey sets only the key (used for reads).
func (w *Workload) fillKey(st *workerState, op *Operation) {
	op.key = w.buildKeyName(w.keyChooser.Next(st.r), st)
}

// fillWrite sets the key, value and ttl for a write, choosing the key via the
// configured distribution.
func (w *Workload) fillWrite(st *workerState, op *Operation) {
	w.fillWriteAt(st, op, w.keyChooser.Next(st.r))
}

// fillWriteAt fills a write for an explicit key number (used by the load phase).
func (w *Workload) fillWriteAt(st *workerState, op *Operation, keyNum int64) {
	op.key = w.buildKeyName(keyNum, st)
	op.ttl = w.pickTTL(st)
	if w.t == String {
		op.strVal = st.valBuf[:w.pickValueSize(st)]
		op.typedVal = nil
	} else {
		op.strVal = nil
		op.typedVal = w.typedVal
	}
}

// WorkloadConfig holds the parameters needed to build a Workload for one type.
type WorkloadConfig struct {
	Type        Type
	KeyPrefix   string
	KeyMin      int64
	KeyMax      int64
	KeyPattern  string
	ZipfExp     float64
	ZeroPadding int

	SetWeight int
	GetWeight int

	FieldNum    int // fields for non-string types
	DataSize    int
	DataSizeMin int
	DataSizeMax int
	RandomData  bool

	TTLFixed    time.Duration
	TTLMin      time.Duration
	TTLMax      time.Duration
	HasTTLRange bool
}

func NewWorkload(c WorkloadConfig) *Workload {
	setW, getW := c.SetWeight, c.GetWeight
	// Reads are only supported for the string type; force write-only otherwise.
	if c.Type != String {
		getW = 0
	}
	if setW == 0 && getW == 0 {
		setW = 1
	}

	w := &Workload{
		t:           c.Type,
		keyPrefix:   c.KeyPrefix,
		zeroPadding: c.ZeroPadding,
		keyChooser:  newKeyChooser(c.KeyPattern, c.KeyMin, c.KeyMax, c.ZipfExp),
		keyMin:      c.KeyMin,
		keyMax:      c.KeyMax,
		setW:        setW,
		getW:        getW,
		opTotal:     setW + getW,
		dataSize:    c.DataSize,
		dataSizeMin: c.DataSizeMin,
		dataSizeMax: c.DataSizeMax,
		randomData:  c.RandomData,
		ttlFixed:    c.TTLFixed,
		ttlMin:      c.TTLMin,
		ttlMax:      c.TTLMax,
		hasTTLRange: c.HasTTLRange,
	}
	if c.Type != String {
		w.typedVal = typedPipelineArgs(c.Type, typedValue(c.Type, c.FieldNum))
	}
	return w
}
