package main

import "fmt"

type Type string

const (
	String Type = "string"
	Hash   Type = "hash"
	List   Type = "list"
	Set    Type = "set"
	ZSet   Type = "zset"
)

func IsSupportedType(t string) bool {
	switch Type(t) {
	case String, Hash, List, Set, ZSet:
		return true
	default:
		return false
	}
}

// typedValue returns the deterministic payload for a non-string type. It is
// independent of the key, so it is computed once per workload and shared
// read-only across every write, avoiding per-command allocations.
func typedValue(t Type, fieldNum int) any {
	switch t {
	case List, Set:
		return typedListValues(fieldNum)
	case Hash:
		return typedHashValues(fieldNum)
	case ZSet:
		return typedZSetValues(fieldNum)
	default:
		return nil
	}
}

func typedListValues(n int) []string {
	values := make([]string, 0, n)
	for i := 0; i < n; i++ {
		values = append(values, fmt.Sprintf("value_%d", i))
	}
	return values
}

func typedHashValues(n int) map[string]string {
	data := make(map[string]string, n)
	for i := 0; i < n; i++ {
		data[fmt.Sprintf("field%d", i)] = fmt.Sprintf("value%d", i)
	}
	return data
}

func typedZSetValues(n int) map[string]float64 {
	data := make(map[string]float64, n)
	for i := 0; i < n; i++ {
		data[fmt.Sprintf("member%d", i)] = float64(i)
	}
	return data
}
