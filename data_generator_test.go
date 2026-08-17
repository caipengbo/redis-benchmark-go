package main

import "testing"

func TestIsSupportedType(t *testing.T) {
	for _, ok := range []string{"string", "hash", "list", "set", "zset"} {
		if !IsSupportedType(ok) {
			t.Errorf("IsSupportedType(%q) = false, want true", ok)
		}
	}
	for _, bad := range []string{"", "String", "stream", "int"} {
		if IsSupportedType(bad) {
			t.Errorf("IsSupportedType(%q) = true, want false", bad)
		}
	}
}

func TestTypedValue(t *testing.T) {
	if v, ok := typedValue(List, 8).([]string); !ok || len(v) != 8 {
		t.Errorf("List typedValue = %#v, want []string len 8", typedValue(List, 8))
	}
	if v, ok := typedValue(Set, 8).([]string); !ok || len(v) != 8 {
		t.Errorf("Set typedValue = %#v, want []string len 8", typedValue(Set, 8))
	}
	if v, ok := typedValue(Hash, 8).(map[string]string); !ok || len(v) != 8 {
		t.Errorf("Hash typedValue = %#v, want map len 8", typedValue(Hash, 8))
	}
	if v, ok := typedValue(ZSet, 8).(map[string]float64); !ok || len(v) != 8 {
		t.Errorf("ZSet typedValue = %#v, want map len 8", typedValue(ZSet, 8))
	}
	if typedValue(String, 8) != nil {
		t.Errorf("String typedValue should be nil")
	}
}
