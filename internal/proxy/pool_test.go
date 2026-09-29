package proxy

import (
	"strings"
	"testing"
)

func TestSIDRotationAndMasking(t *testing.T) {
	pool := New(nil, "http://account-region-TR-sid-{sid}:secret@proxy.example:9000")
	first := pool.Next("first")
	second := pool.Next("second")
	if first == second || !strings.Contains(first, "sid-first") || !strings.Contains(second, "sid-second") {
		t.Fatalf("sticky SID was not replaced: %q %q", first, second)
	}
	if masked := Mask(first); masked != "http://proxy.example:9000" || strings.Contains(masked, "secret") {
		t.Fatalf("unsafe mask: %q", masked)
	}
	pool.ReportFailure(first, "http_429")
	stats := pool.Snapshot()
	if stats.Total != 1 || stats.Cooling != 1 || stats.Failures != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}
