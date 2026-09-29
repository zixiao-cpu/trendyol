package bypass

import (
	"testing"
	"time"
)

func TestDisabledAndBudgetReservation(t *testing.T) {
	disabled, err := New(false, "key", time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer disabled.Close()
	if disabled.reserve(3) {
		t.Fatal("disabled CloudBypass reserved budget")
	}

	limited, err := New(true, "key", time.Second, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer limited.Close()
	if !limited.reserve(3) || limited.reserve(3) {
		t.Fatal("budget limit not enforced")
	}
	stats := limited.Snapshot()
	if stats.Attempts != 1 || stats.Spent != 3 {
		t.Fatalf("attempt accounting mismatch: %+v", stats)
	}
	restarted, err := New(true, "key", time.Second, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	restarted.SetPersistedSpent(3)
	if restarted.reserve(3) {
		t.Fatal("persisted budget spend was ignored")
	}
}
