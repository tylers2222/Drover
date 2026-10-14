package herdr

import "testing"

func TestSatisfies(t *testing.T) {
	cases := []struct {
		got, want AgentStatus
		ends      bool
	}{
		{StatusIdle, StatusIdle, true},
		{StatusDone, StatusDone, true},
		{StatusDone, StatusIdle, true},
		{StatusIdle, StatusDone, true},
		{StatusWorking, StatusWorking, true},
		{StatusBlocked, StatusBlocked, true},
		{StatusWorking, StatusIdle, false},
		{StatusWorking, StatusDone, false},
		{StatusBlocked, StatusIdle, false},
		{StatusUnknown, StatusDone, false},
		{StatusIdle, StatusWorking, false},
		{StatusDone, StatusWorking, false},
		{StatusIdle, StatusBlocked, false},
	}
	for _, c := range cases {
		if got := c.got.Satisfies(c.want); got != c.ends {
			t.Errorf("%q.Satisfies(%q) = %v, want %v", c.got, c.want, got, c.ends)
		}
	}
}
