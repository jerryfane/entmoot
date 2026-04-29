package main

import "testing"

func TestNormalizeLocalPilotHostname(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
		ok   bool
	}{
		{name: "hostname", in: "mars.local", want: "mars.local", ok: true},
		{name: "trim", in: "  mars.local  ", want: "mars.local", ok: true},
		{name: "empty", in: "", ok: false},
		{name: "spaces", in: "   ", ok: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := normalizeLocalPilotHostname(tt.in)
			if got != tt.want || ok != tt.ok {
				t.Fatalf("normalizeLocalPilotHostname(%q) = %q, %v; want %q, %v", tt.in, got, ok, tt.want, tt.ok)
			}
		})
	}
}
