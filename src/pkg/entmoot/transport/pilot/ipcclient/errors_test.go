package ipcclient

import (
	"errors"
	"testing"
)

func TestIPCErrorClassifiesConnectionLifecycleErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want error
	}{
		{
			name: "dynamic connection id not found",
			err:  &IPCError{Message: "connection 123 not found"},
			want: ErrConnectionNotFound,
		},
		{
			name: "not established",
			err:  &IPCError{Message: "connection not established"},
			want: ErrConnectionNotEstablished,
		},
		{
			name: "closing",
			err:  &IPCError{Message: "connection closing"},
			want: ErrConnectionClosing,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !errors.Is(tc.err, tc.want) {
				t.Fatalf("errors.Is(%v, %v) = false", tc.err, tc.want)
			}
		})
	}
}
