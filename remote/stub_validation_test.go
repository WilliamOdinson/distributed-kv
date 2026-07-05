package remote

// Tests for the interface-validation rules enforced by NewCalleeStub and
// CallerStubCreator. These are pure reflection checks with no network I/O, so
// they run instantly and pin down exactly which interfaces are accepted.

import "testing"

// validationCases enumerates the interface shapes both constructors must judge
// identically. accept is true when the interface is a valid service interface.
var validationCases = []struct {
	name   string
	iface  any
	accept bool
}{
	{"valid service interface", &testService{}, true},
	{"nil interface", nil, false},
	{"non-struct interface", ptrTo(notAStruct(0)), false},
	{"method missing RemoteError", &invalidService{}, false},
}

func ptrTo[T any](v T) *T { return &v }

func TestNewCalleeStub_InterfaceValidation(t *testing.T) {
	addr := freeAddr(t)
	for _, tc := range validationCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("step: NewCalleeStub with %q interface, want accept=%v", tc.name, tc.accept)
			_, err := NewCalleeStub(tc.iface, &testInstance{}, addr, false, false)
			if tc.accept && err != nil {
				t.Fatalf("expected acceptance, got error: %v", err)
			}
			if !tc.accept && err == nil {
				t.Fatal("expected rejection, but NewCalleeStub accepted the interface")
			}
			t.Logf("ok: %q judged as accept=%v", tc.name, tc.accept)
		})
	}
}

func TestNewCalleeStub_NilInstanceRejected(t *testing.T) {
	addr := freeAddr(t)
	t.Log("step: NewCalleeStub with a nil service instance should be rejected")
	if _, err := NewCalleeStub(&testService{}, nil, addr, false, false); err == nil {
		t.Fatal("NewCalleeStub accepted a nil service instance")
	}
	t.Log("ok: nil service instance rejected")
}

func TestCallerStubCreator_InterfaceValidation(t *testing.T) {
	addr := freeAddr(t)
	for _, tc := range validationCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("step: CallerStubCreator with %q interface, want accept=%v", tc.name, tc.accept)
			err := CallerStubCreator(tc.iface, addr, false, false)
			if tc.accept && err != nil {
				t.Fatalf("expected acceptance, got error: %v", err)
			}
			if !tc.accept && err == nil {
				t.Fatal("expected rejection, but CallerStubCreator accepted the interface")
			}
			t.Logf("ok: %q judged as accept=%v", tc.name, tc.accept)
		})
	}
}

// A value (non-pointer) service interface should also be accepted by
// CallerStubCreator's type check, though only a pointer can actually be
// populated; this documents that the Kind()-based validation dereferences
// pointers before inspecting fields.
func TestCallerStubCreator_ValueStructValidates(t *testing.T) {
	// A pointer is required for the stub to be writable; passing a value here
	// would panic on Set. We assert the pointer path succeeds.
	addr := freeAddr(t)
	t.Log("step: CallerStubCreator with a pointer-to-struct interface should validate")
	if err := CallerStubCreator(&testService{}, addr, false, false); err != nil {
		t.Fatalf("expected pointer-to-struct interface to validate, got: %v", err)
	}
	t.Log("ok: pointer-to-struct interface validated")
}
