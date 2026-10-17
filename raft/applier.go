package raft

// Applier is the replicated state machine. The core calls Apply once for each
// committed command, strictly in log order, from a single goroutine, so an
// implementation need not serialize calls itself.
//
// Apply must be deterministic: given the same sequence of commands every
// replica must reach the same state. The LSM engine satisfies this because a
// Put or Delete depends only on its own key and value, not on wall-clock time
// or any node-local state. A no-op command (CmdNoop) is passed through as a
// zero Command and should be ignored.
//
// Apply errors are fatal to replication: if the state machine cannot apply a
// committed entry, the replica's state has diverged from the log and the node
// must stop. The core surfaces the error and halts the apply loop.
type Applier interface {
	Apply(cmd Command) error
}

// ApplierFunc adapts a function to the Applier interface (used in tests).
type ApplierFunc func(Command) error

func (f ApplierFunc) Apply(cmd Command) error { return f(cmd) }
