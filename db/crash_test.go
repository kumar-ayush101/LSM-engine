package db

import (
	"bufio"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The crash test runs the test binary itself as a child process (selected
// by crashChildEnv). The child writes keys as fast as it can and prints
// "ACK <writer> <i>" to stdout only after Put returns. The parent kills it
// with SIGKILL / TerminateProcess at a random moment, reopens the DB, and
// checks that every acknowledged write survived.
//
// This verifies the guarantee for process crashes. Power loss is not
// simulated; that depends on fsync, which the policies document.
const (
	crashChildEnv  = "LSM_CRASH_CHILD_DIR"
	crashPolicyEnv = "LSM_CRASH_CHILD_POLICY"
	crashRoundEnv  = "LSM_CRASH_CHILD_ROUND"
	crashWriters   = 4
)

func crashKey(round, writer, i int) string { return fmt.Sprintf("r%d-w%d-%06d", round, writer, i) }
func crashValue(key string) string         { return "value-of-" + key }

// TestCrashChild is the child process body. It is skipped in normal runs.
func TestCrashChild(t *testing.T) {
	dir := os.Getenv(crashChildEnv)
	if dir == "" {
		t.Skip("helper process for TestCrashRecovery")
	}
	pol, _ := ParseSyncPolicy(os.Getenv(crashPolicyEnv))
	round, _ := strconv.Atoi(os.Getenv(crashRoundEnv))
	d, err := Open(dir, &Options{Sync: pol})
	if err != nil {
		fmt.Println("ERR", err)
		os.Exit(2)
	}
	fmt.Println("READY")

	var outMu sync.Mutex
	out := bufio.NewWriter(os.Stdout)
	var wg sync.WaitGroup
	for w := 0; w < crashWriters; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				k := crashKey(round, w, i)
				if i%5 == 4 {
					// Exercise tombstones: delete the previous key.
					prev := crashKey(round, w, i-1)
					if err := d.Delete([]byte(prev)); err != nil {
						return
					}
					outMu.Lock()
					fmt.Fprintf(out, "DEL %d %d\n", w, i-1)
					out.Flush()
					outMu.Unlock()
				}
				if err := d.Put([]byte(k), []byte(crashValue(k))); err != nil {
					return
				}
				outMu.Lock()
				fmt.Fprintf(out, "ACK %d %d\n", w, i)
				out.Flush()
				outMu.Unlock()
			}
		}(w)
	}
	wg.Wait() // never returns normally; the parent kills us
}

func TestCrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("crash test spawns processes")
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, pol := range allPolicies {
		t.Run(pol.String(), func(t *testing.T) {
			dir := t.TempDir()
			r := rand.New(rand.NewPCG(uint64(pol), 99))
			acked := map[string]bool{}   // key -> must exist
			deleted := map[string]bool{} // key -> deletion acknowledged
			mayBeDeleted := map[string]bool{}

			const rounds = 4
			for round := 0; round < rounds; round++ {
				cmd := exec.Command(exe, "-test.run=^TestCrashChild$", "-test.count=1")
				cmd.Env = append(os.Environ(),
					crashChildEnv+"="+dir,
					crashPolicyEnv+"="+pol.String(),
					crashRoundEnv+"="+strconv.Itoa(round))
				stdout, err := cmd.StdoutPipe()
				if err != nil {
					t.Fatal(err)
				}
				if err := cmd.Start(); err != nil {
					t.Fatal(err)
				}

				lines := make(chan string, 1024)
				go func() {
					sc := bufio.NewScanner(stdout)
					for sc.Scan() {
						lines <- sc.Text()
					}
					close(lines)
				}()

				killAt := time.After(time.Duration(100+r.IntN(400)) * time.Millisecond)
				ready, killed := false, false
				roundAcks := 0
			read:
				for {
					select {
					case <-killAt:
						if !ready {
							killAt = time.After(50 * time.Millisecond)
							continue
						}
						cmd.Process.Kill() // SIGKILL on Unix, TerminateProcess on Windows
						killed = true
						killAt = nil
					case line, ok := <-lines:
						if !ok {
							break read
						}
						f := strings.Fields(line)
						switch {
						case len(f) == 1 && f[0] == "READY":
							ready = true
						case len(f) == 3 && (f[0] == "ACK" || f[0] == "DEL"):
							w, _ := strconv.Atoi(f[1])
							i, _ := strconv.Atoi(f[2])
							k := crashKey(round, w, i)
							if f[0] == "ACK" {
								acked[k] = true
								roundAcks++
								if i%5 == 3 {
									// The child deletes this key next. If it is killed
									// after the delete but before printing DEL, the key
									// is legitimately absent, so either state is valid.
									mayBeDeleted[k] = true
								}
							} else {
								deleted[k] = true
							}
						default:
							t.Fatalf("child: %s", line)
						}
					}
				}
				cmd.Wait()
				if !killed {
					t.Fatalf("round %d: child exited before being killed", round)
				}
				if roundAcks == 0 {
					t.Fatalf("round %d: child acknowledged no writes", round)
				}

				// Reopen and verify every acknowledged operation survived.
				d, err := Open(dir, &Options{Sync: pol})
				if err != nil {
					t.Fatalf("round %d: reopen after kill: %v", round, err)
				}
				lost := 0
				for k := range acked {
					v, err := d.Get([]byte(k))
					if deleted[k] {
						if err == nil {
							t.Errorf("round %d: %s was deleted (acked) but is present", round, k)
						}
						continue
					}
					if err != nil && mayBeDeleted[k] {
						continue
					}
					if err != nil || string(v) != crashValue(k) {
						lost++
						if lost <= 5 {
							t.Errorf("round %d: acknowledged %s lost: %q, %v", round, k, v, err)
						}
					}
				}
				st := d.Stats()
				if err := d.Close(); err != nil {
					t.Fatal(err)
				}
				t.Logf("round %d: killed after %d acks this round; %d acked total, %d lost; memtable entries %d, WAL %d bytes",
					round, roundAcks, len(acked), lost, st.Entries, st.WALBytes)
				if lost > 0 {
					t.FailNow()
				}
			}
		})
	}
}
