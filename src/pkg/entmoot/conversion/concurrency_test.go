package conversion

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"entmoot/pkg/entmoot/keystore"
)

func TestCompletedRootDoesNotTouchOperationalDatabase(t *testing.T) {
	root := t.TempDir()
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	if err := Run(root, identity); err != nil {
		t.Fatal(err)
	}
	// A live owner may hold an exclusive transaction in an operational DB.
	// Routine startup must read only the completed conversion journal, not
	// open, integrity-check, or checkpoint this database.
	db, err := sql.Open("sqlite", filepath.Join(root, "live.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE live(value TEXT); BEGIN EXCLUSIVE; INSERT INTO live VALUES('pending')`); err != nil {
		t.Fatal(err)
	}
	defer db.Exec(`ROLLBACK`)
	if err := Run(root, identity); err != nil {
		t.Fatalf("completed-root startup touched the live database: %v", err)
	}
	status, exists, err := ReadStatus(root)
	if err != nil || !exists || status.Stage != StageComplete {
		t.Fatalf("completed journal: %+v exists=%v err=%v", status, exists, err)
	}
}

func TestConcurrentConversionProcesses(t *testing.T) {
	root := t.TempDir()
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	if err := identity.Save(filepath.Join(root, "identity.json")); err != nil {
		t.Fatal(err)
	}
	// Start all processes before releasing a shared stdin barrier. Exercise
	// first conversion, not only concurrent reads of an existing journal.
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const count = 8
	commands := make([]*exec.Cmd, 0, count)
	outputs := make([]bytes.Buffer, count)
	for i := range count {
		command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestConversionProcessHelper$")
		command.Env = append(os.Environ(), "ENTMOOT_CONVERSION_TEST_ROOT="+root)
		command.Stdin = reader
		command.Stdout = &outputs[i]
		command.Stderr = &outputs[i]
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		commands = append(commands, command)
		t.Cleanup(func() {
			if command.ProcessState == nil {
				_ = command.Process.Kill()
				_ = command.Wait()
			}
		})
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	for i, command := range commands {
		if err := command.Wait(); err != nil {
			t.Errorf("converter %d: %v\n%s", i, err, &outputs[i])
		}
	}
	assertStageHistory(t, root)
}

func TestConversionProcessHelper(t *testing.T) {
	root := os.Getenv("ENTMOOT_CONVERSION_TEST_ROOT")
	if root == "" {
		t.Skip("subprocess helper")
	}
	if _, err := io.Copy(io.Discard, os.Stdin); err != nil {
		t.Fatal(err)
	}
	identity, err := keystore.Load(filepath.Join(root, "identity.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := Run(root, identity); err != nil {
		t.Fatal(err)
	}
}
