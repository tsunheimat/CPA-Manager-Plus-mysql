package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	mysqlrepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
)

func TestMySQLServerProcess(t *testing.T) {
	if os.Getenv("CPA_MYSQL_PROCESS_HELPER") == "1" {
		runServer()
		return
	}
	if !testdb.MySQL() {
		t.Skip("CPA_TEST_MYSQL_DSN is not set")
	}
	dir := t.TempDir()
	dsn := testdb.DSN(t, dir)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	listener.Close()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	start := func() (*exec.Cmd, <-chan error, *bytes.Buffer) {
		cmd := exec.Command(binary, "-test.run=^TestMySQLServerProcess$")
		cmd.Env = append(os.Environ(), "CPA_MYSQL_PROCESS_HELPER=1", "USAGE_DB_DRIVER=mysql", "USAGE_DB_DSN="+dsn, "USAGE_DB_DSN_FILE=", "USAGE_DATA_DIR="+dir, "USAGE_DB_PATH="+filepath.Join(dir, "must-not-exist.sqlite"), "CPA_MANAGER_DATA_KEY_PATH="+filepath.Join(dir, "data.key"), "CPA_MANAGER_CONFIG="+filepath.Join(dir, "config.json"), "HTTP_ADDR="+addr, "CPA_UPSTREAM_URL=", "CPA_MANAGEMENT_KEY=", "CPA_MANAGER_ADMIN_KEY=cpamp_test_process_key_0123456789")
		var output bytes.Buffer
		cmd.Stdout = &output
		cmd.Stderr = &output
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		t.Cleanup(func() {
			if cmd.ProcessState == nil {
				cmd.Process.Kill()
				<-done
			}
		})
		client := http.Client{Timeout: 500 * time.Millisecond}
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			resp, err := client.Get("http://" + addr + "/health")
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					return cmd, done, &output
				}
			}
			select {
			case err := <-done:
				t.Fatalf("server exited during startup: %v: %s", err, output.String())
			default:
			}
			time.Sleep(30 * time.Millisecond)
		}
		t.Fatal("MySQL server did not become healthy")
		return nil, nil, nil
	}
	_, done, output := start()
	db, err := mysqlrepo.OpenNative(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("insert into settings(`key`,value,updated_at_ms) values('process-test','persisted',0)"); err != nil {
		t.Fatal(err)
	}
	var name string
	if err := db.QueryRow("select database()").Scan(&name); err != nil {
		t.Fatal(err)
	}
	lockName := fmt.Sprintf("cpamp-process-%x", sha256.Sum256([]byte(name)))[:60]
	var connectionID int64
	if err := db.QueryRow("select is_used_lock(?)", lockName).Scan(&connectionID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(fmt.Sprintf("kill connection %d", connectionID)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("server did not fail after losing database ownership")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("server kept serving after its MySQL lock was lost")
	}
	if !bytes.Contains(output.Bytes(), []byte("ownership lost")) {
		t.Fatalf("unexpected exit: %s", output.String())
	}
	if _, err := os.Stat(filepath.Join(dir, "must-not-exist.sqlite")); !os.IsNotExist(err) {
		t.Fatal("MySQL runtime created a SQLite database")
	}
	restarted, done, _ := start()
	var value string
	if err := db.QueryRow("select value from settings where `key`='process-test'").Scan(&value); err != nil || value != "persisted" {
		t.Fatalf("restart lost state %q: %v", value, err)
	}
	restarted.Process.Kill()
	<-done
}
