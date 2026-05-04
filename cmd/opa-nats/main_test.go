package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration runs the docker-compose setup and tests OPA integration
func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Get the examples directory path
	examplesDir := filepath.Join("..", "..", "examples", "opa-nats")

	// Verify the examples directory exists
	if _, err := os.Stat(examplesDir); os.IsNotExist(err) {
		t.Fatalf("Examples directory does not exist: %s", examplesDir)
	}

	// Resolve the test-only compose override that drops host-port bindings on
	// services we don't talk to (NATS, NATS UI). Only OPA's 8181 needs to be
	// reachable from the test runner. This avoids host-port collisions with
	// e.g. the GHA workflow's `services: nats:` (which already binds 4222).
	pwd, err := os.Getwd()
	require.NoError(t, err)
	overrideFile, err := filepath.Abs(filepath.Join(pwd, "docker-compose.test.override.yaml"))
	require.NoError(t, err)
	if _, err := os.Stat(overrideFile); err != nil {
		t.Fatalf("Test override compose file missing at %s: %v", overrideFile, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Change to examples directory for docker-compose
	originalDir, err := os.Getwd()
	require.NoError(t, err)
	defer os.Chdir(originalDir)

	err = os.Chdir(examplesDir)
	require.NoError(t, err)

	// Clean up any existing containers (also runs at end via t.Cleanup).
	composeArgs := []string{"compose", "-f", "docker-compose.yaml", "-f", overrideFile}
	dumpComposeLogs := func(reason string) {
		t.Logf("Dumping diagnostics (%s):", reason)
		// OPA goroutine dump first — most useful for diagnosing hung requests.
		t.Logf("--- BEGIN OPA goroutine dump ---\n%s\n--- END OPA goroutine dump ---", fetchOPAGoroutineDump())
		logsArgs := append([]string{}, composeArgs...)
		logsArgs = append(logsArgs, "logs", "--no-color", "--tail", "200")
		out, _ := exec.Command("docker", logsArgs...).CombinedOutput()
		t.Logf("--- BEGIN compose logs ---\n%s\n--- END compose logs ---", string(out))
		psArgs := append([]string{}, composeArgs...)
		psArgs = append(psArgs, "ps", "-a")
		ps, _ := exec.Command("docker", psArgs...).CombinedOutput()
		t.Logf("--- compose ps ---\n%s", string(ps))
	}
	t.Cleanup(func() {
		_ = os.Chdir(examplesDir)
		if t.Failed() {
			dumpComposeLogs("test failed")
		}
		downArgs := append([]string{}, composeArgs...)
		downArgs = append(downArgs, "down", "-v", "--remove-orphans")
		exec.Command("docker", downArgs...).Run()
	})
	preDown := append([]string{}, composeArgs...)
	preDown = append(preDown, "down", "-v", "--remove-orphans")
	_ = exec.Command("docker", preDown...).Run()

	// Start docker-compose services with the test override applied.
	t.Log("Starting docker-compose services...")
	upArgs := append([]string{}, composeArgs...)
	upArgs = append(upArgs, "up", "-d", "--build")
	cmd := exec.CommandContext(ctx, "docker", upArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	require.NoError(t, err)

	// Wait for OPA to be ready
	t.Log("Waiting for OPA to be ready...")
	if err := waitForOPA(ctx, "http://localhost:8181"); err != nil {
		dumpComposeLogs("OPA never became ready")
		t.Fatalf("OPA never became ready: %v", err)
	}

	// Test OPA health endpoint
	t.Run("OPA Health Check", func(t *testing.T) {
		resp, err := http.Get("http://localhost:8181/health")
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})

	// Test root bucket data is available in OPA data store
	t.Run("Root Bucket Data", func(t *testing.T) {
		// Check if root bucket data is available at data.nats.kv
		result := evaluatePolicy(t, "data.nats.kv", map[string]interface{}{})
		t.Logf("Root bucket data: %+v", result)

		// Should have some data from the root bucket
		if result["result"] != nil {
			t.Log("Successfully found root bucket data in OPA store")
		} else {
			t.Log("No root bucket data found - this might be expected if root bucket is empty")
		}
	})

	// Test bucket watching behavior. The first call to nats.kv.watch_bucket
	// for a fresh bucket spawns an async goroutine that loads bucket data
	// and registers the watcher. We expect bucket_watched=false on the very
	// first call, then bucket_watched=true on a subsequent call once the
	// async registration has completed. Because that registration happens
	// off the request-handling path, we poll instead of asserting that the
	// second call alone is enough.
	t.Run("Bucket Watching Behavior", func(t *testing.T) {
		bucketID := "550e8400-e29b-41d4-a716-446655440000"
		input := map[string]interface{}{
			"bucket_id": bucketID,
		}

		// First call - should show bucket_watched: false
		result1 := evaluatePolicy(t, "data.test", input)
		t.Logf("First call result: %+v", result1)

		require.Contains(t, result1, "result")
		resultData1 := result1["result"].(map[string]interface{})

		// Should have bucket_watched: false on first call
		bucketWatched1, ok := resultData1["bucket_watched"]
		require.True(t, ok, "bucket_watched should be present")
		assert.False(t, bucketWatched1.(bool), "First call should have bucket_watched: false")

		// Should have x field
		x1, ok := resultData1["x"]
		require.True(t, ok, "x should be present")
		t.Logf("First call - bucket_watched: %v, x: %+v", bucketWatched1, x1)

		// Poll until the async watcher registration completes (bucket_watched=true)
		// or we exceed the deadline.
		var resultData2 map[string]interface{}
		require.Eventually(t, func() bool {
			r := evaluatePolicy(t, "data.test", input)
			rd, ok := r["result"].(map[string]interface{})
			if !ok {
				return false
			}
			bw, ok := rd["bucket_watched"].(bool)
			if !ok {
				return false
			}
			if bw {
				resultData2 = rd
			}
			return bw
		}, 30*time.Second, 200*time.Millisecond, "bucket_watched never became true within 30s")

		t.Logf("Subsequent call - bucket_watched: true, x: %+v", resultData2["x"])

		// The two calls intentionally return different shapes for `x`:
		//   bucket_watched=false → x = nats.kv.get_data(bucket, "members")  (members map only)
		//   bucket_watched=true  → x = data.nats.kv[bucket]                  (entire bucket)
		// What we verify is that the members data the unwatched path returns
		// matches the members data the watched path stores under the bucket.
		x1Members, ok1 := x1.(map[string]interface{})
		require.True(t, ok1, "x1 should be a map (members)")
		x2Bucket, ok2 := resultData2["x"].(map[string]interface{})
		require.True(t, ok2, "x2 should be a map (bucket)")
		x2Members, ok2m := x2Bucket["members"].(map[string]interface{})
		require.True(t, ok2m, "x2 should contain members map")
		assert.Equal(t, x1Members, x2Members, "members data should match between unwatched and watched calls")

		t.Log("Bucket watching behavior verified: bucket_watched flips to true once the async watcher registration completes")
	})

	// Test that x returns consistent data regardless of bucket watching state.
	// Use a different bucket so we do not piggyback on the watcher registered
	// by the previous subtest.
	t.Run("Data Consistency Test", func(t *testing.T) {
		bucketID := "660e8400-e29b-41d4-a716-446655440001" // Developers group
		input := map[string]interface{}{
			"bucket_id": bucketID,
		}

		// First call
		first := evaluatePolicy(t, "data.test", input)
		require.Contains(t, first, "result")
		firstData := first["result"].(map[string]interface{})
		t.Logf("First call result: %+v", firstData)
		assert.False(t, firstData["bucket_watched"].(bool), "First call should have bucket_watched: false")

		// Wait for the async registration before checking subsequent calls.
		var watchedData map[string]interface{}
		require.Eventually(t, func() bool {
			r := evaluatePolicy(t, "data.test", input)
			rd, ok := r["result"].(map[string]interface{})
			if !ok {
				return false
			}
			bw, _ := rd["bucket_watched"].(bool)
			if bw {
				watchedData = rd
			}
			return bw
		}, 30*time.Second, 200*time.Millisecond, "bucket_watched never became true within 30s")

		results := []map[string]interface{}{firstData, watchedData}

		// One more call after the watcher is registered — should still see true.
		extra := evaluatePolicy(t, "data.test", input)
		require.Contains(t, extra, "result")
		extraData := extra["result"].(map[string]interface{})
		t.Logf("Post-watcher call result: %+v", extraData)
		results = append(results, extraData)

		assert.False(t, results[0]["bucket_watched"].(bool), "First call should have bucket_watched: false")
		assert.True(t, results[1]["bucket_watched"].(bool), "Post-registration call should have bucket_watched: true")
		assert.True(t, results[2]["bucket_watched"].(bool), "Subsequent call should have bucket_watched: true")

		// As in the previous subtest, the bucket_watched=false branch returns
		// only the `members` submap, while the bucket_watched=true branch
		// returns the entire bucket. Compare the members slice from each
		// representation rather than the raw `x` values.
		x0Members, ok0 := results[0]["x"].(map[string]interface{})
		require.True(t, ok0, "x[0] should be a members map")
		x1Bucket, ok1 := results[1]["x"].(map[string]interface{})
		require.True(t, ok1, "x[1] should be a bucket map")
		x2Bucket, ok2 := results[2]["x"].(map[string]interface{})
		require.True(t, ok2, "x[2] should be a bucket map")

		x1Members, _ := x1Bucket["members"].(map[string]interface{})
		x2Members, _ := x2Bucket["members"].(map[string]interface{})

		assert.Equal(t, x0Members, x1Members, "members data should match between unwatched and post-watcher calls")
		assert.Equal(t, x1Members, x2Members, "members data should be stable across consecutive watched calls")

		t.Logf("Data consistency verified: members data is stable across watched/unwatched and consecutive calls")
	})
}

// waitForOPA waits for OPA to be ready
func waitForOPA(ctx context.Context, url string) error {
	client := &http.Client{Timeout: 5 * time.Second}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			resp, err := client.Get(url + "/health")
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					return nil
				}
			}
			time.Sleep(2 * time.Second)
		}
	}
}

// evaluatePolicy evaluates a policy with input data.
//
// Uses a per-request HTTP timeout of 30s. If OPA hangs (e.g. policy
// evaluation deadlocks), the test fails with a clear timeout error
// within 30s instead of hanging until the Go test framework's 10-minute
// kill — which would prevent t.Cleanup from running and dumping
// compose logs.
func evaluatePolicy(t *testing.T, path string, input map[string]interface{}) map[string]interface{} {
	payload := map[string]interface{}{
		"input": input,
	}

	jsonData, err := json.Marshal(payload)
	require.NoError(t, err)

	// Handle path - remove "data." prefix if present
	apiPath := path
	if strings.HasPrefix(path, "data.") {
		apiPath = path[5:] // Remove "data." prefix
	}

	url := fmt.Sprintf("http://localhost:8181/v1/data/%s", apiPath)
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Post(url, "application/json", bytes.NewBuffer(jsonData))
	require.NoError(t, err, "POST %s timed out or failed (OPA likely hung) — see compose logs in cleanup", url)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var result map[string]interface{}
	err = json.Unmarshal(body, &result)
	require.NoError(t, err)

	return result
}

// fetchOPAGoroutineDump returns OPA's full goroutine stack via pprof.
// OPA must be started with --pprof for this to work; the test compose
// override adds it. Returns the empty string if pprof is unavailable.
func fetchOPAGoroutineDump() string {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get("http://localhost:8181/debug/pprof/goroutine?debug=2")
	if err != nil {
		return fmt.Sprintf("(failed to fetch goroutine dump: %v)", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Sprintf("(pprof returned HTTP %d)", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Sprintf("(failed to read pprof body: %v)", err)
	}
	return string(body)
}
