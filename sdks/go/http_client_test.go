package rymedb

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type seenRequest struct {
	method string
	path   string
	auth   string
	body   string
}

func stubServer(t *testing.T, handler func(seen seenRequest) (int, string)) (*HttpClient, *seenRequest) {
	t.Helper()
	var seen seenRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		seen = seenRequest{
			method: r.Method,
			path:   r.URL.RequestURI(),
			auth:   r.Header.Get("Authorization"),
			body:   string(raw),
		}
		status, body := handler(seen)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return NewHttpClient(server.URL, "k"), &seen
}

func TestReadsReadyAndMetrics(t *testing.T) {
	client, seen := stubServer(t, func(seen seenRequest) (int, string) {
		if seen.path == "/ready" {
			return 200, `{"ready":true,"node":"ryme-0","commit":9,"leader":true,"cluster":false}`
		}
		return 200, `{"node":"ryme-0","uptime_secs":31,"commit":9,"rest_count":4,"rest_mean_micros":12.5,"rest_max_micros":40}`
	})
	ready, err := client.Ready()
	if err != nil {
		t.Fatal(err)
	}
	if !ready.Ready || ready.Node != "ryme-0" || ready.Commit != 9 || !ready.Leader || ready.Cluster {
		t.Fatalf("unexpected ready: %+v", ready)
	}
	if seen.path != "/ready" {
		t.Fatalf("unexpected path: %s", seen.path)
	}
	metrics, err := client.Metrics()
	if err != nil {
		t.Fatal(err)
	}
	if metrics.Node != "ryme-0" || metrics.UptimeSecs != 31 || metrics.RestCount != 4 {
		t.Fatalf("unexpected metrics: %+v", metrics)
	}
	if seen.path != "/metrics" {
		t.Fatalf("unexpected path: %s", seen.path)
	}
	if _, err := client.SlowLog(nil, nil); err != nil {
		t.Fatal(err)
	}
	if seen.method != "GET" || seen.path != "/v1/observe/slow" {
		t.Fatalf("unexpected request: %+v", seen)
	}
	limit := 5
	if _, err := client.SlowLog(&limit, nil); err != nil {
		t.Fatal(err)
	}
	if seen.path != "/v1/observe/slow?limit=5" {
		t.Fatalf("unexpected path: %s", seen.path)
	}
	table := "docs"
	if _, err := client.SlowLog(&limit, &table); err != nil {
		t.Fatal(err)
	}
	if seen.path != "/v1/observe/slow?limit=5&table=docs" {
		t.Fatalf("unexpected path: %s", seen.path)
	}
	if _, err := client.Traces(nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if seen.method != "GET" || seen.path != "/v1/traces" {
		t.Fatalf("unexpected request: %+v", seen)
	}
	if _, err := client.Traces(&limit, nil, nil); err != nil {
		t.Fatal(err)
	}
	if seen.path != "/v1/traces?limit=5" {
		t.Fatalf("unexpected path: %s", seen.path)
	}
	name := "kv_put"
	spanTable := "docs"
	if _, err := client.Traces(&limit, &name, &spanTable); err != nil {
		t.Fatal(err)
	}
	if seen.path != "/v1/traces?limit=5&name=kv_put&table=docs" {
		t.Fatalf("unexpected path: %s", seen.path)
	}
}

func TestKvPutSendsTtlQueryAndBearerAuth(t *testing.T) {
	client, seen := stubServer(t, func(seen seenRequest) (int, string) {
		return 200, `{"commit":3}`
	})
	ttl := 60
	out, err := client.KvPut("docs", "a", `{"x":1}`, &ttl)
	if err != nil {
		t.Fatal(err)
	}
	mapped, ok := out.(map[string]any)
	if !ok || mapped["commit"] != float64(3) {
		t.Fatalf("unexpected body: %v", out)
	}
	if seen.method != "PUT" || seen.path != "/v1/kv/docs/a?ttl=60" {
		t.Fatalf("unexpected request: %+v", seen)
	}
	if seen.auth != "Bearer k" || seen.body != `{"x":1}` {
		t.Fatalf("unexpected auth/body: %+v", seen)
	}
}

func TestSqlAndClusterReplacePayloads(t *testing.T) {
	var bodies []string
	client, _ := stubServer(t, func(seen seenRequest) (int, string) {
		bodies = append(bodies, seen.body)
		return 200, `{"ok":true}`
	})
	if _, err := client.Sql("SELECT * FROM docs KEY $1", []string{"a"}); err != nil {
		t.Fatal(err)
	}
	var sqlBody map[string]any
	if err := json.Unmarshal([]byte(bodies[0]), &sqlBody); err != nil {
		t.Fatal(err)
	}
	if sqlBody["sql"] != "SELECT * FROM docs KEY $1" {
		t.Fatalf("unexpected sql body: %s", bodies[0])
	}
	_, err := client.ClusterReplace([]ClusterMember{
		{ID: 0, Addr: "127.0.0.1:9000"},
		{ID: 1, Addr: "127.0.0.1:9001"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(bodies[1], `"id":0`) || !strings.Contains(bodies[1], "127.0.0.1:9001") {
		t.Fatalf("unexpected replace body: %s", bodies[1])
	}
}

func TestKvGetReturnsRawText(t *testing.T) {
	client, _ := stubServer(t, func(seen seenRequest) (int, string) {
		return 200, `{"n":1}`
	})
	got, err := client.KvGet("docs", "a")
	if err != nil {
		t.Fatal(err)
	}
	if got != `{"n":1}` {
		t.Fatalf("unexpected value: %q", got)
	}
}

func TestErrorMapsToTypedHttpError(t *testing.T) {
	client, _ := stubServer(t, func(seen seenRequest) (int, string) {
		return 400, `{"error":"cluster"}`
	})
	_, err := client.ClusterMembers()
	httpErr, ok := err.(*HttpError)
	if !ok {
		t.Fatalf("expected *HttpError, got %T", err)
	}
	if httpErr.Status != 400 || !strings.Contains(httpErr.Body, "cluster") {
		t.Fatalf("unexpected error: %+v", httpErr)
	}
}

func TestBackupEndpoints(t *testing.T) {
	client, seen := stubServer(t, func(seen seenRequest) (int, string) {
		return 200, `{"ok":true}`
	})
	if _, err := client.LatestCheckpoint(); err != nil {
		t.Fatal(err)
	}
	if seen.method != "GET" || seen.path != "/v1/backups/latest" {
		t.Fatalf("unexpected request: %+v", seen)
	}
	if _, err := client.Pitr(1735689600); err != nil {
		t.Fatal(err)
	}
	if seen.path != "/v1/backups/pitr?target=1735689600" {
		t.Fatalf("unexpected request: %+v", seen)
	}
	if _, err := client.Restore(1735689600); err != nil {
		t.Fatal(err)
	}
	if seen.method != "POST" || seen.path != "/v1/backups/restore?target=1735689600" {
		t.Fatalf("unexpected request: %+v", seen)
	}
	backup := "nightly-042"
	if _, err := client.Archive(&backup); err != nil {
		t.Fatal(err)
	}
	if seen.path != "/v1/backups/archive" {
		t.Fatalf("unexpected request: %+v", seen)
	}
	var archived map[string]any
	if err := json.Unmarshal([]byte(seen.body), &archived); err != nil {
		t.Fatal(err)
	}
	if archived["backup_id"] != "nightly-042" {
		t.Fatalf("unexpected body: %s", seen.body)
	}
	if _, err := client.Archives(); err != nil {
		t.Fatal(err)
	}
	if seen.method != "GET" || seen.path != "/v1/backups/archives" {
		t.Fatalf("unexpected request: %+v", seen)
	}
	if _, err := client.BackupDrill(); err != nil {
		t.Fatal(err)
	}
	if seen.method != "GET" || seen.path != "/v1/backups/drill" {
		t.Fatalf("unexpected request: %+v", seen)
	}
	if _, err := client.BackupCopy("nightly-042"); err != nil {
		t.Fatal(err)
	}
	if seen.method != "POST" || seen.path != "/v1/backups/copy" {
		t.Fatalf("unexpected request: %+v", seen)
	}
	var copied map[string]any
	if err := json.Unmarshal([]byte(seen.body), &copied); err != nil {
		t.Fatal(err)
	}
	if copied["backup_id"] != "nightly-042" {
		t.Fatalf("unexpected body: %s", seen.body)
	}
}

func TestRangesEndpoints(t *testing.T) {
	client, seen := stubServer(t, func(seen seenRequest) (int, string) {
		return 200, `{"ok":true}`
	})
	if _, err := client.Ranges(nil); err != nil {
		t.Fatal(err)
	}
	if seen.method != "GET" || seen.path != "/v1/ranges" {
		t.Fatalf("unexpected request: %+v", seen)
	}
	key := "m"
	if _, err := client.Ranges(&key); err != nil {
		t.Fatal(err)
	}
	if seen.method != "GET" || seen.path != "/v1/ranges?key=m" {
		t.Fatalf("unexpected request: %+v", seen)
	}
	if _, err := client.RangeSplit("range-0", "m", "range-a", "range-b", 0); err != nil {
		t.Fatal(err)
	}
	if seen.method != "POST" || seen.path != "/v1/ranges/split" {
		t.Fatalf("unexpected request: %+v", seen)
	}
	var split map[string]any
	if err := json.Unmarshal([]byte(seen.body), &split); err != nil {
		t.Fatal(err)
	}
	if split["id"] != "range-0" || split["mid"] != "m" || split["left_id"] != "range-a" || split["right_id"] != "range-b" || split["expected_epoch"] != float64(0) {
		t.Fatalf("unexpected body: %s", seen.body)
	}
	if _, err := client.RangeMerge("range-a", "range-b", "range-c", 1, 1); err != nil {
		t.Fatal(err)
	}
	if seen.method != "POST" || seen.path != "/v1/ranges/merge" {
		t.Fatalf("unexpected request: %+v", seen)
	}
	var merged map[string]any
	if err := json.Unmarshal([]byte(seen.body), &merged); err != nil {
		t.Fatal(err)
	}
	if merged["left_id"] != "range-a" || merged["right_id"] != "range-b" || merged["merged_id"] != "range-c" || merged["expected_left_epoch"] != float64(1) || merged["expected_right_epoch"] != float64(1) {
		t.Fatalf("unexpected body: %s", seen.body)
	}
	if _, err := client.RangeAutosplit(nil); err != nil {
		t.Fatal(err)
	}
	if seen.method != "POST" || seen.path != "/v1/ranges/autosplit" {
		t.Fatalf("unexpected request: %+v", seen)
	}
	min := uint64(10)
	if _, err := client.RangeAutosplit(&min); err != nil {
		t.Fatal(err)
	}
	if seen.method != "POST" || seen.path != "/v1/ranges/autosplit?min_writes=10" {
		t.Fatalf("unexpected request: %+v", seen)
	}
	if _, err := client.RangeLoads(); err != nil {
		t.Fatal(err)
	}
	if seen.method != "GET" || seen.path != "/v1/ranges/loads" {
		t.Fatalf("unexpected request: %+v", seen)
	}
	if _, err := client.AuthToken("ada", "correct-horse", nil); err != nil {
		t.Fatal(err)
	}
	if seen.method != "POST" || seen.path != "/v1/auth/token" {
		t.Fatalf("unexpected request: %+v", seen)
	}
	var token map[string]any
	if err := json.Unmarshal([]byte(seen.body), &token); err != nil {
		t.Fatal(err)
	}
	if token["id"] != "ada" || token["password"] != "correct-horse" || token["code"] != nil {
		t.Fatalf("unexpected body: %s", seen.body)
	}
	code := "123456"
	if _, err := client.AuthToken("ada", "correct-horse", &code); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(seen.body), &token); err != nil {
		t.Fatal(err)
	}
	if token["code"] != "123456" {
		t.Fatalf("unexpected body: %s", seen.body)
	}
	if _, err := client.AuthRevoke("ryme_deadbeef"); err != nil {
		t.Fatal(err)
	}
	if seen.method != "DELETE" || seen.path != "/v1/auth/keys" {
		t.Fatalf("unexpected request: %+v", seen)
	}
	var revoked map[string]any
	if err := json.Unmarshal([]byte(seen.body), &revoked); err != nil {
		t.Fatal(err)
	}
	if revoked["key"] != "ryme_deadbeef" {
		t.Fatalf("unexpected body: %s", seen.body)
	}
	if _, err := client.PasskeyRegister("ada", "cred-9", "cHVi"); err != nil {
		t.Fatal(err)
	}
	if seen.method != "POST" || seen.path != "/v1/auth/passkey/register" {
		t.Fatalf("unexpected request: %+v", seen)
	}
	if _, err := client.PasskeyVerify("ada", "cred-9", "YXV0aA", "Y2xpZW50", "c2ln"); err != nil {
		t.Fatal(err)
	}
	if seen.path != "/v1/auth/passkey/verify" {
		t.Fatalf("unexpected path: %s", seen.path)
	}
	author := "ada"
	if _, err := client.MigrateApply("m1", "INSERT INTO docs KEY 'k1' VALUE 'v1'", nil); err != nil {
		t.Fatal(err)
	}
	if seen.method != "POST" || seen.path != "/v1/migrate/apply" {
		t.Fatalf("unexpected request: %+v", seen)
	}
	if _, err := client.MigrateApply("m1", "INSERT INTO docs KEY 'k1' VALUE 'v1'", &author); err != nil {
		t.Fatal(err)
	}
	var applied map[string]any
	if err := json.Unmarshal([]byte(seen.body), &applied); err != nil {
		t.Fatal(err)
	}
	if applied["author"] != "ada" {
		t.Fatalf("unexpected body: %s", seen.body)
	}
	if _, err := client.MigrateLedger(); err != nil {
		t.Fatal(err)
	}
	if seen.method != "GET" || seen.path != "/v1/migrate/ledger" {
		t.Fatalf("unexpected request: %+v", seen)
	}
}
