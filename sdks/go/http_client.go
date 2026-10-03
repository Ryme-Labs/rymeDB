package rymedb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type HttpError struct {
	Status int
	Body   string
}

func (e *HttpError) Error() string {
	return fmt.Sprintf("rymeDB HTTP %d: %s", e.Status, e.Body)
}

type HttpClient struct {
	base   string
	apiKey string
	http   *http.Client
}

type ClusterMember struct {
	ID   int    `json:"id"`
	Addr string `json:"addr"`
	Self bool   `json:"self"`
}

type ClusterStatus struct {
	Term    int             `json:"term"`
	Leader  bool            `json:"leader"`
	Commit  int             `json:"commit"`
	Members []ClusterMember `json:"members"`
	Joint   []int           `json:"joint"`
}

func NewHttpClient(base string, apiKey string) *HttpClient {
	if apiKey == "" {
		apiKey = os.Getenv("RYME_API_KEY")
	}
	return &HttpClient{
		base:   strings.TrimRight(base, "/"),
		apiKey: apiKey,
		http:   &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *HttpClient) request(method string, path string, body any) (any, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, c.base+path, reader)
	if err != nil {
		return nil, err
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &HttpError{Status: resp.StatusCode, Body: string(raw)}
	}
	if len(raw) == 0 {
		return nil, nil
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return string(raw), nil
	}
	return decoded, nil
}

func (c *HttpClient) Health() bool {
	resp, err := c.http.Get(c.base + "/health")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

type ReadyStatus struct {
	Ready   bool   `json:"ready"`
	Node    string `json:"node"`
	Commit  uint64 `json:"commit"`
	Leader  bool   `json:"leader"`
	Cluster bool   `json:"cluster"`
}

func (c *HttpClient) Ready() (ReadyStatus, error) {
	var status ReadyStatus
	out, err := c.request("GET", "/ready", nil)
	if err != nil {
		return status, err
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return status, err
	}
	if err := json.Unmarshal(raw, &status); err != nil {
		return status, err
	}
	return status, nil
}

type NodeMetrics struct {
	Node           string  `json:"node"`
	UptimeSecs     uint64  `json:"uptime_secs"`
	Commit         uint64  `json:"commit"`
	RestCount      uint64  `json:"rest_count"`
	RestMeanMicros float64 `json:"rest_mean_micros"`
	RestMaxMicros  uint64  `json:"rest_max_micros"`
	P50Micros      uint64  `json:"p50_micros"`
	P90Micros      uint64  `json:"p90_micros"`
	P95Micros      uint64  `json:"p95_micros"`
	P99Micros      uint64  `json:"p99_micros"`
}

func (c *HttpClient) Metrics() (NodeMetrics, error) {
	var metrics NodeMetrics
	out, err := c.request("GET", "/metrics", nil)
	if err != nil {
		return metrics, err
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return metrics, err
	}
	if err := json.Unmarshal(raw, &metrics); err != nil {
		return metrics, err
	}
	return metrics, nil
}

func (c *HttpClient) SlowLog(limit *int, table *string) (any, error) {
	query := url.Values{}
	if limit != nil {
		query.Set("limit", strconv.Itoa(*limit))
	}
	if table != nil {
		query.Set("table", *table)
	}
	path := "/v1/observe/slow"
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return c.request("GET", path, nil)
}

func (c *HttpClient) Traces(limit *int, name *string, table *string) (any, error) {
	query := url.Values{}
	if limit != nil {
		query.Set("limit", strconv.Itoa(*limit))
	}
	if name != nil {
		query.Set("name", *name)
	}
	if table != nil {
		query.Set("table", *table)
	}
	path := "/v1/traces"
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return c.request("GET", path, nil)
}

func (c *HttpClient) requestText(method string, path string, body string) (string, error) {
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, c.base+path, reader)
	if err != nil {
		return "", err
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", &HttpError{Status: resp.StatusCode, Body: string(raw)}
	}
	return string(raw), nil
}

func (c *HttpClient) KvGet(table string, key string) (string, error) {
	return c.requestText("GET", "/v1/kv/"+table+"/"+key, "")
}

func (c *HttpClient) KvPut(table string, key string, value string, ttl *int) (any, error) {
	path := "/v1/kv/" + table + "/" + key
	if ttl != nil {
		path += fmt.Sprintf("?ttl=%d", *ttl)
	}
	raw, err := c.requestText("PUT", path, value)
	if err != nil {
		return nil, err
	}
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return raw, nil
	}
	return decoded, nil
}

func (c *HttpClient) KvDelete(table string, key string) (any, error) {
	return c.request("DELETE", "/v1/kv/"+table+"/"+key, nil)
}

func (c *HttpClient) KvTtl(table string, key string) (any, error) {
	return c.request("GET", "/v1/kv/"+table+"/"+key+"/ttl", nil)
}

func (c *HttpClient) Sql(sql string, params []string) (any, error) {
	return c.request("POST", "/v1/sql", map[string]any{"sql": sql, "params": params})
}

func (c *HttpClient) Scan(table string, limit *int) (any, error) {
	path := "/v1/scan/" + table
	if limit != nil {
		path += fmt.Sprintf("?limit=%d", *limit)
	}
	return c.request("GET", path, nil)
}

func (c *HttpClient) BranchCreate(id string, parent string, baseCommitTs uint64) (any, error) {
	return c.request("POST", "/v1/branches", map[string]any{
		"id": id, "parent": parent, "base_commit_ts": baseCommitTs,
	})
}

func (c *HttpClient) BranchGet(id string) (any, error) {
	return c.request("GET", "/v1/branches/"+id, nil)
}

func (c *HttpClient) BranchDelete(id string) (any, error) {
	return c.request("DELETE", "/v1/branches/"+id, nil)
}

func (c *HttpClient) BranchList() (any, error) {
	return c.request("GET", "/v1/branches", nil)
}

func (c *HttpClient) BranchReset(id string, baseCommitTs uint64) (any, error) {
	return c.request("POST", "/v1/branches/"+id+"/reset", map[string]any{
		"base_commit_ts": baseCommitTs,
	})
}

func (c *HttpClient) BranchPromote(id string) (any, error) {
	return c.request("POST", "/v1/branches/"+id+"/promote", nil)
}

func (c *HttpClient) BranchDiff(id string, against string) (any, error) {
	return c.request("GET", "/v1/branches/"+id+"/diff?against="+against, nil)
}

func (c *HttpClient) BillingSummary() (any, error) {
	return c.request("GET", "/v1/billing/summary", nil)
}

func (c *HttpClient) VectorAnnSearch(table string, vector []float64, topK int, ef int) (any, error) {
	return c.request("POST", "/v1/vector/ann-search", map[string]any{
		"table": table, "vector": vector, "top_k": topK, "ef": ef,
	})
}

func (c *HttpClient) OidcLogin(redirectURI string, state *string) (any, error) {
	return c.request("POST", "/v1/auth/oidc/login", map[string]any{
		"redirect_uri": redirectURI, "state": state,
	})
}

func (c *HttpClient) OidcToken(idToken string) (any, error) {
	return c.request("POST", "/v1/auth/oidc/token", map[string]any{"id_token": idToken})
}

func (c *HttpClient) MigrateSupabase(dump string) (any, error) {
	return c.request("POST", "/v1/migrate/supabase", map[string]any{"dump": dump})
}

func (c *HttpClient) BackupVerify(backupID string) (any, error) {
	return c.request("GET", "/v1/backups/verify?backup_id="+backupID, nil)
}

func (c *HttpClient) BackupDrill() (any, error) {
	return c.request("GET", "/v1/backups/drill", nil)
}

func (c *HttpClient) BackupCopy(backupID string) (any, error) {
	return c.request("POST", "/v1/backups/copy", map[string]any{"backup_id": backupID})
}

func (c *HttpClient) MigrateNeon(branches string) (any, error) {
	return c.request("POST", "/v1/migrate/neon", map[string]any{"branches": branches})
}

func (c *HttpClient) MigrateApply(id string, sql string, author *string) (any, error) {
	return c.request("POST", "/v1/migrate/apply", map[string]any{"id": id, "sql": sql, "author": author})
}

func (c *HttpClient) MigrateLedger() (any, error) {
	return c.request("GET", "/v1/migrate/ledger", nil)
}

func (c *HttpClient) IndexStats() (any, error) {
	return c.request("GET", "/v1/index/stats", nil)
}

func (c *HttpClient) BillingInvoice(tenant string) (any, error) {
	path := "/v1/billing/invoice"
	if tenant != "" {
		path += "?tenant=" + tenant
	}
	return c.request("GET", path, nil)
}

func (c *HttpClient) Regions() (any, error) {
	return c.request("GET", "/v1/regions", nil)
}

func (c *HttpClient) VectorUpsert(table string, id string, vector []float64) (any, error) {
	return c.request("POST", "/v1/vector/upsert", map[string]any{
		"table": table, "id": id, "vector": vector,
	})
}

func (c *HttpClient) VectorSearch(table string, vector []float64, topK int) (any, error) {
	return c.request("POST", "/v1/vector/search", map[string]any{
		"table": table, "vector": vector, "top_k": topK,
	})
}

func (c *HttpClient) VectorDelete(table string, id string) (any, error) {
	return c.request("DELETE", "/v1/vector/"+table+"/"+id, nil)
}

func (c *HttpClient) TextIndex(table string, id string, text string) (any, error) {
	return c.request("POST", "/v1/text/index", map[string]any{
		"table": table, "id": id, "text": text,
	})
}

func (c *HttpClient) TextSearch(table string, query string, topK int) (any, error) {
	return c.request("POST", "/v1/text/search", map[string]any{
		"table": table, "query": query, "top_k": topK,
	})
}

func (c *HttpClient) TextDelete(table string, id string) (any, error) {
	return c.request("DELETE", "/v1/text/"+table+"/"+id, nil)
}

func (c *HttpClient) Checkpoint(manifestID *string) (any, error) {
	return c.request("POST", "/v1/backups/checkpoint", map[string]any{
		"manifest_id": manifestID,
	})
}

func (c *HttpClient) Snapshot() (any, error) {
	return c.request("POST", "/v1/snapshots", nil)
}

func (c *HttpClient) LatestCheckpoint() (any, error) {
	return c.request("GET", "/v1/backups/latest", nil)
}

func (c *HttpClient) Pitr(target uint64) (any, error) {
	return c.request("GET", fmt.Sprintf("/v1/backups/pitr?target=%d", target), nil)
}

func (c *HttpClient) Restore(target uint64) (any, error) {
	return c.request("POST", fmt.Sprintf("/v1/backups/restore?target=%d", target), nil)
}

func (c *HttpClient) Archive(backupID *string) (any, error) {
	return c.request("POST", "/v1/backups/archive", map[string]any{
		"backup_id": backupID,
	})
}

func (c *HttpClient) Archives() (any, error) {
	return c.request("GET", "/v1/backups/archives", nil)
}

func (c *HttpClient) ShardLayout() (any, error) {
	return c.request("GET", "/v1/shards", nil)
}

func (c *HttpClient) ShardMove(table string, target *int) (any, error) {
	return c.request("POST", "/v1/shards/move", map[string]any{
		"table": table, "target": target,
	})
}

func (c *HttpClient) Ranges(key *string) (any, error) {
	path := "/v1/ranges"
	if key != nil {
		path += "?key=" + url.QueryEscape(*key)
	}
	return c.request("GET", path, nil)
}

func (c *HttpClient) RangeSplit(id string, mid string, leftID string, rightID string, expectedEpoch uint64) (any, error) {
	return c.request("POST", "/v1/ranges/split", map[string]any{
		"id": id, "mid": mid, "left_id": leftID, "right_id": rightID,
		"expected_epoch": expectedEpoch,
	})
}

func (c *HttpClient) RangeMerge(leftID string, rightID string, mergedID string, expectedLeftEpoch uint64, expectedRightEpoch uint64) (any, error) {
	return c.request("POST", "/v1/ranges/merge", map[string]any{
		"left_id": leftID, "right_id": rightID, "merged_id": mergedID,
		"expected_left_epoch": expectedLeftEpoch, "expected_right_epoch": expectedRightEpoch,
	})
}

func (c *HttpClient) RangeAutosplit(minWrites *uint64) (any, error) {
	path := "/v1/ranges/autosplit"
	if minWrites != nil {
		path += "?min_writes=" + strconv.FormatUint(*minWrites, 10)
	}
	return c.request("POST", path, nil)
}

func (c *HttpClient) RangeLoads() (any, error) {
	return c.request("GET", "/v1/ranges/loads", nil)
}

func (c *HttpClient) ClusterMembers() (ClusterStatus, error) {
	var status ClusterStatus
	out, err := c.request("GET", "/v1/cluster/members", nil)
	if err != nil {
		return status, err
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return status, err
	}
	if err := json.Unmarshal(raw, &status); err != nil {
		return status, err
	}
	return status, nil
}

func (c *HttpClient) ClusterAdd(id int, addr string) (any, error) {
	return c.request("POST", "/v1/cluster/members", map[string]any{
		"id": id, "addr": addr,
	})
}

func (c *HttpClient) ClusterRemove(id int) (any, error) {
	return c.request("DELETE", fmt.Sprintf("/v1/cluster/members/%d", id), nil)
}

func (c *HttpClient) ClusterTransfer(target int) (any, error) {
	return c.request("POST", "/v1/cluster/transfer", map[string]any{"target": target})
}

func (c *HttpClient) ClusterReplace(members []ClusterMember) (any, error) {
	return c.request("POST", "/v1/cluster/replace", map[string]any{"members": members})
}

func (c *HttpClient) SqlCopy(table string, rows []map[string]string) (any, error) {
	return c.request("POST", "/v1/sql/copy", map[string]any{"table": table, "rows": rows})
}

func (c *HttpClient) SqlExplain(sql string) (any, error) {
	return c.request("POST", "/v1/sql/explain", map[string]any{"sql": sql})
}

func (c *HttpClient) RestList(table string, query string) (any, error) {
	path := "/rest/v1/" + table
	if query != "" {
		path += "?" + query
	}
	return c.request("GET", path, nil)
}

func (c *HttpClient) RestInsert(table string, row any) (any, error) {
	return c.request("POST", "/rest/v1/"+table, row)
}

func (c *HttpClient) RestDelete(table string, key string) (any, error) {
	return c.request("DELETE", "/rest/v1/"+table+"?key=eq."+key, nil)
}

func (c *HttpClient) Graphql(query string) (any, error) {
	return c.request("POST", "/graphql", map[string]any{"query": query})
}

func (c *HttpClient) Metering() (any, error) {
	return c.request("GET", "/v1/metering", nil)
}

func (c *HttpClient) Autoscale(query string) (any, error) {
	path := "/v1/autoscale"
	if query != "" {
		path += "?" + query
	}
	return c.request("GET", path, nil)
}

func (c *HttpClient) Prometheus() (string, error) {
	return c.requestText("GET", "/metrics/prometheus", "")
}

func (c *HttpClient) Qos() (any, error) {
	return c.request("GET", "/v1/qos", nil)
}

func (c *HttpClient) QosSetTier(tenant string, tier string) (any, error) {
	return c.request("POST", "/v1/qos/tier", map[string]any{"tenant": tenant, "tier": tier})
}

func (c *HttpClient) AuthRegister(id string, password string, tenant *string, roles []string) (any, error) {
	return c.request("POST", "/v1/auth/register", map[string]any{
		"id": id, "password": password, "tenant": tenant, "roles": roles,
	})
}

func (c *HttpClient) AuthVerify(id string, password string) (any, error) {
	return c.request("POST", "/v1/auth/verify", map[string]any{"id": id, "password": password})
}

func (c *HttpClient) AuthToken(id string, password string, code *string) (any, error) {
	return c.request("POST", "/v1/auth/token", map[string]any{"id": id, "password": password, "code": code})
}

func (c *HttpClient) AuthRevoke(key string) (any, error) {
	return c.request("DELETE", "/v1/auth/keys", map[string]any{"key": key})
}

func (c *HttpClient) OtpSetup(id string) (any, error) {
	return c.request("POST", "/v1/auth/otp/setup", map[string]any{"id": id})
}

func (c *HttpClient) OtpVerify(id string, code string) (any, error) {
	return c.request("POST", "/v1/auth/otp/verify", map[string]any{"id": id, "code": code})
}

func (c *HttpClient) PasskeyChallenge(user string) (any, error) {
	return c.request("POST", "/v1/auth/passkey/challenge", map[string]any{"user": user})
}

func (c *HttpClient) PasskeyRegister(user string, credentialID string, publicKey string) (any, error) {
	return c.request("POST", "/v1/auth/passkey/register", map[string]any{
		"user": user, "credential_id": credentialID, "public_key": publicKey,
	})
}

func (c *HttpClient) PasskeyVerify(user string, credentialID string, authenticatorData string, clientDataJSON string, signature string) (any, error) {
	return c.request("POST", "/v1/auth/passkey/verify", map[string]any{
		"user": user, "credential_id": credentialID, "authenticator_data": authenticatorData,
		"client_data_json": clientDataJSON, "signature": signature,
	})
}

func (c *HttpClient) MaskSet(table string, fields []string) (any, error) {
	return c.request("POST", "/v1/auth/mask", map[string]any{"table": table, "fields": fields})
}

func (c *HttpClient) PresenceJoin(channel string, member string, state any, ttlSecs *int) (any, error) {
	return c.request("POST", "/v1/presence/join", map[string]any{
		"channel": channel, "member": member, "state": state, "ttl_secs": ttlSecs,
	})
}

func (c *HttpClient) PresenceLeave(channel string, member string) (any, error) {
	return c.request("POST", "/v1/presence/leave", map[string]any{"channel": channel, "member": member})
}

func (c *HttpClient) PresenceList(channel string) (any, error) {
	return c.request("GET", "/v1/presence/"+channel, nil)
}

func (c *HttpClient) Broadcast(channel string, payload any, from *string) (any, error) {
	return c.request("POST", "/v1/broadcast", map[string]any{
		"channel": channel, "payload": payload, "from": from,
	})
}

func (c *HttpClient) TopicAppend(partition string, key string, value string, retention *int) (any, error) {
	return c.request("POST", "/v1/topics/append", map[string]any{
		"partition": partition, "key": key, "value": value, "retention": retention,
	})
}

func (c *HttpClient) TopicRead(partition string, from int, limit int) (any, error) {
	return c.request("GET", fmt.Sprintf("/v1/topics/read?partition=%s&from=%d&limit=%d", partition, from, limit), nil)
}
