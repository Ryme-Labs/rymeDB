package rymedb

import (
	"net/http/httptest"
	"strings"
	"testing"

	"golang.org/x/net/websocket"
)

func stubSocketServer(t *testing.T, frames []string) (*HttpClient, *string) {
	t.Helper()
	var gotPath string
	server := httptest.NewServer(websocket.Server{
		Handler: func(conn *websocket.Conn) {
			gotPath = conn.Request().URL.RequestURI()
			for _, frame := range frames {
				if err := websocket.Message.Send(conn, frame); err != nil {
					return
				}
			}
		},
	})
	t.Cleanup(server.Close)
	return NewHttpClient(server.URL, "k"), &gotPath
}

func TestSubscribeTableReceivesChangeRecords(t *testing.T) {
	client, gotPath := stubSocketServer(t, []string{
		`{"tenant":"t","database":"d","branch":"main","table":"docs","op":"INSERT","pk":[49],"after":[50],"commit_ts":7,"sequence":1}`,
	})
	sub, err := client.SubscribeTable("docs")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	var record ChangeRecord
	if err := sub.Next(&record); err != nil {
		t.Fatal(err)
	}
	if record.Op != "INSERT" || record.CommitTs != 7 || record.Table != "docs" {
		t.Fatalf("unexpected record: %+v", record)
	}
	if len(record.Pk) != 1 || record.Pk[0] != 49 {
		t.Fatalf("unexpected pk: %+v", record)
	}
	if !strings.HasPrefix(*gotPath, "/v1/stream?") {
		t.Fatalf("unexpected path: %s", *gotPath)
	}
	for _, want := range []string{"table=docs", "api_key=k"} {
		if !strings.Contains(*gotPath, want) {
			t.Fatalf("missing %s in path: %s", want, *gotPath)
		}
	}
}

func TestSubscribeBroadcastReceivesMessages(t *testing.T) {
	client, gotPath := stubSocketServer(t, []string{
		`{"channel":"lobby","from":"ada","payload":{"hello":true},"commit_ts":3,"sequence":1}`,
	})
	sub, err := client.SubscribeBroadcast("lobby")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	var record BroadcastMessage
	if err := sub.Next(&record); err != nil {
		t.Fatal(err)
	}
	if record.Channel != "lobby" || record.CommitTs != 3 {
		t.Fatalf("unexpected record: %+v", record)
	}
	if !strings.HasPrefix(*gotPath, "/v1/broadcast/lobby") {
		t.Fatalf("unexpected path: %s", *gotPath)
	}
}

func TestSubscribeTableFromSendsWatermark(t *testing.T) {
	client, gotPath := stubSocketServer(t, []string{})
	from := uint64(7)
	sub, err := client.SubscribeTableFrom("docs", &from)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	for _, want := range []string{"table=docs", "from=7", "api_key=k"} {
		if !strings.Contains(*gotPath, want) {
			t.Fatalf("missing %s in path: %s", want, *gotPath)
		}
	}
}

func TestSubscribeTableFromSequenceSendsExactCursor(t *testing.T) {
	client, gotPath := stubSocketServer(t, []string{})
	sequence := uint64(11)
	sub, err := client.SubscribeTableFromSequence("docs", &sequence)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	for _, want := range []string{"table=docs", "from_sequence=11", "api_key=k"} {
		if !strings.Contains(*gotPath, want) {
			t.Fatalf("missing %s in path: %s", want, *gotPath)
		}
	}
}

func TestSubscribeQueryReceivesSnapshotThenUpdate(t *testing.T) {
	client, gotPath := stubSocketServer(t, []string{
		`{"type":"snapshot","commit":7,"rows":[{"pk":"k1","value":"one"}]}`,
		`{"type":"update","commit":8,"rows":[{"pk":"k2","value":"two"}],"truncated":false}`,
	})
	limit := 100
	sub, err := client.SubscribeQuery("docs", &limit)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	var snapshot, update QueryMessage
	if err := sub.Next(&snapshot); err != nil {
		t.Fatal(err)
	}
	if err := sub.Next(&update); err != nil {
		t.Fatal(err)
	}
	if snapshot.Type != "snapshot" || snapshot.Commit != 7 {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
	if len(snapshot.Rows) != 1 || snapshot.Rows[0].Pk != "k1" {
		t.Fatalf("unexpected rows: %+v", snapshot.Rows)
	}
	if update.Type != "update" || update.Commit != 8 || update.Truncated {
		t.Fatalf("unexpected update: %+v", update)
	}
	if !strings.HasPrefix(*gotPath, "/v1/query-stream?") {
		t.Fatalf("unexpected path: %s", *gotPath)
	}
	for _, want := range []string{"table=docs", "limit=100", "api_key=k"} {
		if !strings.Contains(*gotPath, want) {
			t.Fatalf("missing %s in path: %s", want, *gotPath)
		}
	}
}
