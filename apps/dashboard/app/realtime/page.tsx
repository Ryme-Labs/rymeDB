"use client";

import { useState } from "react";
import {
  Button,
  Card,
  CardContent,
  CardHeader,
  CodeBlock,
  Field,
  Input,
  Pill,
  QueryError,
  Row,
  SectionHeader,
} from "@/components/studio/ui";
import { broadcastPost, presenceJoin, presenceList, topicAppend, topicRead, wsUrl } from "@/lib/api";
import { useConnection } from "@/lib/connection";
import { useFeed } from "@/lib/use-feed";

function Feed({ lines, live, empty }: { lines: string[]; live: boolean; empty: string }) {
  return (
    <div className="relative">
      <pre className="mono max-h-64 min-h-24 overflow-auto whitespace-pre-wrap rounded-md border border-border-muted bg-surface-100 p-3 text-xs leading-relaxed text-foreground-light">
        {lines.length === 0 ? empty : lines.join("\n")}
      </pre>
      {live ? (
        <span className="absolute right-2 top-2">
          <Pill tone="ok">live</Pill>
        </span>
      ) : null}
    </div>
  );
}

export default function RealtimePage() {
  const conn = useConnection();
  const [streamTable, setStreamTable] = useState("docs");
  const [partition, setPartition] = useState("orders");
  const [topicKey, setTopicKey] = useState("k1");
  const [topicVal, setTopicVal] = useState("v1");
  const [channel, setChannel] = useState("lobby");
  const [payload, setPayload] = useState('{"hello":true}');
  const [presenceChannel, setPresenceChannel] = useState("room:1");
  const [member, setMember] = useState("ada");
  const [out, setOut] = useState<unknown>(null);
  const [err, setErr] = useState<unknown>(null);

  const stream = useFeed(null);
  const cast = useFeed(null);

  async function act(fn: () => Promise<unknown>) {
    setErr(null);
    try {
      setOut(await fn());
    } catch (e) {
      setErr(e);
    }
  }

  return (
    <div>
      <SectionHeader
        title="Realtime"
        description="Live change streams, durable topics, broadcast channels and presence — Studio's Realtime inspector for rymeDB."
      />
      <div className="grid gap-3 lg:grid-cols-2">
        <Card>
          <CardHeader title="Change stream" description="WS /v1/stream?table=" actions={stream.live ? <Pill tone="ok">live</Pill> : <Pill tone="muted">idle</Pill>} />
          <CardContent>
            <Row>
              <Field label="Table">
                <Input value={streamTable} onChange={(e) => setStreamTable(e.target.value)} className="w-36" />
              </Field>
              <Button
                variant="primary"
                onClick={() =>
                  stream.start(wsUrl(conn, `/v1/stream?table=${encodeURIComponent(streamTable)}`))
                }
              >
                Watch
              </Button>
              <Button onClick={stream.stop}>Stop</Button>
            </Row>
            <div className="mt-3">
              <Feed lines={stream.lines} live={stream.live} empty="Press Watch to tail live changes" />
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader title="Broadcast" description="POST /v1/broadcast · WS /v1/broadcast/{channel}" actions={cast.live ? <Pill tone="ok">live</Pill> : <Pill tone="muted">idle</Pill>} />
          <CardContent>
            <Row>
              <Field label="Channel">
                <Input value={channel} onChange={(e) => setChannel(e.target.value)} className="w-32" />
              </Field>
              <Field label="Payload JSON" className="min-w-40 flex-1">
                <Input value={payload} onChange={(e) => setPayload(e.target.value)} className="mono" spellCheck={false} />
              </Field>
            </Row>
            <Row className="mt-2">
              <Button
                variant="primary"
                onClick={() => {
                  let parsed: unknown = payload;
                  try {
                    parsed = JSON.parse(payload);
                  } catch {
                    /* send raw */
                  }
                  void act(() => broadcastPost(conn, channel, parsed));
                }}
              >
                Post
              </Button>
              <Button onClick={() => cast.start(wsUrl(conn, `/v1/broadcast/${encodeURIComponent(channel)}`))}>
                Watch
              </Button>
              <Button onClick={cast.stop}>Stop</Button>
            </Row>
            <div className="mt-3">
              <Feed lines={cast.lines} live={cast.live} empty="Broadcast frames will appear here" />
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader title="Topics" description="Durable partitions via /v1/topics" />
          <CardContent>
            <Row>
              <Field label="Partition">
                <Input value={partition} onChange={(e) => setPartition(e.target.value)} className="w-32" />
              </Field>
              <Field label="Key">
                <Input value={topicKey} onChange={(e) => setTopicKey(e.target.value)} className="w-24" />
              </Field>
              <Field label="Value">
                <Input value={topicVal} onChange={(e) => setTopicVal(e.target.value)} className="w-32" />
              </Field>
            </Row>
            <Row className="mt-2">
              <Button variant="primary" onClick={() => void act(() => topicAppend(conn, partition, topicKey, topicVal))}>
                Append
              </Button>
              <Button onClick={() => void act(() => topicRead(conn, partition))}>Read</Button>
            </Row>
          </CardContent>
        </Card>

        <Card>
          <CardHeader title="Presence" description="Join and list channel members" />
          <CardContent>
            <Row>
              <Field label="Channel">
                <Input value={presenceChannel} onChange={(e) => setPresenceChannel(e.target.value)} className="w-32" />
              </Field>
              <Field label="Member">
                <Input value={member} onChange={(e) => setMember(e.target.value)} className="w-28" />
              </Field>
            </Row>
            <Row className="mt-2">
              <Button variant="primary" onClick={() => void act(() => presenceJoin(conn, presenceChannel, member))}>
                Join
              </Button>
              <Button onClick={() => void act(() => presenceList(conn, presenceChannel))}>List</Button>
            </Row>
          </CardContent>
        </Card>
      </div>

      <div className="mt-3 grid gap-2">
        <QueryError error={err ?? stream.error ?? cast.error} />
        {out !== null && !err ? (
          <Card>
            <CardHeader title="Result" />
            <CardContent>
              <CodeBlock data={out} />
            </CardContent>
          </Card>
        ) : null}
      </div>
    </div>
  );
}
