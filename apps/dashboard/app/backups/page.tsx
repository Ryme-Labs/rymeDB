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
  QueryError,
  Row,
  SectionHeader,
} from "@/components/studio/ui";
import {
  archiveBackup,
  checkpoint,
  latestBackup,
  listArchives,
  scheduledDrill,
  takeSnapshot,
  verifyBackup,
} from "@/lib/api";
import { useConnection } from "@/lib/connection";

export default function BackupsPage() {
  const conn = useConnection();
  const [backupId, setBackupId] = useState("");
  const [out, setOut] = useState<unknown>(null);
  const [err, setErr] = useState<unknown>(null);
  const [pending, setPending] = useState(false);

  async function act(fn: () => Promise<unknown>) {
    setPending(true);
    setErr(null);
    try {
      setOut(await fn());
    } catch (e) {
      setErr(e);
    } finally {
      setPending(false);
    }
  }

  return (
    <div>
      <SectionHeader
        title="Backups"
        description="Checkpoints, snapshots, off-site archives and restore drills for this database."
      />
      <Card>
        <CardHeader
          title="Backup operations"
          description="Checkpoint · latest · snapshots · archive · verify · drill"
        />
        <CardContent>
          <Row>
              <Button variant="primary" disabled={pending} onClick={() => void act(() => checkpoint(conn))}>
              Checkpoint
            </Button>
            <Button disabled={pending} onClick={() => void act(() => latestBackup(conn))}>
              Latest
            </Button>
            <Button disabled={pending} onClick={() => void act(() => takeSnapshot(conn))}>
              Snapshot
            </Button>
            <Button disabled={pending} onClick={() => void act(() => listArchives(conn))}>
              List archives
            </Button>
          </Row>
          <Row className="mt-2">
            <Field label="Backup id" className="min-w-44">
              <Input
                value={backupId}
                onChange={(e) => setBackupId(e.target.value)}
                placeholder="backup id"
                className="mono"
              />
            </Field>
            <Button disabled={pending} onClick={() => void act(() => archiveBackup(conn, backupId || null))}>
              Archive
            </Button>
            <Button
              disabled={pending || !backupId}
              onClick={() => void act(() => verifyBackup(conn, backupId))}
            >
              Verify drill
            </Button>
            <Button disabled={pending} onClick={() => void act(() => scheduledDrill(conn))}>
              Scheduled drill
            </Button>
          </Row>
          <div className="mt-3 grid gap-2">
            <QueryError error={err} />
            {out !== null && !err ? <CodeBlock data={out} /> : null}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
