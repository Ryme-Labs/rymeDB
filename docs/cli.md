---
title: Command-line tool
sidebar_label: CLI
---

# rymeDB CLI

Install the public npm package:

```sh
npm install --global @rymelabs/rymedb-cli
```

Set the server URL and API key, then check connectivity:

```sh
export RYME_BASE=http://127.0.0.1:8080
export RYME_API_KEY=your-api-key
rymedb-cli ready
rymedb-cli --help
```

The CLI can inspect and operate SQL, backups, branches, cluster placement,
realtime streams, auth, and other server APIs. See the [REST API reference](https://db.rymelabs.dev/api/)
for the server endpoints it calls.

To manage named credentials, authenticate with a session key and create a
role-scoped key. For browser clients, use `anon` only after configuring the
intended SQL grants and row-level security:

```sh
rymedb-cli auth token <user-id> <password>
# set RYME_API_KEY to the returned session key before continuing
export RYME_API_KEY=<session-key>
rymedb-cli auth keys create browser-app --role anon
rymedb-cli auth keys list
```

The generated secret is shown once and expires after 90 days. See [Auth and
security](./auth.md#named-api-keys) for role delegation and public-key safety.
