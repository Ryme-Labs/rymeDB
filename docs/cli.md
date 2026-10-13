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
