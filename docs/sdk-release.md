---
title: SDKs and client libraries
sidebar_label: SDKs
---

# SDKs and client libraries

Use an SDK when you want typed helpers for HTTP, SQL requests, key/value
operations, and realtime subscriptions. Use PostgreSQL or RESP clients when
you want to keep an existing driver and protocol in your application.

## JavaScript and TypeScript

Install `@rymelabs/rymedb`:

```sh
npm install @rymelabs/rymedb
```

Create a `RymeHttpClient` with the server URL and API key, then use its
`kvPut`, `kvGet`, `sql`, and `ready` methods. Realtime helpers include
`subscribeTable`, `subscribeQuery`, `subscribeBroadcast`, and presence APIs.
See the [JavaScript SDK guide](sdk-js.md) for examples and options.

## Rust

Add `rymedb-client` and a Tokio runtime to your application. `RymeClient`
provides async HTTP operations and WebSocket subscriptions. See the
[Rust SDK guide](sdk-rust.md) for a complete quickstart.

SDK releases use `sdk-v<major>.0.0` GitHub tags. Database releases use
`rymedb-v<major>.<minor>.<patch>` tags, so the two release streams have
independent version sequences and cannot collide with the legacy SDK tags.

## Direct protocol clients

Connect PostgreSQL drivers to port `5433` and RESP/Redis-compatible clients to
port `6380` by default. HTTP and WebSocket clients use port `8080`. The
[PostgreSQL](postgres.md), [RESP](resp.md), and [REST API](api.md) guides
describe supported behavior and compatibility boundaries.
