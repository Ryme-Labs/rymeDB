use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Peer {
    stream: tokio::net::TcpStream,
    buffer: Vec<u8>,
}

impl Peer {
    async fn connect(addr: &str) -> Self {
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        Self { stream, buffer: Vec::new() }
    }

    async fn command<S: AsRef<str>>(&mut self, parts: &[S]) -> Vec<u8> {
        let mut frame = format!("*{}\r\n", parts.len());
        for part in parts {
            let part = part.as_ref();
            frame.push_str(&format!("${}\r\n{part}\r\n", part.len()));
        }
        self.stream.write_all(frame.as_bytes()).await.unwrap();
        loop {
            if let Some(consumed) = frame_len(&self.buffer) {
                return self.buffer.drain(..consumed).collect();
            }
            let mut chunk = vec![0u8; 8192];
            let read = self.stream.read(&mut chunk).await.unwrap();
            assert!(read > 0, "connection closed");
            self.buffer.extend_from_slice(&chunk[..read]);
        }
    }
}

fn frame_len(input: &[u8]) -> Option<usize> {
    if input.is_empty() {
        return None;
    }
    match input[0] {
        b'+' | b'-' | b':' => line_len(input),
        b'$' => {
            let header = line_len(input)?;
            let len: i64 = std::str::from_utf8(&input[1..header - 2]).ok()?.parse().ok()?;
            if len < 0 {
                return Some(header);
            }
            let total = header + len as usize + 2;
            if input.len() < total {
                return None;
            }
            Some(total)
        }
        b'*' => {
            let header = line_len(input)?;
            let count: i64 = std::str::from_utf8(&input[1..header - 2]).ok()?.parse().ok()?;
            if count < 0 {
                return Some(header);
            }
            let mut offset = header;
            for _ in 0..count {
                offset += frame_len(&input[offset..])?;
            }
            Some(offset)
        }
        _ => None,
    }
}

fn line_len(input: &[u8]) -> Option<usize> {
    input.windows(2).position(|w| w == b"\r\n").map(|index| index + 2)
}

fn is_error(reply: &[u8]) -> bool {
    reply.first() == Some(&b'-')
}

fn normalize(reply: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(reply.len());
    let mut index = 0;
    while index < reply.len() {
        match reply[index] {
            b'$' => {
                let start = index;
                let end = index
                    + reply[index..]
                        .windows(2)
                        .position(|w| w == b"\r\n")
                        .map(|position| position + 2)
                        .unwrap_or(reply.len() - index);
                if &reply[start..end] == b"$-1\r\n" {
                    out.extend_from_slice(b"$-1\r\n");
                    index = end;
                    continue;
                }
                let length: usize = std::str::from_utf8(&reply[start + 1..end - 2])
                    .ok()
                    .and_then(|text| text.parse().ok())
                    .unwrap_or(0);
                let total = end + length + 2;
                if total > reply.len() {
                    out.extend_from_slice(&reply[index..]);
                    break;
                }
                let payload = &reply[end..end + length];
                if let Ok(text) = std::str::from_utf8(payload) {
                    if let Ok(value) = text.parse::<f64>() {
                        if value.is_finite() {
                            let normal = format!("{value:.6}");
                            let framed = format!("${}\r\n{normal}\r\n", normal.len());
                            out.extend_from_slice(framed.as_bytes());
                            index = total;
                            continue;
                        }
                    }
                }
                out.extend_from_slice(&reply[index..total]);
                index = total;
            }
            _ => {
                let end = index
                    + reply[index..]
                        .windows(2)
                        .position(|w| w == b"\r\n")
                        .map(|position| position + 2)
                        .unwrap_or(reply.len() - index);
                out.extend_from_slice(&reply[index..end]);
                index = end;
            }
        }
    }
    out
}

fn split_top(reply: &[u8]) -> Vec<Vec<u8>> {
    if reply.first() != Some(&b'*') {
        return vec![reply.to_vec()];
    }
    let header = line_len(reply).unwrap_or(reply.len());
    let count: i64 =
        std::str::from_utf8(&reply[1..header - 2]).ok().and_then(|t| t.parse().ok()).unwrap_or(-1);
    if count < 0 {
        return vec![reply.to_vec()];
    }
    let mut out = Vec::new();
    let mut offset = header;
    for _ in 0..count {
        match frame_len(&reply[offset..]) {
            Some(len) => {
                out.push(reply[offset..offset + len].to_vec());
                offset += len;
            }
            None => break,
        }
    }
    out.sort();
    out
}

fn is_unordered(name: &str) -> bool {
    matches!(name, "HGETALL" | "HKEYS" | "HVALS" | "SMEMBERS")
}

fn matrix(prefix: &str) -> Vec<Vec<String>> {
    let key = |name: &str| format!("{prefix}:{name}");
    let s = key("s");
    let n = key("n");
    let h = key("h");
    let l = key("l");
    let t = key("t");
    let z = key("z");
    let st = key("st");
    let g = key("g");
    let hll = key("hll");
    let hllm = key("hllm");
    let bl = key("bl");
    let dst = key("dst");
    let ghost = key("ghost");
    let cmd = |parts: &[&str]| parts.iter().map(|p| p.to_string()).collect::<Vec<String>>();
    vec![
        cmd(&["DEL", &s, &n, &h, &l, &t, &z, &st, &g, &hll, &bl, &dst]),
        cmd(&["PING"]),
        cmd(&["SET", &s, "abc"]),
        cmd(&["GET", &s]),
        cmd(&["APPEND", &s, "def"]),
        cmd(&["STRLEN", &s]),
        cmd(&["GETDEL", &s]),
        cmd(&["GET", &s]),
        cmd(&["MSET", &s, "x", &n, "41"]),
        cmd(&["MGET", &s, &n, &ghost]),
        cmd(&["INCR", &n]),
        cmd(&["INCRBY", &n, "8"]),
        cmd(&["DECRBY", &n, "50"]),
        cmd(&["DECR", &n]),
        cmd(&["INCRBYFLOAT", &n, "0.5"]),
        cmd(&["INCR", &s]),
        cmd(&["EXISTS", &s, &ghost]),
        cmd(&["TYPE", &s]),
        cmd(&["TYPE", &ghost]),
        cmd(&["HSET", &h, "f1", "v1", "f2", "v2"]),
        cmd(&["HSET", &h, "f1", "v1b"]),
        cmd(&["HGET", &h, "f1"]),
        cmd(&["HGET", &h, "ghost"]),
        cmd(&["HEXISTS", &h, "f2"]),
        cmd(&["HLEN", &h]),
        cmd(&["HGETALL", &h]),
        cmd(&["HKEYS", &h]),
        cmd(&["HVALS", &h]),
        cmd(&["HDEL", &h, "f1", "ghost"]),
        cmd(&["RPUSH", &l, "a", "b", "c"]),
        cmd(&["LPUSH", &l, "z"]),
        cmd(&["LLEN", &l]),
        cmd(&["LRANGE", &l, "0", "-1"]),
        cmd(&["LRANGE", &l, "1", "2"]),
        cmd(&["LPOP", &l]),
        cmd(&["RPOP", &l]),
        cmd(&["SADD", &t, "b", "a", "a"]),
        cmd(&["SCARD", &t]),
        cmd(&["SISMEMBER", &t, "a"]),
        cmd(&["SMEMBERS", &t]),
        cmd(&["SREM", &t, "a", "ghost"]),
        cmd(&["ZADD", &z, "1", "a", "2", "b", "1.5", "c"]),
        cmd(&["ZADD", &z, "5", "a"]),
        cmd(&["ZSCORE", &z, "a"]),
        cmd(&["ZSCORE", &z, "ghost"]),
        cmd(&["ZCARD", &z]),
        cmd(&["ZRANK", &z, "a"]),
        cmd(&["ZREVRANK", &z, "a"]),
        cmd(&["ZRANGE", &z, "0", "-1"]),
        cmd(&["ZRANGE", &z, "0", "1", "WITHSCORES"]),
        cmd(&["ZRANGE", &z, "0", "-1", "REV"]),
        cmd(&["ZCOUNT", &z, "1.5", "5"]),
        cmd(&["ZCOUNT", &z, "(1.5", "5"]),
        cmd(&["ZCOUNT", &z, "-inf", "+inf"]),
        cmd(&["ZINCRBY", &z, "1", "c"]),
        cmd(&["ZADD", &z, "NX", "9", "c", "0", "new"]),
        cmd(&["ZREM", &z, "a", "ghost"]),
        cmd(&["XADD", &st, "100-1", "f", "a"]),
        cmd(&["XADD", &st, "100-2", "f", "b"]),
        cmd(&["XLEN", &st]),
        cmd(&["XRANGE", &st, "-", "+"]),
        cmd(&["XREVRANGE", &st, "+", "-", "COUNT", "1"]),
        cmd(&["XREAD", "STREAMS", &st, "100-1"]),
        cmd(&["XREAD", "STREAMS", &st, "$"]),
        cmd(&["XDEL", &st, "100-1", "ghost"]),
        cmd(&["XTRIM", &st, "MAXLEN", "10"]),
        cmd(&["XGROUP", "CREATE", &st, "grp", "0-0"]),
        cmd(&["XREADGROUP", "GROUP", "grp", "c1", "STREAMS", &st, ">"]),
        cmd(&["XPENDING", &st, "grp"]),
        cmd(&["XACK", &st, "grp", "100-2"]),
        cmd(&["XPENDING", &st, "grp"]),
        cmd(&["XAUTOCLAIM", &st, "grp", "c2", "0", "0-0"]),
        cmd(&["PFADD", &hll, "m1", "m2", "m3"]),
        cmd(&["PFADD", &hll, "m1"]),
        cmd(&["PFCOUNT", &hll]),
        cmd(&["PFMERGE", &hllm, &hll]),
        cmd(&["PFCOUNT", &hllm]),
        cmd(&[
            "GEOADD",
            &g,
            "13.361389",
            "38.115556",
            "Palermo",
            "15.087269",
            "37.502669",
            "Catania",
        ]),
        cmd(&["GEODIST", &g, "Palermo", "Catania"]),
        cmd(&["GEODIST", &g, "Palermo", "Catania", "km"]),
        cmd(&["GEODIST", &g, "Palermo", "ghost"]),
        cmd(&["GEOPOS", &g, "Palermo", "ghost"]),
        cmd(&["GEOHASH", &g, "Palermo", "ghost"]),
        cmd(&["GEOSEARCH", &g, "FROMMEMBER", "Palermo", "BYRADIUS", "200", "km"]),
        cmd(&["GEOSEARCH", &g, "FROMMEMBER", "Palermo", "BYRADIUS", "200", "km", "WITHDIST"]),
        cmd(&["GEORADIUS", &g, "15", "37", "200", "km"]),
        cmd(&["GEORADIUS", &g, "15", "37", "200", "km", "WITHHASH"]),
        cmd(&["GEORADIUSBYMEMBER", &g, "Palermo", "200", "km"]),
        cmd(&["MULTI"]),
        cmd(&["SET", &s, "tx"]),
        cmd(&["INCRBY", &n, "2"]),
        cmd(&["EXEC"]),
        cmd(&["GET", &s]),
        cmd(&["DISCARD"]),
        cmd(&["BLPOP", &bl, "0.1"]),
        cmd(&["RPUSH", &bl, "v"]),
        cmd(&["BLPOP", &bl, "1"]),
        cmd(&["BRPOP", &bl, "1"]),
        cmd(&["RPUSH", &bl, "a", "b"]),
        cmd(&["BLMOVE", &bl, &dst, "LEFT", "RIGHT", "1"]),
        cmd(&["LRANGE", &dst, "0", "-1"]),
        cmd(&["XREAD", "BLOCK", "10", "STREAMS", &st, "$"]),
        cmd(&["DEL", &s, &n, &h, &l, &t, &z, &st, &g, &hll, &hllm, &bl, &dst]),
    ]
}

async fn serve() -> String {
    let gateway = ryme_wire_resp::RespGateway::new(String::from("t"), String::from("d"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    addr.to_string()
}

#[tokio::test]
async fn resp_parity_with_redis() {
    let reference_addr = match std::env::var("RYME_REDIS_ADDR") {
        Ok(addr) => addr,
        Err(_) => return,
    };
    let prefix = format!("p{}", std::process::id());
    let mut reference = Peer::connect(&reference_addr).await;
    let mut candidate = Peer::connect(&serve().await).await;
    for (index, command) in matrix(&prefix).iter().enumerate() {
        let expected = reference.command(command).await;
        let actual = candidate.command(command).await;
        if command[0] == "PFCOUNT" {
            let want: u64 = std::str::from_utf8(&expected[1..])
                .unwrap_or(":0")
                .split("\r\n")
                .next()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0);
            let got: u64 = std::str::from_utf8(&actual[1..])
                .unwrap_or(":0")
                .split("\r\n")
                .next()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0);
            let ratio = (got as f64) / (want.max(1) as f64);
            assert!(
                (0.9..=1.1).contains(&ratio),
                "command {index} {command:?}: redis={want} ryme={got}"
            );
            continue;
        }
        if is_error(&expected) || is_error(&actual) {
            assert!(
                is_error(&expected) && is_error(&actual),
                "command {index} {command:?}: redis={expected:?} ryme={actual:?}"
            );
            continue;
        }
        if is_unordered(&command[0]) {
            assert_eq!(
                split_top(&normalize(&expected)),
                split_top(&normalize(&actual)),
                "command {index} {command:?}"
            );
            continue;
        }
        assert_eq!(normalize(&expected), normalize(&actual), "command {index} {command:?}");
    }
}

#[tokio::test]
async fn scan_parity_with_redis() {
    let reference_addr = match std::env::var("RYME_REDIS_ADDR") {
        Ok(addr) => addr,
        Err(_) => return,
    };
    let mut reference = Peer::connect(&reference_addr).await;
    let mut candidate = Peer::connect(&serve().await).await;
    let prefix = format!("q{}", std::process::id());
    let keys: Vec<String> = ["a", "b", "c"].iter().map(|k| format!("{prefix}:{k}")).collect();
    let match_pat = format!("{prefix}:*");
    for peer in [&mut reference, &mut candidate] {
        peer.command(&["DEL".to_string(), keys[0].clone(), keys[1].clone(), keys[2].clone()]).await;
        peer.command(&[
            "MSET".to_string(),
            keys[0].clone(),
            "1".to_string(),
            keys[1].clone(),
            "2".to_string(),
            keys[2].clone(),
            "3".to_string(),
        ])
        .await;
    }
    let mut seen_reference = std::collections::BTreeSet::new();
    let mut cursor = String::from("0");
    loop {
        let reply = reference
            .command(&[
                "SCAN".to_string(),
                cursor.clone(),
                "MATCH".to_string(),
                match_pat.clone(),
                "COUNT".to_string(),
                "2".to_string(),
            ])
            .await;
        let text = String::from_utf8(reply).unwrap();
        let mut lines = text.split("\r\n");
        lines.next();
        let cursor_len: usize = lines.next().unwrap_or("$0")[1..].parse().unwrap_or(0);
        let next: &str = lines.next().unwrap_or("0");
        assert_eq!(next.len(), cursor_len);
        let count: usize = lines.next().unwrap_or("*0")[1..].parse().unwrap_or(0);
        for _ in 0..count {
            let len: usize = lines.next().unwrap_or("$0")[1..].parse().unwrap_or(0);
            seen_reference.insert(lines.next().unwrap_or("").to_string());
            assert!(len > 0);
        }
        cursor = next.to_string();
        if cursor == "0" {
            break;
        }
    }
    let mut seen_candidate = std::collections::BTreeSet::new();
    let mut cursor = String::from("0");
    loop {
        let reply = candidate
            .command(&[
                "SCAN".to_string(),
                cursor.clone(),
                "MATCH".to_string(),
                match_pat.clone(),
                "COUNT".to_string(),
                "2".to_string(),
            ])
            .await;
        let text = String::from_utf8(reply).unwrap();
        let mut lines = text.split("\r\n");
        lines.next();
        let cursor_len: usize = lines.next().unwrap_or("$0")[1..].parse().unwrap_or(0);
        let next: &str = lines.next().unwrap_or("0");
        assert_eq!(next.len(), cursor_len);
        let count: usize = lines.next().unwrap_or("*0")[1..].parse().unwrap_or(0);
        for _ in 0..count {
            let len: usize = lines.next().unwrap_or("$0")[1..].parse().unwrap_or(0);
            seen_candidate.insert(lines.next().unwrap_or("").to_string());
            assert!(len > 0);
        }
        cursor = next.to_string();
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(seen_reference, seen_candidate);
    assert!(seen_candidate.contains(&keys[0]));
}
