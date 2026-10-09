use ryme_sql::{parse, Executor};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn read_frame(socket: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    let tag =
        tokio::time::timeout(Duration::from_secs(5), socket.read_u8()).await.unwrap().unwrap();
    let mut length_buffer = [0u8; 4];
    socket.read_exact(&mut length_buffer).await.unwrap();
    let length = u32::from_be_bytes(length_buffer) as usize;
    let mut payload = vec![0u8; length - 4];
    socket.read_exact(&mut payload).await.unwrap();
    (tag, payload)
}

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + body.len());
    out.push(tag);
    out.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn cstring(value: &str) -> Vec<u8> {
    let mut out = value.as_bytes().to_vec();
    out.push(0);
    out
}

#[tokio::test]
async fn extended_query_flow() {
    let executor = Executor::new(String::from("t"), String::from("d"));
    executor.execute(parse("INSERT INTO t KEY '1' VALUE 'ada'").unwrap()).await.unwrap();
    let gateway = ryme_wire_pg::PgGateway::with_executor(executor);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut startup = Vec::new();
    startup.extend_from_slice(b"user\0u\0\0");
    let mut startup_frame = (startup.len() + 8) as u32;
    let mut hello = startup_frame.to_be_bytes().to_vec();
    startup_frame = 196608;
    hello.extend_from_slice(&startup_frame.to_be_bytes());
    hello.extend_from_slice(&startup);
    socket.write_all(&hello).await.unwrap();
    assert_eq!(read_frame(&mut socket).await.0, b'R');
    loop {
        let (tag, _) = read_frame(&mut socket).await;
        if tag == b'Z' {
            break;
        }
        assert!(tag == b'S' || tag == b'K');
    }
    let mut parse_body = cstring("stmt1");
    parse_body.extend(cstring("SELECT * FROM t KEY $1"));
    parse_body.extend_from_slice(&0i16.to_be_bytes());
    socket.write_all(&frame(b'P', &parse_body)).await.unwrap();
    assert_eq!(read_frame(&mut socket).await.0, b'1');
    let mut statement_describe = vec![b'S'];
    statement_describe.extend(cstring("stmt1"));
    socket.write_all(&frame(b'D', &statement_describe)).await.unwrap();
    assert_eq!(read_frame(&mut socket).await.0, b't');
    assert_eq!(read_frame(&mut socket).await.0, b'T');
    let mut bind_body = cstring("portal1");
    bind_body.extend(cstring("stmt1"));
    bind_body.extend_from_slice(&0i16.to_be_bytes());
    bind_body.extend_from_slice(&1i16.to_be_bytes());
    bind_body.extend_from_slice(&1i32.to_be_bytes());
    bind_body.extend_from_slice(b"1");
    bind_body.extend_from_slice(&0i16.to_be_bytes());
    socket.write_all(&frame(b'B', &bind_body)).await.unwrap();
    assert_eq!(read_frame(&mut socket).await.0, b'2');
    let mut describe_body = vec![b'P'];
    describe_body.extend(cstring("portal1"));
    socket.write_all(&frame(b'D', &describe_body)).await.unwrap();
    assert_eq!(read_frame(&mut socket).await.0, b'T');
    let mut execute_body = cstring("portal1");
    execute_body.extend_from_slice(&0i32.to_be_bytes());
    socket.write_all(&frame(b'E', &execute_body)).await.unwrap();
    let mut saw_data = false;
    for _ in 0..4 {
        let (tag, payload) = read_frame(&mut socket).await;
        if tag == b'D' && payload.windows(3).any(|w| w == b"ada") {
            saw_data = true;
        }
        if tag == b'C' {
            break;
        }
    }
    assert!(saw_data);
    socket.write_all(&frame(b'S', b"")).await.unwrap();
    assert_eq!(read_frame(&mut socket).await.0, b'Z');
}
