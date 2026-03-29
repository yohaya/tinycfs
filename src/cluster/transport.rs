/// Length-prefixed TCP framing for cluster messages.
///
/// Wire format per message:
///   [4 bytes big-endian length][<length> bytes bincode payload]
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use crate::cluster::message::Message;

/// Maximum allowed message size (16 MiB) as a safety guard.
const MAX_MSG_LEN: u32 = 16 * 1024 * 1024;

/// Encode and send a single message over the stream.
///
/// The 4-byte length header and payload are combined into one buffer before
/// writing.  Two separate write_all() calls would cause the OS to emit two
/// sendto() syscalls, letting TCP deliver them as two separate segments; the
/// receiver then needs two epoll_pwait + recvfrom cycles per message instead
/// of one — a 2× syscall overhead observed in strace on an idle cluster.
pub async fn send_msg(stream: &mut TcpStream, msg: &Message) -> io::Result<()> {
    let payload = bincode::serialize(msg)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let len = (payload.len() as u32).to_be_bytes();
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&len);
    frame.extend_from_slice(&payload);
    stream.write_all(&frame).await
}

/// Receive a single message from the stream.
pub async fn recv_msg(stream: &mut TcpStream) -> io::Result<Message> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_MSG_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("message too large: {} bytes", len),
        ));
    }
    let mut buf = vec![0u8; len as usize];
    stream.read_exact(&mut buf).await?;
    bincode::deserialize(&buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}
