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
pub async fn send_msg(stream: &mut TcpStream, msg: &Message) -> io::Result<()> {
    let payload = bincode::serialize(msg)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let len = payload.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(&payload).await?;
    Ok(())
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
