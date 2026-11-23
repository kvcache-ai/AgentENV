//! A bounded QMP session: negotiate capabilities and match replies by ID.
use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::path::Path;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::time::{timeout, Duration};

pub(super) async fn execute(path: &Path, command: &str) -> Result<Value> {
    timeout(Duration::from_secs(5), async {
        let mut io = BufReader::new(UnixStream::connect(path).await?);
        let greeting = read_message(&mut io).await?;
        anyhow::ensure!(greeting.get("QMP").is_some(), "invalid QMP greeting");
        let mut result = Value::Null;
        for (id, name) in ["qmp_capabilities", command].iter().enumerate() {
            let mut request = serde_json::to_vec(&json!({"execute": name, "id": id}))?;
            request.push(b'\n');
            io.write_all(&request).await?;
            loop {
                let message = read_message(&mut io).await?;
                if message.get("id") != Some(&json!(id)) {
                    continue;
                }
                if let Some(error) = message.get("error") {
                    bail!("QMP {name}: {error}");
                }
                result = message
                    .get("return")
                    .context("QMP reply has no return value")?
                    .clone();
                break;
            }
        }
        Ok(result)
    })
    .await
    .context("QMP request timed out")?
}

async fn read_message(io: &mut BufReader<UnixStream>) -> Result<Value> {
    let mut bytes = Vec::new();
    loop {
        let available = io.fill_buf().await?;
        anyhow::ensure!(!available.is_empty(), "QMP disconnected");
        let end = available.iter().position(|b| *b == b'\n');
        let count = end.map_or(available.len(), |i| i + 1);
        anyhow::ensure!(bytes.len() + count <= 1024 * 1024, "QMP message too large");
        bytes.extend_from_slice(&available[..count]);
        io.consume(count);
        if end.is_some() {
            return Ok(serde_json::from_slice(&bytes)?);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixListener;

    #[tokio::test]
    async fn qmp_negotiates_and_ignores_events() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("qmp.socket");
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut io = BufReader::new(socket);
            io.write_all(b"{\"QMP\":{}}\n").await.unwrap();
            for (id, command) in ["qmp_capabilities", "query-status"].iter().enumerate() {
                let request = read_message(&mut io).await.unwrap();
                assert_eq!(request["execute"], *command);
                let response = format!("{{\"event\":\"STOP\"}}\n{{\"return\":{{\"status\":\"running\"}},\"id\":{id}}}\n");
                io.write_all(response.as_bytes()).await.unwrap();
            }
        });
        assert_eq!(
            execute(&path, "query-status").await.unwrap()["status"],
            "running"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn qmp_reports_command_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("qmp.socket");
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut io = BufReader::new(socket);
            io.write_all(b"{\"QMP\":{}}\n").await.unwrap();
            read_message(&mut io).await.unwrap();
            io.write_all(b"{\"return\":{},\"id\":0}\n").await.unwrap();
            read_message(&mut io).await.unwrap();
            io.write_all(b"{\"error\":{\"desc\":\"unsupported\"},\"id\":1}\n")
                .await
                .unwrap();
        });
        assert!(execute(&path, "bad")
            .await
            .unwrap_err()
            .to_string()
            .contains("unsupported"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn qmp_disconnect_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("qmp.socket");
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            let _ = listener.accept().await.unwrap();
        });
        assert!(execute(&path, "quit").await.is_err());
        server.await.unwrap();
    }
}
