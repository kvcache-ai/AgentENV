use super::{handle_status, Client};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::Instant;

#[derive(Debug, Serialize)]
struct CreateSnapshot<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SnapshotInfo {
    #[serde(rename = "snapshotID")]
    pub snapshot_id: String,
    #[serde(default)]
    pub names: Vec<String>,
    #[serde(rename = "imageRef", default, skip_serializing_if = "Option::is_none")]
    pub image_ref: Option<String>,
    #[serde(
        rename = "sourceSandboxMetadata",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub source_sandbox_metadata: Option<HashMap<String, String>>,
}

impl Client {
    pub fn create_snapshot(&self, sandbox_id: &str, name: Option<&str>) -> Result<SnapshotInfo> {
        let body = CreateSnapshot { name };
        let resp = handle_status(
            self.post(&format!("/sandboxes/{}/snapshots", sandbox_id))
                .send_json(&body),
        )?;
        Ok(resp.into_json()?)
    }

    pub fn list_snapshots(&self, sandbox_id: Option<&str>) -> Result<Vec<SnapshotInfo>> {
        let (snapshots, err) = self.list_snapshots_while(sandbox_id, None, |_| true);
        err.map_or(Ok(snapshots), Err)
    }

    /// List snapshots page by page, keeping what was collected even when a
    /// later page fails.
    ///
    /// Returns the snapshots fetched so far alongside the error that stopped
    /// the walk, if any: completion is best-effort, so a page failing after
    /// earlier ones succeeded still offers those candidates, while
    /// `list_snapshots` turns the error into a failure for the CLI.
    ///
    /// `deadline`, when set, bounds the whole walk. The client's own timeouts
    /// apply per request, so a paged walk otherwise has no overall bound; here
    /// each page's request is capped by the time left, and a deadline already
    /// passed stops the walk without issuing another request. A continuation
    /// token the walk has already used stops it with an error: a server that
    /// repeats (or cycles) tokens would otherwise page forever. `keep_going`
    /// is called with everything collected so far after each page, and only
    /// consulted when there is another page to fetch.
    pub fn list_snapshots_while<F>(
        &self,
        sandbox_id: Option<&str>,
        deadline: Option<Instant>,
        mut keep_going: F,
    ) -> (Vec<SnapshotInfo>, Option<anyhow::Error>)
    where
        F: FnMut(&[SnapshotInfo]) -> bool,
    {
        let mut snapshots = Vec::new();
        let mut next_token: Option<String> = None;
        let mut failure = None;
        let mut used_tokens: HashSet<String> = HashSet::new();

        loop {
            // How long the next page may take. A deadline in the past stops
            // the walk; no deadline leaves the client's per-request timeout
            // in charge.
            let timeout = match deadline {
                Some(deadline) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    Some(remaining)
                }
                None => None,
            };

            // Never re-fetch a token the walk has already used: a repeated
            // (or cycling) continuation token means no progress.
            if let Some(token) = next_token.as_deref() {
                if !used_tokens.insert(token.to_string()) {
                    failure = Some(anyhow!(
                        "snapshot pagination made no progress: continuation token {token} was already used"
                    ));
                    break;
                }
            }

            let mut request = self.get("/snapshots").query("limit", "100");
            if let Some(sandbox_id) = sandbox_id {
                request = request.query("sandboxID", sandbox_id);
            }
            if let Some(token) = next_token.as_deref() {
                request = request.query("nextToken", token);
            }
            if let Some(timeout) = timeout {
                request = request.timeout(timeout);
            }

            let mut page: Vec<SnapshotInfo> = match handle_status(request.call()).and_then(|resp| {
                next_token = resp
                    .header("x-next-token")
                    .map(str::trim)
                    .filter(|token| !token.is_empty())
                    .map(str::to_string);
                resp.into_json::<Vec<SnapshotInfo>>().map_err(Into::into)
            }) {
                Ok(page) => page,
                Err(err) => {
                    failure = Some(err);
                    break;
                }
            };
            snapshots.append(&mut page);

            if next_token.is_none() || !keep_going(&snapshots) {
                break;
            }
        }

        (snapshots, failure)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_json_preserves_optional_source_metadata() {
        for metadata in [
            Some(serde_json::json!({"network": "private", "team": "alpha"})),
            Some(serde_json::json!({})),
            None,
        ] {
            let mut response = serde_json::json!({"snapshotID": "snap-1", "names": ["base"]});
            if let Some(metadata) = metadata {
                response["sourceSandboxMetadata"] = metadata;
            }

            let snapshot: SnapshotInfo = serde_json::from_value(response.clone()).unwrap();
            let output = serde_json::to_value(snapshot).unwrap();

            assert_eq!(output, response);
        }
    }

    #[test]
    fn create_snapshot_serializes_optional_name() {
        let named = serde_json::to_value(CreateSnapshot { name: Some("base") }).unwrap();
        assert_eq!(named["name"], "base");

        let unnamed = serde_json::to_value(CreateSnapshot { name: None }).unwrap();
        assert_eq!(unnamed, serde_json::json!({}));
    }

    #[test]
    fn snapshot_info_supports_optional_image_ref() {
        let with_ref: SnapshotInfo = serde_json::from_value(serde_json::json!({
            "snapshotID": "snap-1",
            "names": [],
            "imageRef": "registry.example/ns/app:agentenv-snapshot-snap-1"
        }))
        .unwrap();
        assert_eq!(
            with_ref.image_ref.as_deref(),
            Some("registry.example/ns/app:agentenv-snapshot-snap-1")
        );

        let without_ref: SnapshotInfo = serde_json::from_value(serde_json::json!({
            "snapshotID": "snap-2",
            "names": []
        }))
        .unwrap();
        assert_eq!(without_ref.image_ref, None);
        assert!(serde_json::to_value(without_ref)
            .unwrap()
            .get("imageRef")
            .is_none());
    }

    /// Read one request off `conn`. GET requests carry headers only, so the
    /// blank line ends them.
    fn drain_request_headers(conn: &mut std::net::TcpStream) {
        use std::io::Read;
        let mut buf = [0u8; 1024];
        let mut seen = Vec::new();
        while !seen.ends_with(b"\r\n\r\n") {
            let n = conn.read(&mut buf).expect("request headers");
            assert!(n > 0, "connection closed before the request ended");
            seen.extend_from_slice(&buf[..n]);
        }
    }

    fn write_response(
        conn: &mut std::net::TcpStream,
        status: &str,
        headers: &[(&str, &str)],
        body: &str,
    ) {
        use std::io::Write;
        let mut resp = format!("HTTP/1.1 {status}\r\ncontent-type: application/json\r\n");
        for (name, value) in headers {
            resp.push_str(&format!("{name}: {value}\r\n"));
        }
        resp.push_str(&format!("content-length: {}\r\n\r\n{body}", body.len()));
        conn.write_all(resp.as_bytes()).unwrap();
    }

    /// One canned response: status line, headers, body.
    struct ServedPage {
        status: &'static str,
        headers: Vec<(&'static str, &'static str)>,
        body: &'static str,
    }

    fn serve(
        listener: std::net::TcpListener,
        pages: Vec<ServedPage>,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            for page in pages {
                let (mut conn, _) = listener.accept().unwrap();
                drain_request_headers(&mut conn);
                write_response(&mut conn, page.status, &page.headers, page.body);
            }
        })
    }

    /// A page failing after earlier ones succeeded must not discard those
    /// pages: completion is best-effort and still offers them (see the review
    /// on #250).
    #[test]
    fn page_failure_returns_the_pages_fetched_before_it() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let client = Client::new(&format!("http://{address}"), "test-key").unwrap();

        // `connection: close` makes the client open a fresh connection per
        // page, which is what the stub accepts.
        let server = serve(
            listener,
            vec![
                ServedPage {
                    status: "200 OK",
                    headers: vec![("x-next-token", "page-2"), ("connection", "close")],
                    body: r#"[{"snapshotID":"snap-1","names":["base"]}]"#,
                },
                ServedPage {
                    status: "500 Internal Server Error",
                    headers: vec![("connection", "close")],
                    body: "",
                },
            ],
        );

        let (snapshots, err) = client.list_snapshots_while(None, None, |_| true);
        server.join().unwrap();
        assert!(err.is_some(), "the failed page should be reported");
        assert_eq!(
            snapshots.len(),
            1,
            "the first page's snapshot should survive"
        );
        assert_eq!(snapshots[0].snapshot_id, "snap-1");
    }

    /// The CLI list is not best-effort: any page failing fails the whole call.
    #[test]
    fn list_snapshots_fails_when_a_page_fails() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let client = Client::new(&format!("http://{address}"), "test-key").unwrap();

        let server = serve(
            listener,
            vec![ServedPage {
                status: "500 Internal Server Error",
                headers: Vec::new(),
                body: "",
            }],
        );

        let result = client.list_snapshots(None);
        server.join().unwrap();
        assert!(result.is_err());
    }

    /// A server that keeps handing back a continuation token the walk has
    /// already used would page forever; the walk must stop and report it
    /// instead (the stub serves exactly two pages, so a third request would
    /// hang the test on `accept`).
    #[test]
    fn repeated_continuation_token_stops_the_walk() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let client = Client::new(&format!("http://{address}"), "test-key").unwrap();

        let server = serve(
            listener,
            vec![
                ServedPage {
                    status: "200 OK",
                    headers: vec![("x-next-token", "page-2"), ("connection", "close")],
                    body: r#"[{"snapshotID":"snap-1","names":[]}]"#,
                },
                ServedPage {
                    status: "200 OK",
                    headers: vec![("x-next-token", "page-2"), ("connection", "close")],
                    body: r#"[{"snapshotID":"snap-2","names":[]}]"#,
                },
            ],
        );

        let (snapshots, err) = client.list_snapshots_while(None, None, |_| true);
        server.join().unwrap();
        assert!(err.is_some(), "the repeated token should be reported");
        assert_eq!(snapshots.len(), 2, "both fetched pages should survive");
        assert_eq!(snapshots[0].snapshot_id, "snap-1");
        assert_eq!(snapshots[1].snapshot_id, "snap-2");
    }

    /// A deadline that has already passed stops the walk without another
    /// request — nothing listens on the address, so any request would have
    /// surfaced as an error instead.
    #[test]
    fn passed_deadline_stops_the_walk_without_a_request() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let client = Client::new(&format!("http://{address}"), "test-key").unwrap();

        let (snapshots, err) = client.list_snapshots_while(None, Some(Instant::now()), |_| true);
        assert!(
            err.is_none(),
            "an expired deadline is a normal stop, not a failure"
        );
        assert!(snapshots.is_empty());
    }
}
