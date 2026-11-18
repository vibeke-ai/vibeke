//! A private connection from one Codex TUI to the user's local daemon. The TUI remains
//! the foreground process and owns all requests, including approvals.
use anyhow::{Context, Result, bail};
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::{fs::PermissionsExt, process::CommandExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;
use tokio_tungstenite::{accept_async, client_async, tungstenite::Message};

pub fn run(args: &[String]) -> Result<()> {
    if let [serve, parent, upstream] = args
        && serve == "--serve"
    {
        let parent = parent.parse()?;
        return tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(serve_pane(parent, Path::new(upstream)));
    }
    let (real, args) = args.split_first().context("missing Codex executable")?;
    let mut cli = Command::new(real);
    let (sub, rest) = match args.split_first() {
        Some((sub, rest)) if matches!(sub.as_str(), "resume" | "fork") => (Some(sub), rest),
        _ => (None, args),
    };
    cli.args(sub);
    // These options select a local launch context which a remote TUI cannot reproduce.
    let local = rest.iter().any(|a| {
        matches!(a.as_str(), "--worktree" | "--oss" | "--profile" | "-p")
            || a.starts_with("--profile=")
    });
    if !local && let Ok(endpoint) = start_proxy(real) {
        cli.arg("--remote").arg(endpoint);
        if !rest
            .iter()
            .any(|a| a == "--cd" || a.starts_with("--cd=") || a.starts_with("-C"))
        {
            cli.arg("--cd").arg(std::env::current_dir()?);
        }
    } else {
        cli.arg("--no-daemon");
    }
    Err(cli.args(rest).exec().into())
}

fn start_proxy(real: &str) -> Result<String> {
    let home = std::env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| vk_server::paths::home().join(".codex"));
    let socket = home.join("app-server-control/app-server-control.sock");
    if std::os::unix::net::UnixStream::connect(&socket).is_err() {
        // Respect a user's disabled auto-start setting. The daemon outlives this pane:
        // do not give it this pane's identity or an elevated Vibeke credential.
        let mut daemon = Command::new(real);
        for (key, _) in std::env::vars_os() {
            let name = key.to_string_lossy();
            if name == "VIBEKE" || name.starts_with("VIBEKE_") || name.starts_with("HERDR_") {
                daemon.env_remove(key);
            }
        }
        let features = Command::new(real).args(["features", "list"]).output()?;
        if !String::from_utf8_lossy(&features.stdout).lines().any(|l| {
            let mut words = l.split_whitespace();
            words.next() == Some("daemon_auto_start") && words.last() == Some("true")
        }) || !daemon
            .args(["app-server", "daemon", "start"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?
            .success()
        {
            bail!("shared daemon unavailable");
        }
    }
    let mut worker = Command::new(std::env::current_exe()?)
        .args(["codex-pane", "--serve", &std::process::id().to_string()])
        .arg(socket)
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let mut endpoint = String::new();
    BufReader::new(worker.stdout.take().unwrap()).read_line(&mut endpoint)?;
    if !endpoint.starts_with("unix:///") {
        let _ = worker.wait();
        bail!("proxy failed to start");
    }
    Ok(endpoint.trim_end().into())
}

async fn serve_pane(parent: u32, upstream: &Path) -> Result<()> {
    let dir = tempfile::Builder::new()
        .prefix("vk-cx-")
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir_in("/tmp")?;
    let socket = dir.path().join("rpc");
    let listener = UnixListener::bind(&socket)?;
    // Check the protocol before directing the TUI to the proxy.
    tokio::time::timeout(Duration::from_secs(3), connect(upstream)).await??;
    println!("unix://{}", socket.display());
    std::io::stdout().flush()?;
    let (binding, rx) = watch::channel(None);
    tokio::spawn(report_bindings(parent, rx));
    tokio::select! {
        _ = async { while std::os::unix::process::parent_id() == parent { tokio::time::sleep(Duration::from_millis(500)).await; } } => Ok(()),
        result = async {
            loop {
                let (stream, _) = listener.accept().await?;
                // One TUI connection at a time. A reconnect starts with fresh request IDs.
                let _ = bridge(stream, upstream, &binding).await;
            }
        } => result,
    }
}

async fn connect(path: &Path) -> Result<tokio_tungstenite::WebSocketStream<UnixStream>> {
    let stream = UnixStream::connect(path).await?;
    Ok(client_async("ws://localhost/rpc", stream).await?.0)
}

#[derive(Default)]
struct Threads {
    requests: HashSet<String>,
}

impl Threads {
    fn request(&mut self, frame: &Message) {
        let Some(m) = message(frame) else { return };
        let user = match m["method"].as_str() {
            Some("thread/start") => {
                m["params"]["threadSource"] == "user" && m["params"]["ephemeral"] != true
            }
            Some("thread/resume" | "thread/fork") => true,
            _ => false,
        };
        if user && !m["id"].is_null() {
            self.requests.insert(m["id"].to_string());
        }
    }

    fn response(&mut self, frame: &Message) -> Option<Value> {
        let m = message(frame)?;
        if m.get("method").is_some() || !self.requests.remove(&m.get("id")?.to_string()) {
            return None;
        }
        let th = &m["result"]["thread"];
        Some(
            json!({"session_id":th["id"].as_str()?, "transcript_path":th["path"].as_str()?,
            "cwd": th["cwd"], "model":m["result"]["model"], "source":"startup"}),
        )
    }
}

fn message(frame: &Message) -> Option<Value> {
    let Message::Text(text) = frame else {
        return None;
    };
    serde_json::from_str(text).ok()
}

async fn bridge(
    stream: UnixStream,
    upstream: &Path,
    binding: &watch::Sender<Option<Value>>,
) -> Result<()> {
    let mut client = accept_async(stream).await?;
    let mut server = connect(upstream).await?;
    let mut threads = Threads::default();
    loop {
        tokio::select! {
            msg = client.next() => {
                let Some(msg) = msg.transpose()? else { return Ok(()) };
                threads.request(&msg);
                if msg.is_close() {return Ok(())}
                server.send(msg).await?;
            }
            msg = server.next() => {
                let Some(msg) = msg.transpose()? else { return Ok(()) };
                if let Some(payload) = threads.response(&msg) { binding.send_replace(Some(payload)); }
                if msg.is_close() {return Ok(())}
                client.send(msg).await?;
            }
        }
    }
}

async fn report_bindings(pid: u32, mut rx: watch::Receiver<Option<Value>>) {
    while rx.changed().await.is_ok() {
        loop {
            let payload = rx.borrow_and_update().clone();
            let rpc = async {
                let socket = std::env::var_os("VIBEKE_SOCKET").context("missing pane socket")?;
                let mut c = vk_cli::client::Client::new(UnixStream::connect(socket).await?);
                c.hello("agent").await?;
                c.call(
                    "adapter.signal",
                    json!({"harness":"codex","event":"SessionStart","pid":pid,"payload":payload}),
                )
                .await?;
                Ok::<_, anyhow::Error>(())
            };
            if tokio::time::timeout(Duration::from_secs(3), rpc)
                .await
                .is_ok_and(|r| r.is_ok())
            {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(v: Value) -> Message {
        Message::text(v.to_string())
    }

    #[test]
    fn only_a_reply_to_this_clients_foreground_request_can_bind() {
        let mut threads = Threads::default();
        threads.request(&frame(json!({"id":"root","method":"thread/start","params":{"threadSource":"user","ephemeral":false}})));
        threads.request(&frame(json!({"id":2,"method":"thread/start","params":{"threadSource":"thread_title","ephemeral":true}})));
        let thread = json!({"id":"session-a","path":"/home/user/rollout.jsonl"});
        assert!(
            threads
                .response(&frame(
                    json!({"method":"thread/started","params":{"thread":thread}})
                ))
                .is_none()
        );
        assert!(
            threads
                .response(&frame(json!({"id":2,"result":{"thread":thread}})))
                .is_none()
        );
        let response = frame(json!({"id":"root","result":{"thread":thread}}));
        assert_eq!(
            threads.response(&response).unwrap()["session_id"],
            "session-a"
        );
        assert!(threads.response(&response).is_none());
        for method in ["thread/fork", "thread/resume"] {
            threads.request(&frame(json!({"id":9,"method":method})));
            assert_eq!(
                threads
                    .response(&frame(json!({"id":9,"result":{"thread":thread}})))
                    .unwrap()["transcript_path"],
                thread["path"]
            );
        }
    }
}
