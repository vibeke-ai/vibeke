//! A private connection from one Codex TUI to the user's local daemon. The TUI remains
//! the foreground process and owns all requests, including approvals.
use anyhow::{Context, Result, bail};
use futures::{SinkExt, StreamExt};
use semver::Version;
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
    let subcommand = first_positional(args);
    let verb = subcommand.and_then(|i| args.get(i)).map(String::as_str);
    let daemon_management = matches!(
        verb,
        Some("app" | "remote-control" | "agents" | "queue" | "archive" | "unarchive" | "delete")
    ) || (verb == Some("app-server")
        && args[subcommand.unwrap() + 1..]
            .iter()
            .any(|a| a == "daemon"));
    let embedded = args.iter().any(|a| a == "--no-daemon");
    let explicit_daemon = args
        .iter()
        .any(|a| a == "daemon_auto_start" || a.starts_with("features.daemon_auto_start="));
    let bypass = std::env::var("CODEX_VIBEKE_SHIM").as_deref() == Ok("0");
    if daemon_management || bypass || explicit_daemon {
        if daemon_management || !embedded {
            strip_pane_env(&mut cli);
        }
        return Err(cli.args(args).exec().into());
    }
    if embedded
        || args.iter().any(|a| {
            matches!(
                a.as_str(),
                "--remote" | "--help" | "-h" | "--version" | "-V"
            ) || a.starts_with("--remote=")
        })
    {
        return Err(cli.args(args).exec().into());
    }
    if matches!(verb, Some("exec" | "e" | "review")) {
        return Err(cli
            .args(["--disable", "daemon_auto_start"])
            .args(args)
            .exec()
            .into());
    }
    if matches!(
        verb,
        Some(
            "app-server"
                | "mcp"
                | "mcp-server"
                | "exec-server"
                | "agents"
                | "queue"
                | "archive"
                | "unarchive"
                | "delete"
                | "plugin"
                | "migrate-rollouts"
                | "migrate-nfs-db"
                | "doctor"
                | "update"
                | "login"
                | "logout"
                | "completion"
                | "help"
                | "features"
                | "sandbox"
                | "apply"
                | "cloud"
                | "debug"
                | "proxy"
        )
    ) {
        return Err(cli.args(args).exec().into());
    }
    let split = if matches!(verb, Some("resume" | "fork")) {
        subcommand.unwrap() + 1
    } else {
        0
    };
    let (prefix, rest) = args.split_at(split);
    cli.args(prefix);
    // These options select a local launch context which a remote TUI cannot reproduce.
    let local = args.iter().any(|a| {
        matches!(a.as_str(), "--worktree" | "--oss" | "--profile" | "-p")
            || a.starts_with("--profile=")
    });
    if !local && let Ok(endpoint) = start_proxy(real) {
        cli.arg("--remote").arg(endpoint);
        if !args
            .iter()
            .any(|a| a == "--cd" || a.starts_with("--cd=") || a.starts_with("-C"))
        {
            cli.arg("--cd").arg(std::env::current_dir()?);
        }
    } else if Command::new(real)
        .args(["--no-daemon", "--version"])
        .stdin(Stdio::null())
        .output()?
        .status
        .success()
    {
        cli.arg("--no-daemon");
    } else {
        cli.args(["--disable", "daemon_auto_start"]);
    }
    Err(cli.args(rest).exec().into())
}

// Skip values belonging to options, so e.g. `-m model resume ID` still places
// connection flags after the subcommand and a prompt after `--` stays a prompt.
fn first_positional(args: &[String]) -> Option<usize> {
    let mut words = args.iter().enumerate();
    while let Some((i, arg)) = words.next() {
        match arg.as_str() {
            "--" => return None,
            "-c"
            | "--config"
            | "--enable"
            | "--disable"
            | "-m"
            | "--model"
            | "-p"
            | "--profile"
            | "-s"
            | "--sandbox"
            | "-a"
            | "--ask-for-approval"
            | "-C"
            | "--cd"
            | "-i"
            | "--image"
            | "--local-provider"
            | "--add-dir"
            | "--remote"
            | "--remote-auth-token-env" => {
                words.next();
            }
            flag if flag.starts_with('-') => {}
            _ => return Some(i),
        }
    }
    None
}

fn strip_pane_env(cmd: &mut Command) {
    for (key, _) in std::env::vars_os() {
        let name = key.to_string_lossy();
        if name == "VIBEKE"
            || name.starts_with("VIBEKE_")
            || name.starts_with("HERDR_")
            || name == "BASH_FUNC_codex%%"
        {
            cmd.env_remove(key);
        }
    }
}

fn start_proxy(real: &str) -> Result<String> {
    let version = Command::new(real).arg("--version").output()?;
    let version = String::from_utf8_lossy(&version.stdout);
    if !supported(version.split_whitespace().last()) {
        bail!("Codex lacks the tested foreground thread protocol");
    }
    let home = std::env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| vk_server::paths::home().join(".codex"));
    let mut manager = Command::new(real);
    manager.args(["app-server", "daemon"]).stdin(Stdio::null());
    // The daemon outlives the pane. Its hooks must never inherit a pane credential.
    strip_pane_env(&mut manager);
    if home.is_absolute() {
        manager.current_dir(vk_server::paths::home());
    }
    let control = home.join("app-server-control/app-server-control.sock");
    let operation = if std::os::unix::net::UnixStream::connect(&control).is_ok() {
        "version"
    } else {
        let features = Command::new(real).args(["features", "list"]).output()?;
        if !String::from_utf8_lossy(&features.stdout)
            .lines()
            .any(|line| {
                let mut fields = line.split_whitespace();
                fields.next() == Some("daemon_auto_start") && fields.last() == Some("true")
            })
        {
            bail!("Codex daemon auto-start is disabled");
        }
        "start"
    };
    let status = manager.arg(operation).output()?;
    let info: Value = serde_json::from_slice(&status.stdout)?;
    if !status.status.success() || !supported(info["appServerVersion"].as_str()) {
        bail!("incompatible Codex daemon");
    }
    let socket = info["socketPath"]
        .as_str()
        .context("missing daemon socket")?;
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

fn supported(version: Option<&str>) -> bool {
    version
        .and_then(|v| Version::parse(v).ok())
        .is_some_and(|v| v >= Version::new(0, 169, 0))
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
        if self.requests.is_empty() {
            return None;
        }
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
                let mut c =
                    vk_cli::client::Client::new(vk_cli::client::connect(Path::new(&socket)).await?);
                c.call(
                    "client.hello",
                    json!({"kind":"agent","client":"vibeke-codex-pane",
                    "version":vk_proto::VERSION,"token":std::env::var("VIBEKE_PANE_TOKEN")?}),
                )
                .await?;
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
