//! Per-client focus changes take effect before the ordered storage work.
use super::{Server, core::Tx};
use serde_json::json;
use std::sync::{Arc, Weak, mpsc};
use std::time::{Duration, Instant};
use vk_proto::model::ClientFocus;

pub(super) enum Message {
    Focus(FocusWrite),
    Flush(mpsc::Sender<()>),
}

pub(super) struct FocusWrite {
    pane: String,
    client: String,
    tab: String,
    read: Option<(String, u64)>,
    screen_rev: Option<u64>,
    changed: bool,
    unread: bool,
    marked_unread: bool,
    deadline: Instant,
    done: Option<mpsc::Sender<()>>,
}

pub(super) fn start(server: &Arc<Server>, jobs: mpsc::Receiver<Message>) -> std::io::Result<()> {
    let server: Weak<Server> = Arc::downgrade(server);
    std::thread::Builder::new()
        .name("focus-persistence".into())
        .spawn(move || {
            for message in jobs {
                let job = match message {
                    Message::Focus(job) => job,
                    Message::Flush(done) => {
                        let _ = done.send(());
                        continue;
                    }
                };
                let Some(server) = server.upgrade() else {
                    break;
                };
                while !server.persist_focus(&job) {
                    std::thread::sleep(Duration::from_millis(20));
                }
                if let Some(done) = job.done {
                    let _ = done.send(());
                }
            }
        })?;
    Ok(())
}

impl Server {
    pub(crate) fn flush_focus(&self) {
        let (tx, rx) = mpsc::channel();
        if self.focus_writes.send(Message::Flush(tx)).is_ok() {
            let _ = rx.recv();
        }
    }

    pub fn focus_pane(&self, client: &str, pane: &str) {
        let (tx, rx) = mpsc::channel();
        self.select_focus(client, pane, Some(tx));
        let _ = rx.recv();
    }

    pub(crate) fn select_focus(&self, client: &str, pane: &str, done: Option<mpsc::Sender<()>>) {
        let c = self.core.lock().unwrap();
        let Some(p) = c.pane(pane) else { return };
        let read = c.run_for_pane(pane).map(|r| (r.id.clone(), r.done_rev));
        let mut clients = self.clients.lock().unwrap();
        let st = clients.entry(client.to_owned()).or_default();
        let changed = st.focus.pane.as_deref() != Some(pane);
        st.focus = ClientFocus {
            workspace: Some(p.workspace.clone()),
            tab: Some(p.tab.clone()),
            pane: Some(p.id.clone()),
        };
        st.last_active = Some(Instant::now());
        // Enqueue under the same lock that orders focus changes from different clients.
        let _ = self.focus_writes.send(Message::Focus(FocusWrite {
            pane: p.id.clone(),
            client: client.to_owned(),
            tab: p.tab.clone(),
            read,
            screen_rev: self.pane_rt(pane).map(|rt| rt.rev()),
            changed,
            unread: p.unread,
            marked_unread: p.marked_unread,
            deadline: Instant::now() + Duration::from_secs(5),
            done,
        }));
    }

    fn persist_focus(&self, job: &FocusWrite) -> bool {
        let mut c = self.core.lock().unwrap();
        let Some(p) = c.pane(&job.pane).cloned() else {
            return true;
        };
        let mut tx = Tx::new();
        if p.tab == job.tab
            && let Some(mut tab) = c.tab(&p.tab).cloned()
            && tab.focused_pane.as_ref() != Some(&p.id)
        {
            tab.focused_pane = Some(p.id.clone());
            tx.tab(tab);
        }
        let current = self.client_focus(&job.client).pane.as_ref() == Some(&p.id);
        if ((p.unread && job.unread) || (p.marked_unread && job.marked_unread))
            && (current || self.pane_rt(&p.id).map(|rt| rt.rev()) == job.screen_rev)
        {
            let mut seen = p.clone();
            seen.unread &= !job.unread;
            seen.marked_unread &= !job.marked_unread;
            tx.pane(seen);
        }
        if let Some((run, rev)) = &job.read
            && c.run_for_pane(&p.id)
                .is_some_and(|r| r.id == *run && r.done_rev == *rev)
        {
            tx.m.read_mark("local", &p.id, *rev);
        }
        tx.event(
            "pane.focused",
            super::subject_pane(&p),
            json!({"client": job.client}),
        );
        tx.ephemeral = true;
        tx.focus_deadline = Some(job.deadline);
        if self
            .commit(&mut c, tx)
            .is_err_and(|e| vk_store::Store::is_busy(&e))
        {
            return false;
        }
        drop(c);
        self.refresh_auto_name(&p.id);
        if current && job.changed {
            self.agents.on_focus(self, &p.id);
        }
        true
    }
}
