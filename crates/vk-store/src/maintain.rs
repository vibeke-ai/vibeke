//! Keeping `state.db` and its WAL small (02 §3 "Storage maintenance").
//!
//! - New databases use incremental auto-vacuum, so pages that deletes free can be given back to
//!   the file system. A database created by an older version has no auto-vacuum: the server
//!   rebuilds it once with `VACUUM` ([`Store::compact_if_needed`]) shortly after it starts.
//! - `journal_size_limit` truncates the WAL file after a checkpoint instead of keeping it at the
//!   size of the largest burst of writes it ever held.
//! - [`Store::maintain`] runs after every retention sweep: it returns free pages in small steps
//!   (each its own transaction, so the WAL stays small) and then checkpoints with `TRUNCATE`.
//! - Retention deletes run in batches of [`DELETE_BATCH`] rows for the same reason.

use super::*;

/// Rows removed per statement by retention deletes.
pub(crate) const DELETE_BATCH: i64 = 5_000;
/// The WAL file is cut back to at most this many bytes when SQLite resets it.
pub const WAL_SIZE_LIMIT: i64 = 64 * 1024 * 1024;
/// Free pages returned per `incremental_vacuum` step (16 MiB with 4 KiB pages).
const VACUUM_STEP_PAGES: i64 = 4_096;

/// What [`Store::compact_if_needed`] did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct CompactReport {
    pub bytes_before: u64,
    pub bytes_after: u64,
}

/// What [`Store::maintain`] did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct MaintainReport {
    /// Free pages given back to the file system.
    pub freed_pages: i64,
    /// Whether the checkpoint copied the whole WAL back and truncated it (`false` while another
    /// connection still reads an older snapshot).
    pub wal_truncated: bool,
}

/// Cap the WAL file size on `conn` (per connection, so every opener sets it).
pub(crate) fn limit_wal(conn: &Connection) -> Result<()> {
    let _: i64 = conn.query_row(
        &format!("PRAGMA journal_size_limit = {WAL_SIZE_LIMIT}"),
        [],
        |r| r.get(0),
    )?;
    Ok(())
}

impl Store {
    /// Whether the database still lacks incremental auto-vacuum (created before 0.23).
    pub fn needs_compaction(&self) -> Result<bool> {
        let mode: i64 = self
            .conn
            .query_row("PRAGMA auto_vacuum", [], |r| r.get(0))?;
        Ok(mode != 2)
    }

    /// Rebuild a database created without incremental auto-vacuum: `VACUUM` writes it again
    /// without free pages and with auto-vacuum on, then a `TRUNCATE` checkpoint empties the WAL.
    /// Returns `None` when there is nothing to do. Blocks writers for as long as the rebuild
    /// takes (seconds for a few hundred MB of live data).
    pub fn compact_if_needed(&self) -> Result<Option<CompactReport>> {
        if !self.needs_compaction()? {
            return Ok(None);
        }
        let bytes_before = self.file_bytes()?;
        // `VACUUM` applies the pending auto-vacuum mode of this connection.
        self.conn
            .pragma_update(None, "auto_vacuum", "INCREMENTAL")?;
        self.conn.execute_batch("VACUUM")?;
        self.checkpoint_truncate()?;
        Ok(Some(CompactReport {
            bytes_before,
            bytes_after: self.file_bytes()?,
        }))
    }

    /// Give free pages back to the file system and empty the WAL (after a retention sweep).
    pub fn maintain(&self) -> Result<MaintainReport> {
        let mut freed = 0;
        loop {
            let free = self.free_pages()?;
            if free == 0 {
                break;
            }
            self.incremental_vacuum(free.min(VACUUM_STEP_PAGES))?;
            let after = self.free_pages()?;
            // No progress: auto-vacuum is off (a database still waiting for compaction).
            if after >= free {
                break;
            }
            freed += free - after;
        }
        Ok(MaintainReport {
            freed_pages: freed,
            wal_truncated: self.checkpoint_truncate()?,
        })
    }

    /// Pages on the free list.
    pub fn free_pages(&self) -> Result<i64> {
        Ok(self
            .conn
            .query_row("PRAGMA freelist_count", [], |r| r.get(0))?)
    }

    /// `page_count × page_size`: the main database file's size, free pages included.
    fn file_bytes(&self) -> Result<u64> {
        self.db_bytes()
    }

    fn incremental_vacuum(&self, pages: i64) -> Result<()> {
        // The pragma frees pages while it is stepped: run it to the end (`execute_batch` steps a
        // statement only once).
        let mut st = self
            .conn
            .prepare(&format!("PRAGMA incremental_vacuum({pages})"))?;
        let mut rows = st.raw_query();
        while rows.next()?.is_some() {}
        Ok(())
    }

    /// `PRAGMA wal_checkpoint(TRUNCATE)`; `true` when it completed. In-memory databases have no
    /// WAL and report success.
    fn checkpoint_truncate(&self) -> Result<bool> {
        let busy: i64 = self
            .conn
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| r.get(0))?;
        Ok(busy == 0)
    }

    /// Distinct text values at JSON `path` among the entities of `kind` (only the open ones when
    /// `open_only`). Reads only that field, so a caller that needs one field does not decode
    /// every document.
    pub fn field_values(&self, kind: &str, path: &str, open_only: bool) -> Result<Vec<String>> {
        let mut st = self.conn.prepare_cached(
            "SELECT DISTINCT json_extract(json, ?2) FROM entities
              WHERE kind=?1 AND (closed=0 OR ?3=0) AND json_type(json, ?2) = 'text'",
        )?;
        let rows = st.query_map(params![kind, path, open_only], |r| r.get::<_, String>(0))?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// Closed entities of `kind` last updated before `before_ms`, oldest first, with the value
    /// of the indexed JSON field `field` (see [`INDEXED_FIELDS`]; `None` when absent). At most
    /// `limit` rows, after the `(updated_at, id)` cursor `after` when given, so a caller that
    /// keeps some rows can page past them.
    pub fn closed_before(
        &self,
        kind: &str,
        before_ms: i64,
        field: &str,
        after: Option<&(i64, String)>,
        limit: usize,
    ) -> Result<Vec<ClosedRow>> {
        let field = indexed_field(field)
            .with_context(|| format!("closed_before: {field} is not an indexed field"))?;
        let (cursor_ms, cursor_id) = match after {
            Some((ms, id)) => (*ms, id.as_str()),
            None => (i64::MIN, ""),
        };
        let mut st = self.conn.prepare_cached(&format!(
            "SELECT id, updated_at, json_extract(json, '{field}') FROM entities
              WHERE kind=?1 AND closed=1 AND updated_at < ?2 AND (updated_at, id) > (?3, ?4)
              ORDER BY updated_at, id LIMIT ?5"
        ))?;
        let rows = st.query_map(
            params![kind, before_ms, cursor_ms, cursor_id, limit as i64],
            |r| {
                Ok(ClosedRow {
                    id: r.get(0)?,
                    updated_at: r.get(1)?,
                    field: r.get::<_, Option<String>>(2).unwrap_or(None),
                })
            },
        )?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }
}

/// One row of [`Store::closed_before`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClosedRow {
    pub id: String,
    pub updated_at: i64,
    pub field: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn wal_len(db: &Path) -> u64 {
        let mut p = db.as_os_str().to_owned();
        p.push("-wal");
        std::fs::metadata(Path::new(&p))
            .map(|m| m.len())
            .unwrap_or(0)
    }

    /// Many large rows, then delete them: leaves free pages behind.
    fn churn(s: &mut Store, n: usize) {
        let big = "x".repeat(8 * 1024);
        for chunk in (0..n).collect::<Vec<_>>().chunks(200) {
            let mut m = Mutation::new();
            for i in chunk {
                m.put("blob_row", &format!("r{i}"), None, &json!({"v": big}));
            }
            s.commit(m).unwrap();
        }
        for chunk in (0..n).collect::<Vec<_>>().chunks(200) {
            let mut m = Mutation::new();
            for i in chunk {
                m.delete("blob_row", &format!("r{i}"));
            }
            s.commit(m).unwrap();
        }
    }

    #[test]
    fn new_databases_use_incremental_auto_vacuum_and_need_no_compaction() {
        let d = tempfile::tempdir().unwrap();
        let s = Store::open(&d.path().join("s/state.db")).unwrap();
        assert!(!s.needs_compaction().unwrap());
        assert_eq!(s.compact_if_needed().unwrap(), None);
        let limit: i64 = s
            .conn
            .query_row("PRAGMA journal_size_limit", [], |r| r.get(0))
            .unwrap();
        assert_eq!(limit, WAL_SIZE_LIMIT);
    }

    #[test]
    fn maintain_returns_free_pages_and_truncates_the_wal() {
        let d = tempfile::tempdir().unwrap();
        let db = d.path().join("s/state.db");
        let mut s = Store::open(&db).unwrap();
        churn(&mut s, 2_000);
        assert!(s.free_pages().unwrap() > 1_000);
        let r = s.maintain().unwrap();
        assert!(r.freed_pages > 1_000, "{r:?}");
        assert!(r.wal_truncated);
        assert_eq!(s.free_pages().unwrap(), 0);
        assert_eq!(wal_len(&db), 0);
    }

    #[test]
    fn an_old_database_is_compacted_once() {
        let d = tempfile::tempdir().unwrap();
        let db = d.path().join("s/state.db");
        {
            let mut s = Store::open(&db).unwrap();
            churn(&mut s, 2_000);
            let mut m = Mutation::new();
            m.put("keep", "k", None, &json!({"run_id": "r1"}));
            s.commit(m).unwrap();
        }
        // What a database created before 0.23 looks like: no auto-vacuum, free pages.
        {
            let c = Connection::open(&db).unwrap();
            c.execute_batch("PRAGMA auto_vacuum = NONE; VACUUM;")
                .unwrap();
            c.execute_batch(
                "CREATE TABLE filler (b BLOB);
                 WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 2000)
                 INSERT INTO filler SELECT zeroblob(8192) FROM n;
                 DROP TABLE filler;",
            )
            .unwrap();
            let mode: i64 = c.query_row("PRAGMA auto_vacuum", [], |r| r.get(0)).unwrap();
            assert_eq!(mode, 0);
        }
        let s = Store::open(&db).unwrap();
        assert!(s.needs_compaction().unwrap());
        // Without auto-vacuum, maintenance cannot free pages (and does not loop).
        assert_eq!(s.maintain().unwrap().freed_pages, 0);
        let r = s.compact_if_needed().unwrap().expect("compacted");
        assert!(r.bytes_after < r.bytes_before, "{r:?}");
        assert!(!s.needs_compaction().unwrap());
        assert_eq!(s.free_pages().unwrap(), 0);
        assert_eq!(wal_len(&db), 0);
        let kept: Vec<Value> = s.load_by_field("keep", "$.run_id", "r1").unwrap();
        assert_eq!(kept.len(), 1);
        assert_eq!(s.compact_if_needed().unwrap(), None);
    }

    #[test]
    fn indexed_fields_use_their_index() {
        let mut s = Store::open_in_memory().unwrap();
        let mut m = Mutation::new();
        m.put(
            "stream_item",
            "a",
            None,
            &json!({"native_id": "n1", "run_id": "r1"}),
        );
        m.put(
            "stream_item",
            "b",
            None,
            &json!({"native_id": "n2", "run_id": "r1"}),
        );
        m.close(
            "stream_item",
            "c",
            None,
            &json!({"native_id": "n1", "run_id": "r2"}),
        );
        s.commit(m).unwrap();
        let hits: Vec<Value> = s.load_by_field("stream_item", "$.native_id", "n1").unwrap();
        assert_eq!(hits.len(), 2);
        let hits: Vec<Value> = s.load_by_field("stream_item", "$.run_id", "r1").unwrap();
        assert_eq!(hits.len(), 2);
        for (field, index) in [
            ("$.run_id", "entities_run_id"),
            ("$.run", "entities_run"),
            ("$.turn_id", "entities_turn_id"),
            ("$.native_id", "entities_native_id"),
            ("$.task_id", "entities_task_id"),
        ] {
            let plan: String = s
                .conn
                .query_row(
                    &format!("EXPLAIN QUERY PLAN SELECT json FROM entities WHERE kind='k' AND json_extract(json, '{field}')='v' ORDER BY rowid"),
                    [],
                    |r| r.get(3),
                )
                .unwrap();
            assert!(plan.contains(index), "{field}: {plan}");
        }
    }

    #[test]
    fn closed_before_pages_by_cursor() {
        let mut s = Store::open_in_memory().unwrap();
        let mut m = Mutation::new();
        for i in 0..5 {
            m.close(
                "tool_item",
                &format!("t{i}"),
                None,
                &json!({"run_id": format!("r{i}")}),
            );
        }
        m.put("tool_item", "open", None, &json!({"run_id": "r9"}));
        s.commit(m).unwrap();
        let later = now_ms() + 1_000;
        let first = s
            .closed_before("tool_item", later, "$.run_id", None, 3)
            .unwrap();
        assert_eq!(first.len(), 3);
        let last = first.last().unwrap();
        let rest = s
            .closed_before(
                "tool_item",
                later,
                "$.run_id",
                Some(&(last.updated_at, last.id.clone())),
                10,
            )
            .unwrap();
        assert_eq!(rest.len(), 2);
        let mut runs: Vec<_> = first
            .iter()
            .chain(&rest)
            .map(|r| r.field.clone().unwrap())
            .collect();
        runs.sort();
        assert_eq!(runs, ["r0", "r1", "r2", "r3", "r4"]);
        assert!(
            s.closed_before("tool_item", 0, "$.run_id", None, 10)
                .unwrap()
                .is_empty()
        );
        assert!(
            s.closed_before("tool_item", later, "$.nope", None, 10)
                .is_err()
        );
    }

    #[test]
    fn field_values_reads_one_field_of_open_entities() {
        let mut s = Store::open_in_memory().unwrap();
        let mut m = Mutation::new();
        m.put("stream_item", "a", None, &json!({"payload_ref": "h1"}));
        m.put("stream_item", "b", None, &json!({"payload_ref": "h1"}));
        m.put("stream_item", "c", None, &json!({}));
        m.close("stream_item", "d", None, &json!({"payload_ref": "h2"}));
        s.commit(m).unwrap();
        assert_eq!(
            s.field_values("stream_item", "$.payload_ref", true)
                .unwrap(),
            ["h1"]
        );
        let mut all = s
            .field_values("stream_item", "$.payload_ref", false)
            .unwrap();
        all.sort();
        assert_eq!(all, ["h1", "h2"]);
    }
}
