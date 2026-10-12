//! p8ster: a slow, polite, single-threaded pastebin.com scraper.
//!
//! Usage:
//!   p8ster              guess random 6-char IDs forever
//!   p8ster -l 8         guess random 8-char IDs forever (6, 7 or 8)
//!   p8ster ID [ID...]   scrape just these IDs (any supported length), then exit

use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::thread::sleep;
use std::time::{Duration, Instant};

use chrono::{NaiveDateTime, TimeDelta};
use clap::Parser;
use rusqlite::{Connection, OptionalExtension, params};

const BASE_URL: &str = "https://pastebin.com";
const USER_AGENT: &str =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:157.0) Gecko/20100101 Firefox/157.0";
/// Pastebin ID lengths over the years: 6 (oldest), then 7, now 8.
const ID_LEN_MIN: usize = 6;
const ID_LEN_MAX: usize = 8;
const ID_LEN_DEFAULT: u8 = 6;
const DELAY_MIN_MS: u64 = 1001;
const DELAY_MAX_MS: u64 = 1111;
/// Pause after the 1st, 2nd, 3rd... consecutive 429. The last value repeats.
const BACKOFF_SECS: &[u64] = &[10, 30, 60, 120, 300, 600];
const MAX_BODY_BYTES: u64 = 32 * 1024 * 1024;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS pastes (
    id      VARCHAR(8) NOT NULL PRIMARY KEY CHECK (length(id) BETWEEN 6 AND 8),
    date    DATETIME   NULL DEFAULT NULL,
    content TEXT       NULL DEFAULT NULL
);";

// ------------------------------------------------------------------- cli

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    /// Length of randomly guessed IDs
    #[arg(short = 'l', long = "length", value_name = "LEN",
          default_value_t = ID_LEN_DEFAULT,
          value_parser = clap::value_parser!(u8).range(ID_LEN_MIN as i64..=ID_LEN_MAX as i64))]
    length: u8,

    /// Scrape just these IDs, then exit
    #[arg(value_name = "ID")]
    ids: Vec<String>,
}

// ---------------------------------------------------------------- output

fn color_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("NO_COLOR").is_none() && std::io::stdout().is_terminal())
}

fn paint(code: &str, msg: &str) -> String {
    if color_enabled() {
        format!("\x1b[{code}m{msg}\x1b[0m")
    } else {
        msg.to_string()
    }
}

fn info(msg: impl AsRef<str>) {
    println!("{}", msg.as_ref());
}

fn good(msg: impl AsRef<str>) {
    println!("{}", paint("32", msg.as_ref()));
}

fn bad(msg: impl AsRef<str>) {
    eprintln!("{}", paint("31", msg.as_ref()));
}

// -------------------------------------------------------------- database

/// How far along a given ID is.
#[derive(Clone, Copy, PartialEq)]
enum RowState {
    /// Not in the database at all.
    Unseen,
    /// Content saved, date missing (date fetch failed earlier): scrape again.
    Partial,
    /// Both NULL: pastebin said 404. Don't re-check.
    Dead,
    /// Date present: fully scraped.
    Done,
}

fn db_path() -> Result<PathBuf, String> {
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .ok_or("cannot determine home directory")?;
    Ok(PathBuf::from(home).join(".p8ster").join("p8ster.db"))
}

fn open_db(path: &Path) -> Result<Connection, String> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)
            .map_err(|e| format!("cannot create {}: {e}", dir.display()))?;
    }
    let conn =
        Connection::open(path).map_err(|e| format!("cannot open {}: {e}", path.display()))?;
    migrate_id_check(&conn).map_err(|e| format!("cannot migrate schema: {e}"))?;
    conn.execute_batch(SCHEMA)
        .map_err(|e| format!("cannot create schema: {e}"))?;
    Ok(conn)
}

/// Databases created before variable-length IDs have `CHECK (length(id) = 8)`,
/// which rejects 6- and 7-char IDs. SQLite can't alter a CHECK in place, so
/// rebuild the table with the current schema and copy every row across.
fn migrate_id_check(conn: &Connection) -> rusqlite::Result<()> {
    let sql: Option<String> = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'pastes'",
            [],
            |r| r.get(0),
        )
        .optional()?;
    if !sql.is_some_and(|s| s.contains("length(id) = 8")) {
        return Ok(());
    }
    info("migrating database to allow 6-8 char IDs");
    let tx = conn.unchecked_transaction()?;
    tx.execute_batch("ALTER TABLE pastes RENAME TO pastes_old;")?;
    tx.execute_batch(SCHEMA)?;
    tx.execute_batch(
        "INSERT INTO pastes (id, date, content) SELECT id, date, content FROM pastes_old;
         DROP TABLE pastes_old;",
    )?;
    tx.commit()
}

fn row_state(conn: &Connection, id: &str) -> rusqlite::Result<RowState> {
    let row = conn
        .query_row(
            "SELECT date IS NOT NULL, content IS NOT NULL FROM pastes WHERE id = ?1",
            params![id],
            |r| Ok((r.get::<_, bool>(0)?, r.get::<_, bool>(1)?)),
        )
        .optional()?;
    Ok(match row {
        None => RowState::Unseen,
        Some((true, _)) => RowState::Done,
        Some((false, true)) => RowState::Partial,
        Some((false, false)) => RowState::Dead,
    })
}

fn mark_dead(conn: &Connection, id: &str) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO pastes (id, date, content) VALUES (?1, NULL, NULL)
         ON CONFLICT(id) DO NOTHING",
        params![id],
    )?;
    Ok(())
}

/// Write content and date together, in one statement, once both are known.
fn save_paste(
    conn: &Connection,
    id: &str,
    content: &str,
    date: Option<&str>,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO pastes (id, date, content) VALUES (?1, ?2, ?3)
         ON CONFLICT(id) DO UPDATE SET date = excluded.date, content = excluded.content",
        params![id, date, content],
    )?;
    Ok(())
}

fn partial_ids(conn: &Connection) -> rusqlite::Result<Vec<String>> {
    let mut stmt =
        conn.prepare("SELECT id FROM pastes WHERE date IS NULL AND content IS NOT NULL")?;
    let ids = stmt
        .query_map([], |r| r.get(0))?
        .collect::<Result<Vec<String>, _>>()?;
    Ok(ids)
}

// ---------------------------------------------------------------- network

enum Fetch {
    Body(String),
    Status(u16),
}

struct Client {
    agent: ureq::Agent,
    last_request: Option<Instant>,
    consecutive_429: usize,
}

impl Client {
    fn new() -> Self {
        let config = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .timeout_global(Some(Duration::from_secs(30)))
            .max_idle_connections(1)
            .max_idle_connections_per_host(1)
            .build();
        Self {
            agent: ureq::Agent::new_with_config(config),
            last_request: None,
            consecutive_429: 0,
        }
    }

    /// Wait a random 1001–3001 ms since the previous request finished.
    fn pace(&self) {
        if let Some(last) = self.last_request {
            let delay = Duration::from_millis(fastrand::u64(DELAY_MIN_MS..=DELAY_MAX_MS));
            if let Some(rest) = delay.checked_sub(last.elapsed()) {
                sleep(rest);
            }
        }
    }

    /// GET a URL, transparently waiting out 429s with escalating pauses.
    fn get(&mut self, url: &str) -> Result<Fetch, ureq::Error> {
        loop {
            self.pace();
            let result = self
                .agent
                .get(url)
                .header("User-Agent", USER_AGENT)
                .header(
                    "Accept",
                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                )
                .header("Accept-Language", "en-US,en;q=0.5")
                .call();
            self.last_request = Some(Instant::now());
            let mut resp = result?;
            let status = resp.status().as_u16();

            if status == 429 {
                let step = self.consecutive_429.min(BACKOFF_SECS.len() - 1);
                let mut wait = BACKOFF_SECS[step];
                let retry_after = resp
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.trim().parse::<u64>().ok());
                if let Some(ra) = retry_after {
                    wait = wait.max(ra);
                }
                self.consecutive_429 += 1;
                bad(format!(
                    "HTTP 429 Too Many Requests (#{} in a row), pausing {wait}s",
                    self.consecutive_429
                ));
                sleep(Duration::from_secs(wait));
                continue;
            }

            self.consecutive_429 = 0;
            if status != 200 {
                return Ok(Fetch::Status(status));
            }
            let bytes = resp
                .body_mut()
                .with_config()
                .limit(MAX_BODY_BYTES)
                .read_to_vec()?;
            self.last_request = Some(Instant::now());
            return Ok(Fetch::Body(String::from_utf8_lossy(&bytes).into_owned()));
        }
    }
}

// ----------------------------------------------------------------- dates

/// Pull the creation timestamp from the paste page, e.g.
/// `<div class="date"><span title="Saturday 25th of May 2024 04:12:33 PM CDT">`
fn extract_date_title(html: &str) -> Option<&str> {
    let div = html.find("<div class=\"date\"")?;
    let rest = &html[div..];
    let attr = rest.find("title=\"")?;
    if attr > 500 {
        return None; // too far away, not ours
    }
    let rest = &rest[attr + 7..];
    Some(&rest[..rest.find('"')?])
}

/// "Saturday 25th of May 2024 04:12:33 PM CDT" -> "2024-05-25 21:12:33" (UTC)
fn parse_pastebin_date(title: &str) -> Option<String> {
    let p: Vec<&str> = title.split_whitespace().collect();
    if p.len() < 8 || p[2] != "of" {
        return None;
    }
    let day: String = p[1].chars().take_while(char::is_ascii_digit).collect();
    let naive = NaiveDateTime::parse_from_str(
        &format!("{day} {} {} {} {}", p[3], p[4], p[5], p[6]),
        "%d %B %Y %I:%M:%S %p",
    )
    .ok()?;
    let utc_offset_hours = match p[7] {
        "UTC" | "GMT" => 0,
        "EDT" => -4,
        "EST" | "CDT" => -5,
        "CST" | "MDT" => -6,
        "MST" | "PDT" => -7,
        "PST" => -8,
        _ => return None,
    };
    let utc = naive - TimeDelta::hours(utc_offset_hours);
    Some(utc.format("%Y-%m-%d %H:%M:%S").to_string())
}

// --------------------------------------------------------------- scraping

fn random_id(len: usize) -> String {
    (0..len).map(|_| fastrand::alphanumeric()).collect()
}

fn valid_id(id: &str) -> bool {
    (ID_LEN_MIN..=ID_LEN_MAX).contains(&id.len()) && id.bytes().all(|b| b.is_ascii_alphanumeric())
}

fn next_id(conn: &Connection, len: usize) -> rusqlite::Result<String> {
    loop {
        let id = random_id(len);
        match row_state(conn, &id)? {
            RowState::Unseen | RowState::Partial => return Ok(id),
            RowState::Dead | RowState::Done => info(format!("{id} already in database, rerolling")),
        }
    }
}

/// Full scrape: raw content first, then the paste page for its date.
fn scrape(conn: &Connection, client: &mut Client, id: &str, tag: &str) {
    match client.get(&format!("{BASE_URL}/raw/{id}")) {
        Err(e) => bad(format!("{tag}  network error: {e} (not recorded)")),
        Ok(Fetch::Status(404)) => match mark_dead(conn, id) {
            Ok(()) => info(format!("{tag}  not found")),
            Err(e) => bad(format!("{tag}  database error: {e}")),
        },
        Ok(Fetch::Status(code)) => bad(format!("{tag}  unexpected HTTP {code} (not recorded)")),
        Ok(Fetch::Body(content)) => {
            good(format!("{tag}  FOUND, {} bytes", content.len()));
            // Nothing is written until the paste page has been fetched too.
            let Some(date) = fetch_date(client, id, tag) else {
                return;
            };
            match save_paste(conn, id, &content, date.as_deref()) {
                Ok(()) => match &date {
                    Some(d) => good(format!("{tag}  saved, created {d}")),
                    None => bad(format!("{tag}  saved without date (no date on paste page)")),
                },
                Err(e) => bad(format!("{tag}  database error: {e}")),
            }
        }
    }
}

/// Fetch the paste page and pull out its creation date.
/// `None`: network/HTTP failure, record nothing so the ID stays retryable.
/// `Some(None)`: page loaded fine but had no recognisable date.
fn fetch_date(client: &mut Client, id: &str, tag: &str) -> Option<Option<String>> {
    let html = match client.get(&format!("{BASE_URL}/{id}")) {
        Ok(Fetch::Body(html)) => html,
        Ok(Fetch::Status(code)) => {
            bad(format!(
                "{tag}  paste page returned HTTP {code} (not recorded)"
            ));
            return None;
        }
        Err(e) => {
            bad(format!(
                "{tag}  network error on paste page: {e} (not recorded)"
            ));
            return None;
        }
    };
    let Some(title) = extract_date_title(&html) else {
        return Some(None);
    };
    Some(Some(match parse_pastebin_date(title) {
        Some(utc) => utc,          // "YYYY-MM-DD HH:MM:SS", UTC
        None => title.to_string(), // unrecognised format, kept as-is
    }))
}

fn main() {
    let cli = Cli::parse();
    let path = match db_path() {
        Ok(p) => p,
        Err(e) => {
            bad(format!("error: {e}"));
            std::process::exit(1);
        }
    };
    let existed = path.exists();
    let conn = match open_db(&path) {
        Ok(c) => c,
        Err(e) => {
            bad(format!("database error: {e}"));
            std::process::exit(1);
        }
    };
    info(format!(
        "p8ster: {} {}",
        if existed { "using" } else { "created" },
        path.display()
    ));

    let mut client = Client::new();

    // Explicit IDs on the command line: scrape those and exit.
    if !cli.ids.is_empty() {
        for id in &cli.ids {
            if valid_id(id) {
                scrape(&conn, &mut client, id, id);
            } else {
                bad(format!(
                    "{id}  invalid ID (need {ID_LEN_MIN}-{ID_LEN_MAX} chars of 0-9a-zA-Z)"
                ));
            }
        }
        return;
    }

    // Finish rows that have content but no date from earlier runs.
    match partial_ids(&conn) {
        Ok(ids) if !ids.is_empty() => {
            info(format!("resuming {} incomplete paste(s)", ids.len()));
            for id in ids {
                scrape(&conn, &mut client, &id, &format!("resume {id}"));
            }
        }
        Ok(_) => {}
        Err(e) => bad(format!("database error while resuming: {e}")),
    }

    let mut n: u64 = 0;
    loop {
        let id = match next_id(&conn, cli.length as usize) {
            Ok(id) => id,
            Err(e) => {
                bad(format!("database error: {e}"));
                sleep(Duration::from_secs(5));
                continue;
            }
        };
        n += 1;
        scrape(&conn, &mut client, &id, &format!("#{n} {id}"));
    }
}
