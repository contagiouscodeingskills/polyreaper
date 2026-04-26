//! Manual argv parser for the replayer CLI.
//!
//! No clap — keeps the binary lean and matches the recorder's hand-rolled
//! arg style. The replayer has a small, fixed sub-command set; full
//! flag-discovery isn't worth a 30-crate dependency.

use std::collections::HashSet;
use std::path::PathBuf;

use common::Venue;
use replayer::ReplayFilter;

#[derive(Debug)]
pub enum Command {
    Sessions {
        root: PathBuf,
    },
    Count {
        root: PathBuf,
        filter: ReplayFilter,
    },
    Head {
        root: PathBuf,
        filter: ReplayFilter,
        n: usize,
    },
    Tail {
        root: PathBuf,
        filter: ReplayFilter,
        n: usize,
    },
    Dump {
        root: PathBuf,
        filter: ReplayFilter,
        out: PathBuf,
    },
    Schema,
}

#[derive(Debug)]
pub enum CliError {
    Usage(String),
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CliError::Usage(s) => write!(f, "{s}"),
        }
    }
}

pub const USAGE: &str = "\
polybot replayer — research interface for recorder NDJSON output

USAGE:
    replayer <COMMAND> [OPTIONS]

COMMANDS:
    sessions   List sessions under --root
    count      Count events matching the filter
    head       Print the first -n events as NDJSON
    tail       Print the last -n events as NDJSON
    dump       Write filtered events to a Parquet file at --out
    schema     Print the Parquet export schema

OPTIONS:
    --root <PATH>          Session dir or base dir containing session_<UTC>/...
    --out <PATH>           Output path for `dump` (Parquet file).
    --venue <V>            Filter to venue (binance|polymarket|coinbase|chainlink). Repeatable.
    --stream <NAME>        Filter to exact stream name. Repeatable.
    --stream-prefix <P>    Filter to stream prefix (e.g. \"btcusdt@\"). Repeatable.
    --from <NS>            Inclusive lower bound on local_ts_ns.
    --to <NS>              Exclusive upper bound on local_ts_ns.
    -n <N>                 Number of events for head/tail (default 10).

EXAMPLES:
    replayer sessions --root ./data
    replayer count    --root ./data --venue binance
    replayer head     --root ./data/session_20260425T053013Z --stream-prefix btcusdt@ -n 3
    replayer dump     --root ./data --venue coinbase --out /tmp/cb.parquet
    replayer schema
";

pub fn parse() -> Result<Command, CliError> {
    let mut args = std::env::args().skip(1);
    let sub = args
        .next()
        .ok_or_else(|| CliError::Usage(USAGE.to_string()))?;
    let rest: Vec<String> = args.collect();
    let opts = Opts::parse(&rest)?;

    match sub.as_str() {
        "sessions" => Ok(Command::Sessions {
            root: opts.require_root()?,
        }),
        "count" => Ok(Command::Count {
            root: opts.require_root()?,
            filter: opts.into_filter()?,
        }),
        "head" => Ok(Command::Head {
            root: opts.require_root()?,
            n: opts.n.unwrap_or(10),
            filter: opts.into_filter()?,
        }),
        "tail" => Ok(Command::Tail {
            root: opts.require_root()?,
            n: opts.n.unwrap_or(10),
            filter: opts.into_filter()?,
        }),
        "dump" => {
            let root = opts.require_root()?;
            let out = opts
                .out
                .clone()
                .ok_or_else(|| CliError::Usage(format!("missing --out\n\n{USAGE}")))?;
            Ok(Command::Dump {
                root,
                out,
                filter: opts.into_filter()?,
            })
        }
        "schema" => Ok(Command::Schema),
        "--help" | "-h" | "help" => {
            Err(CliError::Usage(USAGE.to_string()))
        }
        other => Err(CliError::Usage(format!(
            "unknown command: {other:?}\n\n{USAGE}"
        ))),
    }
}

#[derive(Default)]
struct Opts {
    root: Option<PathBuf>,
    out: Option<PathBuf>,
    venues: Vec<String>,
    streams: Vec<String>,
    stream_prefixes: Vec<String>,
    from: Option<u128>,
    to: Option<u128>,
    n: Option<usize>,
}

impl Opts {
    fn parse(args: &[String]) -> Result<Self, CliError> {
        let mut o = Opts::default();
        let mut i = 0;
        while i < args.len() {
            let a = &args[i];
            let v = || {
                args.get(i + 1).cloned().ok_or_else(|| {
                    CliError::Usage(format!("missing value for {a}\n\n{USAGE}"))
                })
            };
            match a.as_str() {
                "--root" => {
                    o.root = Some(PathBuf::from(v()?));
                    i += 2;
                }
                "--out" => {
                    o.out = Some(PathBuf::from(v()?));
                    i += 2;
                }
                "--venue" => {
                    o.venues.push(v()?);
                    i += 2;
                }
                "--stream" => {
                    o.streams.push(v()?);
                    i += 2;
                }
                "--stream-prefix" => {
                    o.stream_prefixes.push(v()?);
                    i += 2;
                }
                "--from" => {
                    o.from = Some(parse_u128(&v()?, "--from")?);
                    i += 2;
                }
                "--to" => {
                    o.to = Some(parse_u128(&v()?, "--to")?);
                    i += 2;
                }
                "-n" => {
                    let n: usize = v()?
                        .parse()
                        .map_err(|_| CliError::Usage(format!("-n needs a positive integer\n\n{USAGE}")))?;
                    o.n = Some(n);
                    i += 2;
                }
                "--help" | "-h" => {
                    return Err(CliError::Usage(USAGE.to_string()));
                }
                other => {
                    return Err(CliError::Usage(format!(
                        "unknown option: {other:?}\n\n{USAGE}"
                    )));
                }
            }
        }
        Ok(o)
    }

    fn require_root(&self) -> Result<PathBuf, CliError> {
        self.root
            .clone()
            .ok_or_else(|| CliError::Usage(format!("missing --root\n\n{USAGE}")))
    }

    fn into_filter(self) -> Result<ReplayFilter, CliError> {
        let venues = if self.venues.is_empty() {
            None
        } else {
            let mut s = HashSet::new();
            for raw in &self.venues {
                s.insert(parse_venue(raw)?);
            }
            Some(s)
        };
        let streams = if self.streams.is_empty() {
            None
        } else {
            Some(self.streams.into_iter().collect::<HashSet<_>>())
        };
        Ok(ReplayFilter {
            venues,
            streams,
            stream_prefixes: self.stream_prefixes,
            from_ts_ns: self.from,
            to_ts_ns: self.to,
        })
    }
}

fn parse_u128(s: &str, flag: &str) -> Result<u128, CliError> {
    s.parse::<u128>()
        .map_err(|_| CliError::Usage(format!("{flag} needs a non-negative integer (got {s:?})")))
}

fn parse_venue(s: &str) -> Result<Venue, CliError> {
    match s.to_ascii_lowercase().as_str() {
        "binance" => Ok(Venue::Binance),
        "polymarket" => Ok(Venue::Polymarket),
        "coinbase" => Ok(Venue::Coinbase),
        "chainlink" => Ok(Venue::Chainlink),
        other => Err(CliError::Usage(format!(
            "unknown venue {other:?} (expected binance|polymarket|coinbase|chainlink)"
        ))),
    }
}
