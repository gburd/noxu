/// Parsed memcache text protocol commands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Set {
        key: Vec<u8>,
        flags: u32,
        exptime: i64,
        data: Vec<u8>,
        noreply: bool,
    },
    Add {
        key: Vec<u8>,
        flags: u32,
        exptime: i64,
        data: Vec<u8>,
        noreply: bool,
    },
    Replace {
        key: Vec<u8>,
        flags: u32,
        exptime: i64,
        data: Vec<u8>,
        noreply: bool,
    },
    Append {
        key: Vec<u8>,
        flags: u32,
        exptime: i64,
        data: Vec<u8>,
        noreply: bool,
    },
    Prepend {
        key: Vec<u8>,
        flags: u32,
        exptime: i64,
        data: Vec<u8>,
        noreply: bool,
    },
    Cas {
        key: Vec<u8>,
        flags: u32,
        exptime: i64,
        data: Vec<u8>,
        cas_token: u64,
        noreply: bool,
    },
    Get {
        keys: Vec<Vec<u8>>,
    },
    Gets {
        keys: Vec<Vec<u8>>,
    },
    Delete {
        key: Vec<u8>,
        noreply: bool,
    },
    Incr {
        key: Vec<u8>,
        value: u64,
        noreply: bool,
    },
    Decr {
        key: Vec<u8>,
        value: u64,
        noreply: bool,
    },
    Stats,
    FlushAll {
        noreply: bool,
    },
    Quit,
}

/// Result of attempting to parse a command line.
#[derive(Debug)]
pub enum ParseResult {
    /// Command fully parsed (no data block needed or data already provided).
    Complete(Command),
    /// Need to read a data block of the specified number of bytes.
    NeedData(PendingStorage),
    /// Parse error — send ERROR or CLIENT_ERROR to the client.
    Error(String),
}

/// A storage command awaiting its data block.
#[derive(Debug)]
pub struct PendingStorage {
    pub kind: StorageKind,
    pub key: Vec<u8>,
    pub flags: u32,
    pub exptime: i64,
    pub bytes: usize,
    pub cas_token: Option<u64>,
    pub noreply: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageKind {
    Set,
    Add,
    Replace,
    Append,
    Prepend,
    Cas,
}

/// Parse the command line (without trailing \r\n).
/// For storage commands, this returns `NeedData` indicating the caller must
/// read the data block next.
pub fn parse_command_line(line: &[u8]) -> ParseResult {
    let line_str = match std::str::from_utf8(line) {
        Ok(s) => s,
        Err(_) => return ParseResult::Error("CLIENT_ERROR bad command line encoding\r\n".into()),
    };

    let parts: Vec<&str> = line_str.split_whitespace().collect();
    if parts.is_empty() {
        return ParseResult::Error("ERROR\r\n".into());
    }

    match parts[0] {
        "get" => parse_get(&parts),
        "gets" => parse_gets(&parts),
        "delete" => parse_delete(&parts),
        "incr" => parse_incr_decr(&parts, true),
        "decr" => parse_incr_decr(&parts, false),
        "stats" => ParseResult::Complete(Command::Stats),
        "flush_all" => parse_flush_all(&parts),
        "quit" => ParseResult::Complete(Command::Quit),
        "set" => parse_storage(&parts, StorageKind::Set),
        "add" => parse_storage(&parts, StorageKind::Add),
        "replace" => parse_storage(&parts, StorageKind::Replace),
        "append" => parse_storage(&parts, StorageKind::Append),
        "prepend" => parse_storage(&parts, StorageKind::Prepend),
        "cas" => parse_storage(&parts, StorageKind::Cas),
        _ => ParseResult::Error("ERROR\r\n".into()),
    }
}

/// Complete a pending storage command with its data block.
pub fn complete_storage(pending: PendingStorage, data: Vec<u8>) -> Command {
    match pending.kind {
        StorageKind::Set => Command::Set {
            key: pending.key,
            flags: pending.flags,
            exptime: pending.exptime,
            data,
            noreply: pending.noreply,
        },
        StorageKind::Add => Command::Add {
            key: pending.key,
            flags: pending.flags,
            exptime: pending.exptime,
            data,
            noreply: pending.noreply,
        },
        StorageKind::Replace => Command::Replace {
            key: pending.key,
            flags: pending.flags,
            exptime: pending.exptime,
            data,
            noreply: pending.noreply,
        },
        StorageKind::Append => Command::Append {
            key: pending.key,
            flags: pending.flags,
            exptime: pending.exptime,
            data,
            noreply: pending.noreply,
        },
        StorageKind::Prepend => Command::Prepend {
            key: pending.key,
            flags: pending.flags,
            exptime: pending.exptime,
            data,
            noreply: pending.noreply,
        },
        StorageKind::Cas => Command::Cas {
            key: pending.key,
            flags: pending.flags,
            exptime: pending.exptime,
            data,
            cas_token: pending.cas_token.unwrap_or(0),
            noreply: pending.noreply,
        },
    }
}

fn parse_get(parts: &[&str]) -> ParseResult {
    if parts.len() < 2 {
        return ParseResult::Error("CLIENT_ERROR missing key\r\n".into());
    }
    let keys = parts[1..].iter().map(|k| k.as_bytes().to_vec()).collect();
    ParseResult::Complete(Command::Get { keys })
}

fn parse_gets(parts: &[&str]) -> ParseResult {
    if parts.len() < 2 {
        return ParseResult::Error("CLIENT_ERROR missing key\r\n".into());
    }
    let keys = parts[1..].iter().map(|k| k.as_bytes().to_vec()).collect();
    ParseResult::Complete(Command::Gets { keys })
}

fn parse_delete(parts: &[&str]) -> ParseResult {
    if parts.len() < 2 {
        return ParseResult::Error("CLIENT_ERROR missing key\r\n".into());
    }
    let noreply = parts.get(2).is_some_and(|&s| s == "noreply");
    ParseResult::Complete(Command::Delete {
        key: parts[1].as_bytes().to_vec(),
        noreply,
    })
}

fn parse_incr_decr(parts: &[&str], is_incr: bool) -> ParseResult {
    if parts.len() < 3 {
        return ParseResult::Error("CLIENT_ERROR missing arguments\r\n".into());
    }
    let key = parts[1].as_bytes().to_vec();
    let value = match parts[2].parse::<u64>() {
        Ok(v) => v,
        Err(_) => {
            return ParseResult::Error(
                "CLIENT_ERROR invalid numeric delta argument\r\n".into(),
            );
        }
    };
    let noreply = parts.get(3).is_some_and(|&s| s == "noreply");
    if is_incr {
        ParseResult::Complete(Command::Incr {
            key,
            value,
            noreply,
        })
    } else {
        ParseResult::Complete(Command::Decr {
            key,
            value,
            noreply,
        })
    }
}

fn parse_flush_all(parts: &[&str]) -> ParseResult {
    let noreply = parts.get(1).is_some_and(|&s| s == "noreply")
        || parts.get(2).is_some_and(|&s| s == "noreply");
    ParseResult::Complete(Command::FlushAll { noreply })
}

fn parse_storage(parts: &[&str], kind: StorageKind) -> ParseResult {
    // Format: <cmd> <key> <flags> <exptime> <bytes> [cas_token] [noreply]\r\n
    let min_parts = if kind == StorageKind::Cas { 6 } else { 5 };
    if parts.len() < min_parts {
        return ParseResult::Error("CLIENT_ERROR bad command line format\r\n".into());
    }

    let key = parts[1].as_bytes().to_vec();

    let flags = match parts[2].parse::<u32>() {
        Ok(v) => v,
        Err(_) => return ParseResult::Error("CLIENT_ERROR bad flags value\r\n".into()),
    };

    let exptime = match parts[3].parse::<i64>() {
        Ok(v) => v,
        Err(_) => return ParseResult::Error("CLIENT_ERROR bad exptime value\r\n".into()),
    };

    let bytes = match parts[4].parse::<usize>() {
        Ok(v) => v,
        Err(_) => return ParseResult::Error("CLIENT_ERROR bad data length\r\n".into()),
    };

    let (cas_token, noreply) = if kind == StorageKind::Cas {
        let cas = match parts[5].parse::<u64>() {
            Ok(v) => v,
            Err(_) => return ParseResult::Error("CLIENT_ERROR bad cas value\r\n".into()),
        };
        let nr = parts.get(6).is_some_and(|&s| s == "noreply");
        (Some(cas), nr)
    } else {
        let nr = parts.get(5).is_some_and(|&s| s == "noreply");
        (None, nr)
    };

    ParseResult::NeedData(PendingStorage {
        kind,
        key,
        flags,
        exptime,
        bytes,
        cas_token,
        noreply,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_get() {
        match parse_command_line(b"get foo bar") {
            ParseResult::Complete(Command::Get { keys }) => {
                assert_eq!(keys.len(), 2);
                assert_eq!(keys[0], b"foo");
                assert_eq!(keys[1], b"bar");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_parse_set() {
        match parse_command_line(b"set mykey 0 3600 5") {
            ParseResult::NeedData(pending) => {
                assert_eq!(pending.kind, StorageKind::Set);
                assert_eq!(pending.key, b"mykey");
                assert_eq!(pending.flags, 0);
                assert_eq!(pending.exptime, 3600);
                assert_eq!(pending.bytes, 5);
                assert!(!pending.noreply);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_parse_cas() {
        match parse_command_line(b"cas mykey 0 0 5 12345 noreply") {
            ParseResult::NeedData(pending) => {
                assert_eq!(pending.kind, StorageKind::Cas);
                assert_eq!(pending.cas_token, Some(12345));
                assert!(pending.noreply);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_parse_delete() {
        match parse_command_line(b"delete foo") {
            ParseResult::Complete(Command::Delete { key, noreply }) => {
                assert_eq!(key, b"foo");
                assert!(!noreply);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_parse_incr() {
        match parse_command_line(b"incr counter 10") {
            ParseResult::Complete(Command::Incr {
                key,
                value,
                noreply,
            }) => {
                assert_eq!(key, b"counter");
                assert_eq!(value, 10);
                assert!(!noreply);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_parse_quit() {
        match parse_command_line(b"quit") {
            ParseResult::Complete(Command::Quit) => {}
            other => panic!("unexpected: {other:?}"),
        }
    }
}
