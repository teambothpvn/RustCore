//! Producer side: parse raw Binance trade payload -> normalized Trade.
//! Raw Binance aggTrade/miniTicker payload fields we care about:
//!   s: symbol "BTCUSDT", p: price str, q: qty str, T: event ms, m: isBuyerMaker.
//! Rule: symbol s isHashed at ingest to u32 id; dense id stamped from Directory snapshot.
//! No String/HashMap allocation on the steady path after intern (see directory.rs).

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct Trade {
    pub symbol_hash: u32,
    pub dense_id: u32,
    pub price: i64, // scaled fixed-point (tick-scaled)
    pub qty: i64,
    pub ts_ns: i64,
    pub trade_id: u64, // Binance `t`; 0 if missing (gap detection downstream)
    pub is_buyer_maker: bool,
    pub valid: bool,
}

pub const EMPTY_TRADE: Trade = Trade {
    symbol_hash: 0,
    dense_id: 0,
    price: 0,
    qty: 0,
    ts_ns: 0,
    trade_id: 0,
    is_buyer_maker: false,
    valid: false,
};

/// Mirror of C++ symbol_key_valid (bn_stream_3): only A-Z 0-9 _ . - allowed,
/// length 1..=32. Blocks quote/backslash injection via dirty symbols.
#[inline]
pub fn symbol_key_valid(bytes: &[u8]) -> bool {
    if bytes.is_empty() || bytes.len() > 32 {
        return false;
    }
    for &b in bytes {
        match b {
            b'A'..=b'Z' | b'0'..=b'9' | b'_' | b'.' | b'-' => {}
            _ => return false,
        }
    }
    true
}

/// Drop counters (mirror of C++ n_invalid diagnostics). Pumped to stderr ~1 line/10s.
#[derive(Default, Debug)]
pub struct InvalidCounters {
    pub bad_symbol: u64,
    pub bad_price: u64,
    pub bad_qty: u64,
    pub bad_ts: u64,
    pub unknown_symbol: u64,
}

#[inline(always)]
pub fn hash_symbol(bytes: &[u8]) -> u32 {
    // FNV-1a 32: fast, deterministic, good for short ASCII symbols.
    let mut h: u32 = 0x811c_9dc5;
    for &b in bytes {
        h ^= b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h | 1 // never 0; 0 = invalid sentinel
}

/// Parse scaled decimal "123.45" with given scale (e.g. scale=1 -> store 1234 for tick 0.1).
/// Returns None on invalid / non-positive (boundary validation: drop hop_le).
#[inline]
pub fn parse_decimal_scaled(s: &str, scale: i64) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    let mut neg = false;
    let mut int: i64 = 0;
    let mut frac: i64 = 0;
    let mut frac_digits = 0;
    let mut seen_dot = false;
    let mut any = false;
    for &b in s.as_bytes() {
        match b {
            b'-' if !any && !neg => neg = true,
            b'0'..=b'9' => {
                any = true;
                if !seen_dot {
                    int = int.checked_mul(10)?.checked_add((b - b'0') as i64)?;
                } else if frac_digits < 9 {
                    frac = frac * 10 + (b - b'0') as i64;
                    frac_digits += 1;
                }
            }
            b'.' if !seen_dot => seen_dot = true,
            _ => return None,
        }
    }
    if !any {
        return None;
    }
    // value = int + frac / 10^frac_digits ; scaled = value * scale
    let mut pow10: i64 = 1;
    for _ in 0..frac_digits {
        pow10 *= 10;
    }
    let scaled = int.checked_mul(scale)?.checked_add(frac.checked_mul(scale)? / pow10.max(1))?;
    let v = if neg { -scaled } else { scaled };
    if v <= 0 {
        return None; // trade_hop_le: drop non-positive
    }
    Some(v)
}

/// Global fixed-point scale: 1.0 == 1_000_000 units.
/// Mirrors C++ `scale` (power of 10): price, qty AND tick are ALL in this unit.
/// Never mix: tick from exchange (e.g. 0.10) must go through `tick_to_scaled`.
pub const SCALE: i64 = 1_000_000;

/// Convert tickSize string ("0.10") to scaled int in SCALE units.
/// tickSize 0.10 -> 100_000. Same parse path as price (no float anywhere).
#[inline]
pub fn tick_to_scaled(tick_size: &str) -> Option<i64> {
    parse_decimal_scaled(tick_size, SCALE)
}

/// Minimal JSON field extractor without a JSON lib (hot path).
/// Finds `"key":` then reads a string or bare value. Returns slice into input.
fn field<'a>(json: &'a str, key: &str) -> Option<&'a str> {
    let pat = format!("\"{}\":", key);
    let i = json.find(&pat)? + pat.len();
    let rest = json[i..].trim_start();
    if let Some(r) = rest.strip_prefix('"') {
        let end = r.find('"')?;
        Some(&r[..end])
    } else {
        let end = rest.find(|c| c == ',' || c == '}').unwrap_or(rest.len());
        Some(rest[..end].trim())
    }
}

/// Parse one raw Binance trade JSON message.
/// `scale` converts price/qty strings to tick-scaled ints. `ts_ns` in ns.
/// Validation order mirrors C++: symbol charset -> price -> qty -> ts.
pub fn parse_trade_payload(json: &str, scale: i64) -> Trade {
    parse_trade_payload_counted(json, scale, None)
}

/// Same + bump `InvalidCounters` instead of dropping silently.
pub fn parse_trade_payload_counted(
    json: &str,
    scale: i64,
    mut ctr: Option<&mut InvalidCounters>,
) -> Trade {
    let s = match field(json, "s") {
        Some(v) if symbol_key_valid(v.as_bytes()) => v,
        _ => {
            if let Some(c) = ctr.as_deref_mut() {
                c.bad_symbol += 1;
            }
            return EMPTY_TRADE;
        }
    };
    let p = match field(json, "p").and_then(|v| parse_decimal_scaled(v, scale)) {
        Some(v) => v,
        None => {
            if let Some(c) = ctr.as_deref_mut() {
                c.bad_price += 1;
            }
            return EMPTY_TRADE;
        }
    };
    let q = match field(json, "q").and_then(|v| parse_decimal_scaled(v, scale)) {
        Some(v) => v,
        None => {
            if let Some(c) = ctr.as_deref_mut() {
                c.bad_qty += 1;
            }
            return EMPTY_TRADE;
        }
    };
    let t_ms: i64 = field(json, "T").and_then(|v| v.parse().ok()).unwrap_or(-1);
    if t_ms <= 0 {
        if let Some(c) = ctr.as_deref_mut() {
            c.bad_ts += 1;
        }
        return EMPTY_TRADE;
    }
    let m = field(json, "m").map(|v| v == "true").unwrap_or(false);
    let tid: u64 = field(json, "t").and_then(|v| v.parse().ok()).unwrap_or(0);
    Trade {
        symbol_hash: hash_symbol(s.as_bytes()),
        dense_id: u32::MAX, // stamped later by Directory; MAX = unstamped sentinel
        price: p,
        qty: q,
        ts_ns: t_ms * 1_000_000,
        trade_id: tid,
        is_buyer_maker: m,
        valid: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parse_ok_and_drop_invalid() {
        let j = r#"{"s":"BTCUSDT","p":"67500.5","q":"2.5","T":1710000000000,"m":false,"t":99}"#;
        let t = parse_trade_payload(j, 10);
        assert!(t.valid && t.price > 0 && t.qty > 0);
        assert_eq!(t.ts_ns, 1710000000000 * 1_000_000);
        assert_eq!(t.trade_id, 99);
        let bad = r#"{"s":"BTCUSDT","p":"0","q":"1","T":1,"m":false}"#;
        assert!(!parse_trade_payload(bad, 10).valid);
        // injection symbol rejected
        let inj = r#"{"s":"BTC\"USDT","p":"1","q":"1","T":1,"m":false}"#;
        let mut ctr = InvalidCounters::default();
        assert!(!parse_trade_payload_counted(inj, 10, Some(&mut ctr)).valid);
        assert_eq!(ctr.bad_symbol, 1);
    }
}
