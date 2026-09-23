//! Exchange: load symbol universe (exchangeInfo) -> subscribe all to Hub.
//! Cold path only. Tick/scale table kept here, never touched per trade.

use crate::hub::Hub;

pub struct SymbolInfo {
    pub symbol: String,
    pub tick: i64,
    pub scale: i64,
}

impl SymbolInfo {
    pub fn new(symbol: &str, tick: i64) -> Self {
        Self { symbol: symbol.to_string(), tick: tick.max(1), scale: tick.max(1) }
    }
}

/// Minimal exchangeInfo parser: extracts ["symbols", {symbol, tickSize}] without serde.
/// Accepts simplified JSON: {"symbols":[{"s":"BTCUSDT","tick":100000}, ...]}
/// tick is in SCALE units (0.10 -> 100_000). Same unit as price/qty.
pub fn load_exchange_info(json: &str) -> Vec<SymbolInfo> {
    let mut out = Vec::new();
    let mut rest = json;
    while let Some(i) = rest.find("\"s\"") {
        rest = &rest[i + 3..];
        let start = match rest.find('"') {
            Some(p) => p + 1,
            None => break,
        };
        let end = match rest[start..].find('"') {
            Some(p) => start + p,
            None => break,
        };
        let sym = &rest[start..end];
        rest = &rest[end..];
        let tick: i64 = rest
            .find("\"tick\"")
            .and_then(|p| {
                let r = &rest[p + 6..];
                let s = r.trim_start_matches(|c| c == ':' || c == ' ' || c == '"');
                let e = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
                s[..e].parse().ok()
            })
            .unwrap_or(10);
        out.push(SymbolInfo::new(sym, tick));
        if out.len() >= 4096 {
            break;
        }
    }
    out
}

pub fn subscribe_all(hub: &mut Hub, infos: &[SymbolInfo]) -> Vec<u32> {
    infos.iter().map(|s| hub.subscribe(&s.symbol)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn load_and_subscribe() {
        let j = r#"{"symbols":[{"s":"BTCUSDT","tick":10},{"s":"ETHUSDT","tick":10}]}"#;
        let infos = load_exchange_info(j);
        assert_eq!(infos.len(), 2);
        let mut hub = Hub::new(2, 4, 4);
        let ids = subscribe_all(&mut hub, &infos);
        assert_eq!(ids.len(), 2);
        assert_ne!(ids[0], ids[1]);
    }
}
