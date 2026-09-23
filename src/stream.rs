//! Live Binance connectivity: exchangeInfo REST + WS aggTrade feed.
//! Entry wiring: load symbols -> subscribe hub -> stream -> Router::dispatch.

use futures_util::StreamExt;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct ExchangeInfo {
    symbols: Vec<SymInfo>,
}
#[derive(Debug, Deserialize)]
struct SymInfo {
    symbol: String,
    status: String,
    #[serde(default)]
    tickSize: Option<String>,
    #[serde(default)]
    pricePrecision: Option<i32>,
}

pub async fn fetch_trading_symbols(url: &str) -> anyhow_lite::Result<Vec<(String, i64)>> {
    let body = reqwest::get(url).await?.text().await?;
    Ok(parse_exchange_info(&body))
}

pub fn parse_exchange_info(body: &str) -> Vec<(String, i64)> {
    // tolerant parse: real /fapi/v1/exchangeInfo has symbols[].filters[] PRICE_FILTER tickSize
    let v: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    let mut out = Vec::new();
    if let Some(arr) = v.get("symbols").and_then(|s| s.as_array()) {
        for s in arr {
            let name = s.get("symbol").and_then(|x| x.as_str()).unwrap_or("");
            let status = s.get("status").and_then(|x| x.as_str()).unwrap_or("");
            if status != "TRADING" || name.is_empty() {
                continue;
            }
            // tickSize from filters PRICE_FILTER, SCALE units (0.10 -> 100_000)
            let mut tick: i64 = 100_000;
            if let Some(filters) = s.get("filters").and_then(|f| f.as_array()) {
                for f in filters {
                    if f.get("filterType").and_then(|x| x.as_str()) == Some("PRICE_FILTER") {
                        if let Some(ts) = f.get("tickSize").and_then(|x| x.as_str()) {
                            tick = tick_size_to_scale(ts).unwrap_or(100_000);
                        }
                    }
                }
            }
            out.push((name.to_string(), tick));
            if out.len() >= 4096 {
                break;
            }
        }
    }
    let _ = (ExchangeInfo { symbols: vec![] }, SymInfo { symbol: String::new(), status: String::new(), tickSize: None, pricePrecision: None });
    out
}

fn tick_size_to_scale(tick: &str) -> Option<i64> {
    // SCALE units (mirror C++): tickSize "0.10" -> 100_000.
    // Same unit as price/qty — never 1/tick, never float.
    crate::ingest::tick_to_scaled(tick)
}

/// Build combined trade stream URL: wss://.../stream?streams=btcusdt@trade/ethusdt@trade
pub fn combined_aggtrade_url(base: &str, symbols: &[String]) -> String {
    let streams: Vec<String> =
        symbols.iter().map(|s| format!("{}@trade", s.to_lowercase())).collect();
    format!("{}/stream?streams={}", base.trim_end_matches('/'), streams.join("/"))
}

/// Live loop: connect WS, read messages, call `on_text` per payload. Auto-reconnect with backoff.
pub async fn run_stream(url: String, mut on_text: impl FnMut(&str) + Send) -> ! {
    let mut backoff = std::time::Duration::from_secs(1);
    loop {
        match tokio_tungstenite::connect_async(&url).await {
            Ok((ws, _)) => {
                eprintln!("ws connected: {}", url);
                backoff = std::time::Duration::from_secs(1);
                let (_, mut read) = ws.split();
                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(m) if m.is_text() => {
                            on_text(&m.into_text().unwrap_or_default());
                        }
                        Ok(m) if m.is_close() => break,
                        Err(e) => {
                            eprintln!("ws read error: {}", e);
                            break;
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                eprintln!("ws connect error: {}", e);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(std::time::Duration::from_secs(30));
                continue;
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(std::time::Duration::from_secs(30));
    }
}

pub mod anyhow_lite {
    #[derive(Debug)]
    pub struct Error(pub String);
    impl std::fmt::Display for Error {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }
    impl std::error::Error for Error {}
    pub type Result<T> = std::result::Result<T, Error>;
    impl From<reqwest::Error> for Error {
        fn from(e: reqwest::Error) -> Self {
            Error(e.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parse_info_and_url() {
        let body = r#"{"symbols":[
          {"symbol":"BTCUSDT","status":"TRADING","filters":[{"filterType":"PRICE_FILTER","tickSize":"0.10"}]},
          {"symbol":"OLD","status":"BREAK","filters":[]}]}"#;
        let v = parse_exchange_info(body);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].0, "BTCUSDT");
        let url = combined_aggtrade_url("wss://fstream.binance.com", &["BTCUSDT".into(), "ETHUSDT".into()]);
        assert!(url.contains("btcusdt@trade"));
    }
}
