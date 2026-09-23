use rust_core::hub::Hub;
use rust_core::ingest::{parse_trade_payload_counted, InvalidCounters};
use rust_core::stream::{combined_aggtrade_url, fetch_trading_symbols, parse_exchange_info, run_stream};
use std::sync::Arc;

fn arg(name: &str, default: &str) -> String {
    std::env::args().skip_while(|a| a != name).nth(1).unwrap_or_else(|| default.to_string())
}

#[tokio::main]
async fn main() {
    let demo_seconds: u64 = arg("--demo_seconds", "20").parse().unwrap_or(20);
    let symbols_arg = arg("--demo_symbols", "");
    let rest_base = arg("--rest_base", "https://fapi.binance.com");
    let ws_base = arg("--ws_base", "wss://fstream.binance.com");

    // 1. Universe: explicit symbols or top from exchangeInfo
    let mut hub = Hub::new(2, 4, 8);
    let symbols: Vec<String> = if symbols_arg.trim().is_empty() {
        match fetch_trading_symbols(&format!("{}/fapi/v1/exchangeInfo", rest_base)).await {
            Ok(v) => v.into_iter().take(10).map(|(s, _)| s).collect(),
            Err(e) => {
                eprintln!("exchangeInfo failed ({}), fallback BTCUSDT", e);
                vec!["BTCUSDT".to_string()]
            }
        }
    } else {
        symbols_arg.split(',').map(|s| s.trim().to_uppercase()).collect()
    };
    for s in &symbols {
        hub.subscribe(s);
    }
    for c in hub.cores.iter_mut() {
        c.active_mask = 0xF;
    }
    println!("subscribed: {:?}", symbols);

    // 2. Spawn workers
    let mut handles = Vec::new();
    for core_idx in 0..2 {
        let w = hub.spawn_worker(core_idx);
        handles.push(std::thread::Builder::new().name(format!("worker-{}", core_idx)).spawn(move || {
            let mut w = w;
            w.run();
            w
        }).unwrap());
    }

    // 3. WS live feed -> hub ingest (single async task owns hub producers via channel)
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let url = combined_aggtrade_url(&ws_base, &symbols);
    println!("connecting {}", url);
    tokio::spawn(async move {
        run_stream(url, |text: &str| {
            // combined stream wraps: {"stream":"...","data":{...}} — unwrap data
            let payload = if text.contains("\"data\"") {
                match serde_json::from_str::<serde_json::Value>(text).ok().and_then(|v| v.get("data").cloned()) {
                    Some(d) => d.to_string(),
                    None => text.to_string(),
                }
            } else {
                text.to_string()
            };
            let _ = tx.send(payload);
        })
        .await;
    });

    // 4. Pump channel -> hub (Binance aggTrade uses p/q/T/s/m fields already)
    // price scale 1e2 (cents), qty scale 1e8 (satoshi-like) so small qtys stay >0
    let t0 = std::time::Instant::now();
    let mut n: u64 = 0;
    let mut ctr = InvalidCounters::default();
    let mut last_log = std::time::Instant::now();
    let mut seen_raw = false;
    let timeout = std::time::Duration::from_secs(demo_seconds);
    while t0.elapsed() < timeout {
        let payload = match tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
            Ok(Some(p)) => p,
            _ => continue,
        };
        if !seen_raw {
            seen_raw = true;
            println!("first payload: {}", &payload[..payload.len().min(300)]);
        }
        let mut t = parse_trade_payload_counted(&payload, 1_000_000, Some(&mut ctr));
        if !t.valid {
            continue;
        }
        match hub.ingest(&mut t) {
            Some(_) => n += 1,
            None => ctr.unknown_symbol += 1, // unknown symbol or queue full (rare)
        }
        if last_log.elapsed() >= std::time::Duration::from_secs(10) {
            last_log = std::time::Instant::now();
            eprintln!(
                "routed={} drop{{sym={} price={} qty={} ts={} noroute={}}}",
                n, ctr.bad_symbol, ctr.bad_price, ctr.bad_qty, ctr.bad_ts, ctr.unknown_symbol
            );
        }
    }
    println!("live trades routed: {}", n);

    hub.stop_workers();
    for h in handles {
        if let Ok(w) = h.join() {
            eprintln!("worker {} slot0: n={} vol={}", w.core_id, w.core.slots[0].count, w.core.slots[0].vol);
            let slot = &w.core.slots[0];
            if slot.count > 0 {
                hub.closer.accept(rust_core::closer::ClosedBar::from_slot(w.core_id as usize, slot, 0));
            }
        } else {
            eprintln!("worker thread panicked/failed");
        }
    }
    println!("handoff(closed): {}", hub.closer.handoff_count);
    if let Some(last) = hub.closer.last(0) {
        println!("bar[0]: O={} H={} L={} C={} vol={} n={}", last.open, last.high, last.low, last.close, last.vol, last.count);
    }
    let _ = parse_exchange_info("{}");
    let _ = Arc::new(());
}
use std::time::Duration;
