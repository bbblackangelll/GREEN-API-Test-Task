import polars as pl
import numpy as np
from clickhouse_driver import Client
from datetime import datetime, timezone

client = Client(host='localhost', port=9000, database='ticks', password='crypto123')

ENTRY_THRESHOLD = -5
CONFIRM_MS = 200
ENTRY_DELAY_MS = 300   # НОВОЕ
EXIT_DELAY_MS = 300    # НОВОЕ
EXIT_SPREAD = -15
TIMEOUT_MS = 60 * 60 * 1000
ENTRY_BATCH_MS = 60 * 60 * 1000
COINEX_MAX_MOVE_PCT = 0.01
CONFIRM_MIN_SPREAD = 0
COINEX_LOOKBACK_MS = 2000
BINANCE_TREND_WINDOW_MS = 5000
BINANCE_SYMBOL = 'BTCUSDT'
SUMMARY_FILE = 'backtest_summary.txt'

# ─────────────────────────────────────────────
START, END = client.execute('''
    SELECT
        toUInt64(toUnixTimestamp64Milli(min(event_time))),
        toUInt64(toUnixTimestamp64Milli(max(event_time)))
    FROM ticks.spreads
''')[0]

print(f"Данные: {START} → {END}\\n")


def ms_to_str(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def coinex_snapshot_at(ts_ms, side='prev', max_gap_ms=None):
    ts_ms = int(ts_ms)

    if side == 'prev':
        where_cond = f"updated_at <= {ts_ms}"
        order = "ORDER BY updated_at DESC"
    else:
        where_cond = f"updated_at >= {ts_ms}"
        order = "ORDER BY updated_at ASC"

    snap = client.execute(f'''
        SELECT updated_at, bid_price, ask_price
        FROM ticks.coinex_btcusdt
        WHERE {where_cond}
        {order}
        LIMIT 1
    ''')

    if not snap:
        return None

    updated_at, bid_price, ask_price = snap[0]
    updated_at = int(updated_at)

    if max_gap_ms is not None and abs(updated_at - ts_ms) > int(max_gap_ms):
        return None

    return {
        'updated_at': updated_at,
        'bid': float(bid_price),
        'ask': float(ask_price),
    }


# ─────────────────────────────────────────────
# Binance тренд (5 секунд)
# ─────────────────────────────────────────────
def binance_trend_up(entry_ms):
    result = client.execute(f'''
        SELECT
            avgIf((bid_price + ask_price) / 2, event_time >= toDateTime64({entry_ms / 1000}, 3)) AS price_now,
            avgIf((bid_price + ask_price) / 2,
                event_time >= toDateTime64({(entry_ms - BINANCE_TREND_WINDOW_MS) / 1000}, 3)
                AND event_time < toDateTime64({entry_ms / 1000}, 3)
            ) AS price_5s_ago
        FROM ticks.binance_bbo
        WHERE event_time >= toDateTime64({(entry_ms - BINANCE_TREND_WINDOW_MS) / 1000}, 3)
          AND event_time <= toDateTime64({entry_ms / 1000}, 3)
          AND symbol = '{BINANCE_SYMBOL}'
    ''')

    if not result or result[0][0] is None or result[0][1] is None:
        return None

    return float(result[0][0]) > float(result[0][1])


# ─────────────────────────────────────────────
all_trades = []
current_t = START

while current_t < END:

    t0 = current_t
    t1 = t0 + ENTRY_BATCH_MS

    print(f"📥 Batch: {ms_to_str(t0)} → {ms_to_str(t1)}")

    spreads_raw = client.execute(f'''
        SELECT toUInt64(toUnixTimestamp64Milli(event_time)) as t, spread
        FROM ticks.spreads
        WHERE event_time > toDateTime64({t0 / 1000}, 3)
          AND event_time < toDateTime64({t1 / 1000}, 3)
        ORDER BY event_time
    ''')

    if not spreads_raw:
        current_t = t1
        continue

    df = pl.DataFrame(spreads_raw, schema=['t', 'spread'], orient='row')

    times = df['t'].to_numpy()
    spreads = df['spread'].to_numpy().astype(np.float32)

    # ─────────────────────────────────────────
    # ВХОД
    # ─────────────────────────────────────────
    prev = np.empty_like(spreads)
    prev[1:] = spreads[:-1]
    prev[0] = spreads[0]

    # Сигнал только на пересечении порога снизу вверх.
    sig_mask = (spreads > ENTRY_THRESHOLD) & (prev <= ENTRY_THRESHOLD)
    sig_idxs = np.where(sig_mask)[0]

    entry_idx = None
    entry_exec_ms = None
    entry_ask = None
    entry_signal_ms = None

    for idx in sig_idxs:
        confirm_t = times[idx] + CONFIRM_MS
        confirm_idx = np.searchsorted(times, confirm_t)

        if confirm_idx >= len(spreads):
            continue

        if spreads[confirm_idx] < CONFIRM_MIN_SPREAD:
            continue

        # 👉 теперь учитываем задержку входа
        candidate_entry_exec_ms = confirm_t + ENTRY_DELAY_MS

        # CoinEx снапшот на входе (по времени исполнения)
        entry_snapshot = coinex_snapshot_at(candidate_entry_exec_ms, side='prev')
        if not entry_snapshot:
            continue

        candidate_entry_ask = entry_snapshot['ask']

        # фильтр движения
        coinex_prev_raw = client.execute(f'''
            SELECT ask_price
            FROM ticks.coinex_btcusdt
            WHERE updated_at <= {candidate_entry_exec_ms - COINEX_LOOKBACK_MS}
            ORDER BY updated_at DESC
            LIMIT 1
        ''')

        coinex_prev = float(coinex_prev_raw[0][0]) if coinex_prev_raw else candidate_entry_ask
        change_pct = abs(candidate_entry_ask - coinex_prev) / coinex_prev * 100 if coinex_prev > 0 else 0

        if change_pct > COINEX_MAX_MOVE_PCT:
            continue

        # тренд
        trend = binance_trend_up(candidate_entry_exec_ms)
        if trend is None or not trend:
            continue

        entry_idx = confirm_idx
        entry_exec_ms = int(candidate_entry_exec_ms)
        entry_ask = candidate_entry_ask
        entry_bid = entry_snapshot['bid']
        entry_px_ms = entry_snapshot['updated_at']
        entry_signal_ms = int(times[idx])
        signal_spread = float(spreads[idx])
        break

    if entry_idx is None:
        current_t = t1
        continue

    # Для сигнала берем ближайшую предыдущую цену CoinEx.
    signal_snapshot = coinex_snapshot_at(entry_signal_ms, side='prev')
    if not signal_snapshot:
        current_t = t1
        continue

    # Фиксируем spread на момент фактического исполнения входа (а не на confirm_idx)
    entry_exec_idx = np.searchsorted(times, entry_exec_ms)
    if entry_exec_idx < len(times):
        spread_entry_ms = int(times[entry_exec_idx])
        spread_entry = float(spreads[entry_exec_idx])
    else:
        spread_after_entry = client.execute(f'''
            SELECT toUInt64(toUnixTimestamp64Milli(event_time)) as t, spread
            FROM ticks.spreads
            WHERE event_time >= toDateTime64({entry_exec_ms / 1000}, 3)
            ORDER BY event_time
            LIMIT 1
        ''')
        if spread_after_entry:
            spread_entry_ms = int(spread_after_entry[0][0])
            spread_entry = float(spread_after_entry[0][1])
        else:
            spread_entry_ms = int(times[-1])
            spread_entry = float(spreads[-1])

    # ─────────────────────────────────────────
    # ВЫХОД (сначала ищем сигнал)
    # ─────────────────────────────────────────
    exit_signal_ms = None
    exit_signal_spread = None

    # Важно: сигнал выхода ищем только ПОСЛЕ фактического входа.
    exit_candidates = np.where((times >= entry_exec_ms) & (spreads <= EXIT_SPREAD))[0]

    if len(exit_candidates) > 0:
        exit_idx = int(exit_candidates[0])
        exit_signal_ms = int(times[exit_idx])
        exit_signal_spread = float(spreads[exit_idx])
    else:
        search_t = max(int(times[-1]) + 1, entry_exec_ms + 1)
        found = False

        while search_t < entry_exec_ms + TIMEOUT_MS:

            next_batch = client.execute(f'''
                SELECT toUInt64(toUnixTimestamp64Milli(event_time)) as t, spread
                FROM ticks.spreads
                WHERE event_time > toDateTime64({search_t / 1000}, 3)
                  AND event_time < toDateTime64({(search_t + ENTRY_BATCH_MS) / 1000}, 3)
                ORDER BY event_time
            ''')

            if not next_batch:
                break

            df2 = pl.DataFrame(next_batch, schema=['t', 'spread'], orient='row')

            t2 = df2['t'].to_numpy()
            s2 = df2['spread'].to_numpy().astype(np.float32)

            idxs = np.where((t2 >= entry_exec_ms) & (s2 <= EXIT_SPREAD))[0]

            if len(idxs) > 0:
                exit_idx = int(idxs[0])
                exit_signal_ms = int(t2[exit_idx])
                exit_signal_spread = float(s2[exit_idx])
                found = True
                break

            search_t = t2[-1] + 1

        if not found:
            exit_signal_ms = entry_exec_ms + TIMEOUT_MS
            spread_at_timeout = client.execute(f'''
                SELECT toUInt64(toUnixTimestamp64Milli(event_time)) as t, spread
                FROM ticks.spreads
                WHERE event_time <= toDateTime64({exit_signal_ms / 1000}, 3)
                ORDER BY event_time DESC
                LIMIT 1
            ''')
            exit_signal_spread = float(spread_at_timeout[0][1]) if spread_at_timeout else None

    # Выход: сигнал по спреду + задержка исполнения
    exit_exec_ms = exit_signal_ms + EXIT_DELAY_MS

    # ─────────────────────────────────────────
    # ЦЕНЫ
    # ─────────────────────────────────────────
    exit_signal_snapshot = coinex_snapshot_at(exit_signal_ms, side='prev')
    if not exit_signal_snapshot:
        current_t = exit_exec_ms + 1
        continue

    # На выходе также используем ближайшую предыдущую цену CoinEx.
    exit_snapshot = coinex_snapshot_at(exit_exec_ms, side='prev')

    if not exit_snapshot:
        current_t = exit_exec_ms + 1
        continue

    exit_bid = exit_snapshot['bid']
    exit_ask = exit_snapshot['ask']
    exit_px_ms = exit_snapshot['updated_at']
    exit_signal_bid = exit_signal_snapshot['bid']
    exit_signal_ask = exit_signal_snapshot['ask']
    exit_signal_px_ms = exit_signal_snapshot['updated_at']

    spread_exit_raw = client.execute(f'''
        SELECT toUInt64(toUnixTimestamp64Milli(event_time)) as t, spread
        FROM ticks.spreads
        WHERE event_time >= toDateTime64({exit_exec_ms / 1000}, 3)
        ORDER BY event_time
        LIMIT 1
    ''')
    if spread_exit_raw:
        spread_exit_ms = int(spread_exit_raw[0][0])
        spread_exit = float(spread_exit_raw[0][1])
    else:
        spread_exit_ms = exit_signal_ms
        spread_exit = float(exit_signal_spread) if exit_signal_spread is not None else float('nan')

    profit_pct = (exit_bid - entry_ask) / entry_ask * 100
    hold_min = (exit_exec_ms - entry_exec_ms) / 60000

    all_trades.append({
        'signal_ms': entry_signal_ms,
        'entry_ms': entry_exec_ms,
        'exit_ms': exit_exec_ms,
        'signal_spread': signal_spread,
        'exit_signal_spread': exit_signal_spread,
        'exit_spread': spread_exit,
        'profit_pct': profit_pct,
        'hold_min': hold_min,
        'spread_entry': spread_entry,
        'signal_spread_ts': entry_signal_ms,
        'entry_spread_ts': spread_entry_ms,
        'exit_signal_spread_ts': exit_signal_ms,
        'exit_spread_ts': spread_exit_ms,
        'signal_coinex_ts': signal_snapshot['updated_at'],
        'signal_coinex_bid': signal_snapshot['bid'],
        'signal_coinex_ask': signal_snapshot['ask'],
        'entry_coinex_ts': entry_px_ms,
        'entry_coinex_bid': entry_bid,
        'entry_coinex_ask': entry_ask,
        'exit_signal_coinex_ts': exit_signal_px_ms,
        'exit_signal_coinex_bid': exit_signal_bid,
        'exit_signal_coinex_ask': exit_signal_ask,
        'exit_coinex_ts': exit_px_ms,
        'exit_coinex_bid': exit_bid,
        'exit_coinex_ask': exit_ask,
    })

    print(
        f"signal={ms_to_str(entry_signal_ms)} spread={signal_spread:.2f} "
        f"spread_ts={ms_to_str(entry_signal_ms)} coinex_ts={ms_to_str(signal_snapshot['updated_at'])} "
        f"bid={signal_snapshot['bid']:.2f} ask={signal_snapshot['ask']:.2f} | "
        f"entry={ms_to_str(entry_exec_ms)} spread={spread_entry:.2f} "
        f"spread_ts={ms_to_str(spread_entry_ms)} coinex_ts={ms_to_str(entry_px_ms)} "
        f"bid={entry_bid:.2f} ask={entry_ask:.2f} | "
        f"exit_signal={ms_to_str(exit_signal_ms)} spread={exit_signal_spread if exit_signal_spread is not None else float('nan'):.2f} "
        f"spread_ts={ms_to_str(exit_signal_ms)} coinex_ts={ms_to_str(exit_signal_px_ms)} "
        f"bid={exit_signal_bid:.2f} ask={exit_signal_ask:.2f} | "
        f"exit={ms_to_str(exit_exec_ms)} spread={spread_exit:.2f} "
        f"spread_ts={ms_to_str(spread_exit_ms)} coinex_ts={ms_to_str(exit_px_ms)} "
        f"bid={exit_bid:.2f} ask={exit_ask:.2f} | "
        f"profit={profit_pct:.4f}% hold={hold_min:.2f}m"
    )

    current_t = exit_exec_ms + 1

# ─────────────────────────────────────────────
# СТАТИСТИКА
# ─────────────────────────────────────────────
if all_trades:
    df = pl.DataFrame(all_trades)

    cum = df['profit_pct'].cum_sum().to_numpy()
    max_dd = float((np.maximum.accumulate(cum) - cum).max())

    print(f"\\nСделок: {len(df)}")
    print(f"Winrate: {(df['profit_pct'] > 0).mean():.2%}")
    print(f"Total: {df['profit_pct'].sum():.4f}%")
    print(f"Avg: {df['profit_pct'].mean():.4f}%")
    print(f"Max DD: {max_dd:.4f}%")
    print(f"Avg hold: {df['hold_min'].mean():.2f} мин")
else:
    print("Сделок нет")

config_lines = [
    "CONFIG:",
    f"ENTRY_THRESHOLD={ENTRY_THRESHOLD}",
    f"CONFIRM_MS={CONFIRM_MS}",
    f"ENTRY_DELAY_MS={ENTRY_DELAY_MS}",
    f"EXIT_DELAY_MS={EXIT_DELAY_MS}",
    f"EXIT_SPREAD={EXIT_SPREAD}",
    f"TIMEOUT_MS={TIMEOUT_MS}",
    f"ENTRY_BATCH_MS={ENTRY_BATCH_MS}",
    f"COINEX_MAX_MOVE_PCT={COINEX_MAX_MOVE_PCT}",
    f"CONFIRM_MIN_SPREAD={CONFIRM_MIN_SPREAD}",
    f"COINEX_LOOKBACK_MS={COINEX_LOOKBACK_MS}",
    f"BINANCE_TREND_WINDOW_MS={BINANCE_TREND_WINDOW_MS}",
    f"BINANCE_SYMBOL={BINANCE_SYMBOL}",
]

print("\n" + "\n".join(config_lines))

if all_trades:
    summary_lines = [
        f"Период: {ms_to_str(START)} → {ms_to_str(END)}",
        f"Сделок: {len(df)}",
        f"Winrate: {(df['profit_pct'] > 0).mean():.2%}",
        f"Total: {df['profit_pct'].sum():.4f}%",
        f"Avg: {df['profit_pct'].mean():.4f}%",
        f"Max DD: {max_dd:.4f}%",
        f"Avg hold: {df['hold_min'].mean():.2f} мин",
    ]
else:
    summary_lines = [
        f"Период: {ms_to_str(START)} → {ms_to_str(END)}",
        "Сделок: 0",
        "Сделок нет",
    ]

result_text = "\n".join(summary_lines + [""] + config_lines) + "\n"
with open(SUMMARY_FILE, 'w', encoding='utf-8') as f:
    f.write(result_text)

print(f"\nИтоговый результат сохранен в {SUMMARY_FILE}")
