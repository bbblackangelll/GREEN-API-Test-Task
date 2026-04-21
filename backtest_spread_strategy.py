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


# ─────────────────────────────────────────────
# Binance тренд (5 секунд)
# ─────────────────────────────────────────────
def binance_trend_up(entry_ms):
    result = client.execute(f'''
        SELECT
            avgIf((bid_price + ask_price) / 2, event_time >= toDateTime64({entry_ms / 1000}, 3)) AS price_now,
            avgIf((bid_price + ask_price) / 2,
                event_time >= toDateTime64({(entry_ms - 5000) / 1000}, 3)
                AND event_time < toDateTime64({entry_ms / 1000}, 3)
            ) AS price_5s_ago
        FROM ticks.binance_bbo
        WHERE event_time >= toDateTime64({(entry_ms - 5000) / 1000}, 3)
          AND event_time <= toDateTime64({entry_ms / 1000}, 3)
          AND symbol = 'BTCUSDT'
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

    sig_mask = (spreads >= ENTRY_THRESHOLD) & (prev < ENTRY_THRESHOLD)
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

        if spreads[confirm_idx] < 0:
            continue

        # 👉 теперь учитываем задержку входа
        candidate_entry_exec_ms = confirm_t + ENTRY_DELAY_MS

        # CoinEx цена
        prices_entry = client.execute(f'''
            SELECT ask_price
            FROM ticks.coinex_btcusdt
            WHERE updated_at >= {candidate_entry_exec_ms}
            ORDER BY updated_at
            LIMIT 1
        ''')

        if not prices_entry:
            continue

        candidate_entry_ask = float(prices_entry[0][0])
        if candidate_entry_ask == 0:
            continue

        # фильтр движения
        coinex_prev_raw = client.execute(f'''
            SELECT ask_price
            FROM ticks.coinex_btcusdt
            WHERE updated_at <= {candidate_entry_exec_ms - 2000}
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
        entry_signal_ms = int(times[idx])
        break

    if entry_idx is None:
        current_t = t1
        continue

    # Фиксируем spread на момент фактического исполнения входа (а не на confirm_idx)
    entry_exec_idx = np.searchsorted(times, entry_exec_ms)
    if entry_exec_idx < len(times):
        spread_entry = float(spreads[entry_exec_idx])
    else:
        spread_after_entry = client.execute(f'''
            SELECT spread
            FROM ticks.spreads
            WHERE event_time >= toDateTime64({entry_exec_ms / 1000}, 3)
            ORDER BY event_time
            LIMIT 1
        ''')
        spread_entry = float(spread_after_entry[0][0]) if spread_after_entry else float(spreads[-1])

    # ─────────────────────────────────────────
    # ВЫХОД (сначала ищем сигнал)
    # ─────────────────────────────────────────
    exit_signal_ms = None

    # Важно: сигнал выхода ищем только ПОСЛЕ фактического входа.
    exit_candidates = np.where((times >= entry_exec_ms) & (spreads <= EXIT_SPREAD))[0]

    if len(exit_candidates) > 0:
        exit_signal_ms = int(times[exit_candidates[0]])
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
                exit_signal_ms = int(t2[idxs[0]])
                found = True
                break

            search_t = t2[-1] + 1

        if not found:
            exit_signal_ms = entry_exec_ms + TIMEOUT_MS

    # 👉 теперь учитываем задержку выхода
    exit_exec_ms = exit_signal_ms + EXIT_DELAY_MS

    # Защита от нереалистичного удержания: выход не может исполниться раньше входа.
    if exit_exec_ms <= entry_exec_ms:
        exit_exec_ms = entry_exec_ms + EXIT_DELAY_MS

    # ─────────────────────────────────────────
    # ЦЕНЫ
    # ─────────────────────────────────────────
    exit_price = client.execute(f'''
        SELECT bid_price
        FROM ticks.coinex_btcusdt
        WHERE updated_at >= {exit_exec_ms}
        ORDER BY updated_at
        LIMIT 1
    ''')

    if not exit_price:
        current_t = exit_exec_ms + 1
        continue

    exit_bid = float(exit_price[0][0])

    profit_pct = (exit_bid - entry_ask) / entry_ask * 100
    hold_min = (exit_exec_ms - entry_exec_ms) / 60000

    all_trades.append({
        'entry_ms': entry_exec_ms,
        'exit_ms': exit_exec_ms,
        'profit_pct': profit_pct,
        'hold_min': hold_min,
        'spread_entry': spread_entry
    })

    print(
        f"[{ms_to_str(entry_exec_ms)} → {ms_to_str(exit_exec_ms)}] "
        f"signal={ms_to_str(entry_signal_ms)} spread={spread_entry:.2f} "
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
