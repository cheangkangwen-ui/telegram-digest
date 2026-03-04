import os
import time
import anthropic
from concurrent.futures import ThreadPoolExecutor, as_completed
from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import Channel
from datetime import datetime, timezone, timedelta

TELEGRAM_API_ID = 33919151
TELEGRAM_API_HASH = "dd0a935bd6545cf56910292ff4445c4e"
TELEGRAM_SESSION = os.environ.get("TELEGRAM_SESSION", "my_session")

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

MAX_WORKERS = 20  # parallel channel analyses


def get_time_window():
    myt = timezone(timedelta(hours=8))
    now = datetime.now(myt)
    hour = now.hour

    if 8 < hour < 22:
        start = now - timedelta(hours=1)
        label = f"Last 1 hour ({start.strftime('%H:%M')} - {now.strftime('%H:%M')} MYT)"
    else:
        overnight_start = now.replace(hour=22, minute=0, second=0, microsecond=0)
        if hour <= 8:
            overnight_start -= timedelta(days=1)
        start = overnight_start
        label = f"Overnight ({start.strftime('%Y-%m-%d %H:%M')} - {now.strftime('%H:%M')} MYT)"

    return start.astimezone(timezone.utc), label


def is_financial_channel(client_ai, channel_name, sample_text):
    prompt = f"""Channel name: "{channel_name}"
Sample messages:
{sample_text[:600]}

Does this channel contain ANY financial, market, economic, geopolitical, crypto, stocks, commodities, macro, or business news that could be relevant to investors or traders? When in doubt, answer YES. Only answer NO if the channel is clearly about non-financial topics like entertainment, sports, personal lifestyle, or university events. Answer only YES or NO."""

    response = client_ai.messages.create(
        model="claude-haiku-4-5",
        max_tokens=5,
        messages=[{"role": "user", "content": prompt}]
    )
    return response.content[0].text.strip().upper().startswith("YES")


def analyze_channel(client_ai, channel_name, messages_text):
    prompt = f"""You are a financial analyst. Extract all market-relevant information from these Telegram messages (channel: "{channel_name}").

MESSAGES:
{messages_text}

Return a structured extract with:

1. **Assets mentioned** (list every asset, ticker, or market covered — be specific: "AAPL", "BTC", "KOSPI", "Gold", "Crude Oil", etc.)

2. **News items** (one bullet per distinct development — include ALL of them, nothing omitted):
   Format each as: [ASSET] — [what happened] — [key figure/number if any]

3. **Divergence alerts** (only where news meaningfully contradicts the asset's known fundamentals):
   [ASSET] | [news] | [what fundamentals say] | [severity: LOW/MEDIUM/HIGH/EXTREME] | [POSITIVE/NEGATIVE]

Be exhaustive on news items. Include every development mentioned, even briefly."""

    for attempt in range(3):
        try:
            # Use Sonnet for per-channel analysis — faster and cheaper
            with client_ai.messages.stream(
                model="claude-sonnet-4-6",
                max_tokens=2048,
                messages=[{"role": "user", "content": prompt}]
            ) as stream:
                return stream.get_final_message()
        except Exception as e:
            if attempt < 2:
                time.sleep(3)
            else:
                raise


def process_channel(client_ai, channel_name, messages_text):
    """Wrapper for parallel execution — returns (name, analysis) or None on failure."""
    try:
        response = analyze_channel(client_ai, channel_name, messages_text)
        for block in response.content:
            if block.type == "text":
                return (channel_name, block.text.strip())
    except Exception as e:
        print(f"  [FAILED] {channel_name}: {type(e).__name__}")
    return None


def generate_digest(client_ai, channel_analyses, label):
    combined = "\n\n".join(
        f"=== {name} ===\n{analysis}"
        for name, analysis in channel_analyses
    )

    prompt = f"""You are a senior financial analyst. Below are raw analyses from multiple financial Telegram channels covering the time window: {label}.

{combined}

Your job is to AGGREGATE this into a unified digest — NOT a channel-by-channel report. Multiple channels often cover the same story; merge and deduplicate them into single entries. Prioritize depth over breadth.

Produce the digest in these sections:

1. **Top Stories** (5-8 biggest developments, aggregated across all sources. Each story should consolidate everything multiple channels said about it. Include specific numbers, prices, percentages. Format:
   ### [Story Title]
   [Full aggregated summary — 3-6 sentences combining all angles covered])

2. **Asset-by-Asset Breakdown** (group ALL news by asset, not by channel. Deduplicate — if 3 channels covered the same story, write it once. Include every asset that had any news. Format:
   ### [Asset Name]
   - [development 1]
   - [development 2]
   ...)

3. **Divergence Watchlist** (HIGH and EXTREME only, ranked by severity):
   Asset | What happened | What fundamentals say | Severity | Direction

4. **Market Sentiment** (1 paragraph — overall tone and key drivers)

5. **Assets to Watch** (bullet list with one-line reason each)

Rules:
- Never repeat the same news twice
- Merge overlapping coverage into the richest single entry
- Organize by what happened, not who reported it"""

    print(f"\n{'='*70}")
    print("  GENERATING MASTER DIGEST...")
    print(f"{'='*70}\n")

    # Use Opus for the final digest — best reasoning for aggregation
    with client_ai.messages.stream(
        model="claude-opus-4-6",
        max_tokens=8000,
        thinking={"type": "adaptive"},
        messages=[{"role": "user", "content": prompt}]
    ) as stream:
        return stream.get_final_message()


def send_to_telegram(tg, label, digest_text):
    header = f"📊 NEWS DIGEST | {label}\n{'='*40}\n\n"
    full_text = header + digest_text
    chunk_size = 4000

    chunks = []
    while len(full_text) > chunk_size:
        split_at = full_text.rfind("\n", 0, chunk_size)
        if split_at == -1:
            split_at = chunk_size
        chunks.append(full_text[:split_at])
        full_text = full_text[split_at:].lstrip("\n")
    if full_text:
        chunks.append(full_text)

    for i, chunk in enumerate(chunks):
        if len(chunks) > 1:
            chunk = f"[{i+1}/{len(chunks)}]\n\n" + chunk
        tg.send_message("me", chunk)
        time.sleep(0.5)

    print(f"  Sent {len(chunks)} message(s) to Telegram Saved Messages.")


def main():
    ai_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    session = StringSession(TELEGRAM_SESSION) if len(TELEGRAM_SESSION) > 20 else TELEGRAM_SESSION
    tg = TelegramClient(session, TELEGRAM_API_ID, TELEGRAM_API_HASH)
    tg.connect()
    if not tg.is_user_authorized():
        tg.disconnect()
        raise Exception("Not authorized. Please run auth separately first.")

    try:
        start_utc, label = get_time_window()
        now_utc = datetime.now(timezone.utc)

        print(f"\n{'='*70}")
        print(f"  TELEGRAM NEWS ANALYSIS  |  Window: {label}")
        print(f"{'='*70}\n")

        dialogs = tg.get_dialogs()
        channels = [d for d in dialogs if isinstance(d.entity, Channel) and not d.entity.megagroup]

        print(f"Found {len(channels)} channels. Fetching messages...\n")

        # Collect all channel messages first (Telegram is sequential)
        raw_channels = []
        for dialog in channels:
            messages = [
                m for m in tg.iter_messages(dialog, limit=200, offset_date=now_utc)
                if m.date and m.date >= start_utc and m.text
            ]
            if not messages:
                continue
            messages_text = "\n".join(
                f"[{m.date.astimezone().strftime('%H:%M')}] {m.text[:500]}"
                for m in reversed(messages)
            )
            sample = "\n".join(m.text[:200] for m in messages[:3])
            raw_channels.append((dialog.name, messages_text, sample))

        print(f"  {len(raw_channels)} channels with messages. Filtering + analyzing in parallel...\n")

        def filter_and_analyze(args):
            name, messages_text, sample = args
            if not is_financial_channel(ai_client, name, sample):
                print(f"  [SKIP] {name}")
                return None
            print(f"  [ANALYZING] {name}")
            return process_channel(ai_client, name, messages_text)

        # Filter + analyze all channels in parallel
        channel_analyses = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(filter_and_analyze, args): args[0]
                for args in raw_channels
            }
            for future in as_completed(futures):
                result = future.result()
                if result:
                    channel_analyses.append(result)
                    print(f"  [DONE] {result[0]}")

        # Master digest using Opus
        digest_text = ""
        if channel_analyses:
            digest = generate_digest(ai_client, channel_analyses, label)
            for block in digest.content:
                if block.type == "text":
                    digest_text = block.text.strip()
                    for line in digest_text.split("\n"):
                        print(f"  {line}")

            if digest_text:
                print("\n  Sending digest to Telegram...")
                send_to_telegram(tg, label, digest_text)

        print(f"\n{'='*70}")
        print("  Done.")
        print(f"{'='*70}\n")

    finally:
        tg.disconnect()


if __name__ == "__main__":
    main()
