import os
import time
import asyncio
import anthropic
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import Channel
from datetime import datetime, timezone, timedelta

TELEGRAM_API_ID = 33919151
TELEGRAM_API_HASH = "dd0a935bd6545cf56910292ff4445c4e"
TELEGRAM_SESSION = os.environ.get("TELEGRAM_SESSION", "my_session")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

TG_SEMAPHORE = 10   # max concurrent Telegram fetches
API_SEMAPHORE = 20  # max concurrent Anthropic calls


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


async def fetch_channel_messages(tg, dialog, start_utc, now_utc, sem):
    async with sem:
        messages = []
        async for m in tg.iter_messages(dialog, limit=100, offset_date=now_utc):
            if m.date and m.date >= start_utc and m.text:
                messages.append(m)
            elif m.date and m.date < start_utc:
                break
        return messages


async def call_anthropic(ai_client, model, max_tokens, messages, thinking=False):
    loop = asyncio.get_event_loop()
    kwargs = dict(model=model, max_tokens=max_tokens, messages=messages)
    if thinking:
        kwargs["thinking"] = {"type": "adaptive"}

    for attempt in range(3):
        try:
            def _call():
                with ai_client.messages.stream(**kwargs) as stream:
                    return stream.get_final_message()
            return await loop.run_in_executor(None, _call)
        except Exception as e:
            if attempt < 2:
                await asyncio.sleep(3)
            else:
                raise


async def is_financial_channel(ai_client, channel_name, sample_text, sem):
    async with sem:
        prompt = f"""Channel name: "{channel_name}"
Sample messages:
{sample_text[:600]}

Does this channel contain ANY financial, market, economic, geopolitical, crypto, stocks, commodities, macro, or business news relevant to investors or traders? When in doubt, answer YES. Only answer NO if clearly non-financial (entertainment, sports, personal lifestyle, university events). Answer only YES or NO."""
        try:
            response = await call_anthropic(
                ai_client, "claude-haiku-4-5", 5,
                [{"role": "user", "content": prompt}]
            )
            return response.content[0].text.strip().upper().startswith("YES")
        except:
            return True  # default to include on error


async def analyze_channel(ai_client, channel_name, messages_text, sem):
    async with sem:
        prompt = f"""You are a financial analyst. Extract all market-relevant information from these Telegram messages (channel: "{channel_name}").

MESSAGES:
{messages_text}

Return a structured extract with:

1. **Assets mentioned** (list every asset, ticker, or market covered — be specific: "AAPL", "BTC", "KOSPI", "Gold", "Crude Oil", etc.)

2. **News items** (one bullet per distinct development — include ALL of them):
   Format: [ASSET] — [what happened] — [key figure/number if any]

3. **Divergence alerts** (only where news contradicts the asset's known fundamentals):
   [ASSET] | [news] | [what fundamentals say] | [severity: LOW/MEDIUM/HIGH/EXTREME] | [POSITIVE/NEGATIVE]

Be exhaustive. Include every development mentioned."""

        try:
            response = await call_anthropic(
                ai_client, "claude-sonnet-4-6", 2048,
                [{"role": "user", "content": prompt}]
            )
            for block in response.content:
                if block.type == "text":
                    return (channel_name, block.text.strip())
        except Exception as e:
            print(f"  [FAILED] {channel_name}: {type(e).__name__}")
        return None


async def process_channel(tg, ai_client, dialog, start_utc, now_utc, tg_sem, api_sem):
    messages = await fetch_channel_messages(tg, dialog, start_utc, now_utc, tg_sem)
    if not messages:
        return None

    messages_text = "\n".join(
        f"[{m.date.astimezone().strftime('%H:%M')}] {m.text[:500]}"
        for m in reversed(messages)
    )
    sample = "\n".join(m.text[:200] for m in messages[:3])

    if not await is_financial_channel(ai_client, dialog.name, sample, api_sem):
        print(f"  [SKIP] {dialog.name}")
        return None

    print(f"  [ANALYZING] {dialog.name} ({len(messages)} messages)")
    result = await analyze_channel(ai_client, dialog.name, messages_text, api_sem)
    if result:
        print(f"  [DONE] {result[0]}")
    return result


async def generate_digest(ai_client, channel_analyses, label):
    combined = "\n\n".join(
        f"=== {name} ===\n{analysis}"
        for name, analysis in channel_analyses
    )

    prompt = f"""You are a senior financial analyst. Aggregate these channel analyses into a unified digest for: {label}.

{combined}

Sections:

1. **Top Stories** (5-8 biggest developments, aggregated across sources, with specific numbers/prices):
   ### [Story Title]
   [3-6 sentence aggregated summary]

2. **Asset-by-Asset Breakdown** (group by asset, deduplicate, every asset with news):
   ### [Asset]
   - [development]

3. **Divergence Watchlist** (HIGH and EXTREME only):
   Asset | What happened | Fundamentals say | Severity | Direction

4. **Market Sentiment** (1 paragraph)

5. **Assets to Watch** (bullet + one-line reason)

Rules: Never repeat the same news. Merge overlapping coverage. Organize by what happened, not who reported it."""

    print(f"\n{'='*70}")
    print("  GENERATING MASTER DIGEST...")
    print(f"{'='*70}\n")

    response = await call_anthropic(
        ai_client, "claude-opus-4-6", 8000,
        [{"role": "user", "content": prompt}],
        thinking=True
    )
    for block in response.content:
        if block.type == "text":
            return block.text.strip()
    return ""


def send_to_telegram_sync(tg, label, digest_text):
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
        tg.loop.run_until_complete(tg.send_message("me", chunk))
        time.sleep(0.5)

    print(f"  Sent {len(chunks)} message(s) to Telegram.")


async def main():
    ai_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    session = StringSession(TELEGRAM_SESSION) if len(TELEGRAM_SESSION) > 20 else TELEGRAM_SESSION
    tg = TelegramClient(session, TELEGRAM_API_ID, TELEGRAM_API_HASH)
    await tg.connect()

    if not await tg.is_user_authorized():
        await tg.disconnect()
        raise Exception("Not authorized.")

    try:
        start_utc, label = get_time_window()
        now_utc = datetime.now(timezone.utc)

        print(f"\n{'='*70}")
        print(f"  TELEGRAM NEWS ANALYSIS  |  Window: {label}")
        print(f"{'='*70}\n")

        dialogs = await tg.get_dialogs()
        channels = [d for d in dialogs if isinstance(d.entity, Channel) and not d.entity.megagroup]
        print(f"Found {len(channels)} channels. Processing concurrently...\n")

        tg_sem = asyncio.Semaphore(TG_SEMAPHORE)
        api_sem = asyncio.Semaphore(API_SEMAPHORE)

        tasks = [
            process_channel(tg, ai_client, dialog, start_utc, now_utc, tg_sem, api_sem)
            for dialog in channels
        ]
        results = await asyncio.gather(*tasks)
        channel_analyses = [r for r in results if r]

        print(f"\n  Analyzed {len(channel_analyses)} financial channels.")

        if channel_analyses:
            digest_text = await generate_digest(ai_client, channel_analyses, label)

            for line in digest_text.split("\n"):
                print(f"  {line}")

            if digest_text:
                print("\n  Sending to Telegram...")
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
                    await tg.send_message("me", chunk)
                    await asyncio.sleep(0.5)
                print(f"  Sent {len(chunks)} message(s).")

        print(f"\n{'='*70}")
        print("  Done.")
        print(f"{'='*70}\n")

    finally:
        await tg.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
