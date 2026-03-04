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

TG_SEMAPHORE = 30


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


async def fetch_channel(tg, dialog, start_utc, now_utc, sem):
    async with sem:
        messages = []
        try:
            async for m in tg.iter_messages(dialog, offset_date=now_utc):
                if not m.date:
                    continue
                if m.date < start_utc:
                    break  # stop as soon as we go past the window
                if m.text:
                    messages.append(m)
        except Exception:
            pass
        return messages


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

        # Pre-filter: only fetch channels that posted within the time window
        # dialog.message.date gives last message time without any extra API call
        active_channels = [
            d for d in channels
            if d.message and d.message.date and d.message.date >= start_utc
        ]
        print(f"{len(channels)} channels total, {len(active_channels)} posted in window. Fetching...\n")

        tg_sem = asyncio.Semaphore(TG_SEMAPHORE)
        tasks = [fetch_channel(tg, d, start_utc, now_utc, tg_sem) for d in active_channels]
        results = await asyncio.gather(*tasks)

        # Build one big raw message dump grouped by channel
        all_messages = []
        for dialog, messages in zip(active_channels, results):
            if not messages:
                continue
            channel_block = f"### {dialog.name}\n" + "\n".join(
                f"[{m.date.astimezone().strftime('%H:%M')}] {m.text[:400]}"
                for m in reversed(messages)
            )
            all_messages.append(channel_block)

        print(f"  Got messages from {len(all_messages)} channels.")

        if not all_messages:
            print("  No messages found.")
            return

        raw_dump = "\n\n".join(all_messages)

        # Single Opus call to do everything at once
        prompt = f"""You are a senior financial analyst. Below are raw messages from {len(all_messages)} Telegram channels over the time window: {label}.

First, ignore any channels that are clearly non-financial (entertainment, sports, lifestyle, university events).

For the remaining financial channels, produce a MASTER DIGEST:

1. **Top Stories** (5-8 biggest developments aggregated across all sources, with specific numbers/prices):
   ### [Story Title]
   [3-6 sentence summary combining all angles]

2. **Asset-by-Asset Breakdown** (group ALL news by asset, deduplicate across channels):
   ### [Asset]
   - [development]

3. **Divergence Watchlist** (HIGH and EXTREME only — news that contradicts asset fundamentals):
   Asset | What happened | What fundamentals say | Severity | Direction

4. **Market Sentiment** (1 paragraph — risk-on/off/mixed and why)

5. **Assets to Watch** (bullet + one-line reason each)

Rules: Never repeat the same news twice. Merge overlapping coverage. Organize by what happened, not by channel.

RAW MESSAGES:
{raw_dump}"""

        print("  Sending to Opus for analysis...\n")

        loop = asyncio.get_event_loop()

        def _call():
            with ai_client.messages.stream(
                model="claude-opus-4-6",
                max_tokens=8000,
                thinking={"type": "adaptive"},
                messages=[{"role": "user", "content": prompt}]
            ) as stream:
                return stream.get_final_message()

        response = await loop.run_in_executor(None, _call)

        digest_text = ""
        for block in response.content:
            if block.type == "text":
                digest_text = block.text.strip()
                for line in digest_text.split("\n"):
                    print(f"  {line}")

        # Send to Telegram
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
