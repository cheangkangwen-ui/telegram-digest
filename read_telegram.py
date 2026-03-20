import os
import sys
import time
import asyncio
import anthropic

# Fix Unicode printing on Windows
if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import Channel
from telethon.tl.functions.channels import CreateChannelRequest
from datetime import datetime, timezone, timedelta

TELEGRAM_API_ID = 33919151
TELEGRAM_API_HASH = "dd0a935bd6545cf56910292ff4445c4e"
TELEGRAM_SESSION = os.environ.get("TELEGRAM_SESSION", "my_session")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
DIGEST_GROUP_NAME = os.environ.get("DIGEST_GROUP_NAME", "📊 News Digest")

TG_SEMAPHORE = 30

SEARCH_TOOL = {
    "name": "web_search",
    "description": (
        "Search the web for current news, market data, or financial information. "
        "Use this when you need real-time facts to ground the Narrative Sustainability debate."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "The search query"}
        },
        "required": ["query"],
    },
}


def web_search(query: str, max_results: int = 6) -> str:
    from duckduckgo_search import DDGS
    try:
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=max_results))
        if not results:
            return "No results found."
        return "\n\n".join(
            f"**{r['title']}**\n{r['href']}\n{r['body']}" for r in results
        )
    except Exception as e:
        return f"Search failed: {e}"



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


async def get_or_create_digest_group(tg):
    """Return the digest group entity, creating it if it doesn't exist."""
    dialogs = await tg.get_dialogs()
    for d in dialogs:
        if d.name == DIGEST_GROUP_NAME and getattr(d.entity, "megagroup", False):
            return d.entity
    result = await tg(CreateChannelRequest(
        title=DIGEST_GROUP_NAME,
        about="Automated financial news digests",
        megagroup=True,
    ))
    return result.chats[0]


async def main():
    ai_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    session = StringSession(TELEGRAM_SESSION) if len(TELEGRAM_SESSION) > 20 else TELEGRAM_SESSION
    tg = TelegramClient(session, TELEGRAM_API_ID, TELEGRAM_API_HASH)
    await tg.connect()

    if not await tg.is_user_authorized():
        await tg.disconnect()
        raise Exception("Not authorized.")

    try:
        digest_group = await get_or_create_digest_group(tg)

        # Duplicate guard: skip if digest already sent in last 5 minutes (blocks near-simultaneous runs)
        if not os.environ.get("SKIP_DUPLICATE_CHECK"):
            cutoff = datetime.now(timezone.utc) - timedelta(minutes=5)
            async for msg in tg.iter_messages(digest_group, limit=3):
                if msg.date and msg.date >= cutoff and msg.text and "NEWS DIGEST" in msg.text:
                    print("Digest already sent in last 5 minutes. Skipping.")
                    return

        now_utc = datetime.now(timezone.utc)
        myt = timezone(timedelta(hours=8))

        # Always use clock-based window (Worker dispatches via workflow_dispatch,
        # so GITHUB_EVENT_NAME is never "schedule" — treat all runs the same way)
        # Weekdays (Mon–Fri): runs at 0,3,6,9,12,15 UTC
        # Weekends (Sat–Sun): runs at 0,12 UTC
        weekday = now_utc.weekday()  # 0=Mon ... 5=Sat, 6=Sun
        current_hour = now_utc.hour

        if weekday in (5, 6):  # Saturday or Sunday
            day_hours = [0, 12]
            prev_hours = [h for h in day_hours if h < current_hour]
            if prev_hours:
                prev_run_utc = now_utc.replace(hour=max(prev_hours), minute=0, second=0, microsecond=0)
            elif weekday == 5:  # Sat 0 UTC -> Fri 15 UTC
                prev_run_utc = (now_utc - timedelta(days=1)).replace(hour=15, minute=0, second=0, microsecond=0)
            else:  # Sun 0 UTC -> Sat 12 UTC
                prev_run_utc = (now_utc - timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)
        else:  # Weekday (Mon–Fri)
            day_hours = [0, 3, 6, 9, 12, 15]
            prev_hours = [h for h in day_hours if h < current_hour]
            if prev_hours:
                prev_run_utc = now_utc.replace(hour=max(prev_hours), minute=0, second=0, microsecond=0)
            elif weekday == 0:  # Mon 0 UTC -> Sun 12 UTC
                prev_run_utc = (now_utc - timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)
            else:  # Tue–Fri 0 UTC -> previous day 15 UTC
                prev_run_utc = (now_utc - timedelta(days=1)).replace(hour=15, minute=0, second=0, microsecond=0)

        start_utc = prev_run_utc
        label = f"{prev_run_utc.astimezone(myt).strftime('%H:%M')} - {now_utc.astimezone(myt).strftime('%H:%M')} MYT"

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
        prompt = f"""You are a senior financial analyst. Below are raw messages from {len(all_messages)} Telegram channels over {label}.

Ignore channels that are clearly non-financial (entertainment, sports, lifestyle).

Produce a digest with exactly 4 sections:

**1. Top Stories**
- [bullet] most impactful development (with specific numbers/prices where available)
- [bullet] ...
(5-8 bullets, deduplicated across all channels, most impactful first)

Then write a concise 3-5 sentence paragraph summarising the key themes and what they mean for markets.

**2. Market Sentiment**
One paragraph — is the market risk-on, risk-off, or mixed? Explain why based on the news above.

**3. Macro Asset Reactions**
For each asset below, state how it has reacted or is likely to react based on the news. Include specific price moves if mentioned.

**US & Global**
- S&P 500 Futures (ES): [direction/move] — [reason]
- Nasdaq Futures (NQ): [direction/move] — [reason]
- Gold (spot): [direction/move] — [reason]
- Oil (WTI / Brent): [direction/move] — [reason]
- Bitcoin (BTC): [direction/move] — [reason]
- DXY (USD Index): [direction/move] — [reason]
- US 10Y Yield: [direction/move] — [reason]

**EM Asia Equities**
- HSI (Hong Kong): [direction/move] — [reason]
- CSI 300 (China): [direction/move] — [reason]
- Shanghai Composite: [direction/move] — [reason]
- Nikkei 225 (Japan): [direction/move] — [reason]
- KOSPI (South Korea): [direction/move] — [reason]
- STI (Singapore): [direction/move] — [reason]
- KLCI (Malaysia): [direction/move] — [reason]

**EM Asia Yields**
- China 10Y: [direction/move] — [reason]
- India 10Y: [direction/move] — [reason]

If there is no relevant news for an asset, write "No significant news this window."

**4. Narrative Sustainability**
Before writing this section, use the web_search tool to look up 2-3 current sources (e.g. latest macro data, recent analyst commentary, breaking news) to ground your debate in real-time facts — not just the Telegram messages above.

Debate yourself. Based on today's dominant market narrative, answer: is this narrative sustainable?

Structure it as:
- **The narrative**: State the current prevailing market narrative in 1-2 sentences.
- **Why it holds (Bull case)**: Key drivers and first principles supporting it continuing.
- **Why it breaks (Bear case)**: What fundamentals, contradictions, or risks undermine it. Be intellectually honest — steelman the opposing view.
- **Verdict**: Is the narrative sustainable? If yes, what would change that? If no, when might it crack and to what extent (magnitude of reversal, assets most affected)?

Be rigorous. Do not hedge everything. Take a position.

Rules: Never repeat the same news twice. Merge overlapping coverage across channels.

RAW MESSAGES:
{raw_dump}"""

        print("  Sending to Opus for analysis...\n")

        loop = asyncio.get_event_loop()

        def _call():
            messages = [{"role": "user", "content": prompt}]
            while True:
                response = ai_client.messages.create(
                    model="claude-opus-4-6",
                    max_tokens=8000,
                    thinking={"type": "adaptive"},
                    tools=[SEARCH_TOOL],
                    messages=messages,
                )
                if response.stop_reason != "tool_use":
                    return response
                # Execute web searches Claude requested
                tool_results = []
                for block in response.content:
                    if block.type == "tool_use":
                        print(f"  [web search] {block.input['query']}")
                        result = web_search(block.input["query"])
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": result,
                        })
                messages.append({"role": "assistant", "content": response.content})
                messages.append({"role": "user", "content": tool_results})

        response = await loop.run_in_executor(None, _call)

        u = response.usage
        print(f"  Tokens — input: {u.input_tokens:,}  output: {u.output_tokens:,}  cache_read: {getattr(u, 'cache_read_input_tokens', 0):,}")

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
            first_msg = None
            for i, chunk in enumerate(chunks):
                if len(chunks) > 1:
                    chunk = f"[{i+1}/{len(chunks)}]\n\n" + chunk
                sent = await tg.send_message(digest_group, chunk)
                if i == 0:
                    first_msg = sent
                await asyncio.sleep(0.5)
            if first_msg:
                await tg.pin_message(digest_group, first_msg.id, notify=False)
            print(f"  Sent {len(chunks)} message(s).")

            # Notify Cloudflare Worker so it knows a digest was sent
            try:
                import urllib.request
                urllib.request.urlopen(urllib.request.Request(
                    "https://telegram-digest-scheduler.cheangkangwen.workers.dev/notify",
                    method="POST"
                ), timeout=5)
            except Exception:
                pass

        print(f"\n{'='*70}")
        print("  Done.")
        print(f"{'='*70}\n")

    finally:
        await tg.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
