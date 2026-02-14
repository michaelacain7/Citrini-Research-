# Citrini Research Substack Monitor

Real-time monitor for Citrini Research's Substack posts, notes, and chat. Detects buy/sell signals, sentiment, and ticker mentions, then sends alerts to Discord.

## Features

- **Multi-source monitoring**: Posts, Notes (Substack Notes), and Chat
- **Market hours aware**: Polls every 3 seconds during market hours (9:30 AM - 4:00 PM EST), every 5 minutes outside
- **Trading signal detection**: Automatically detects buy/sell signals
- **Sentiment analysis**: Positive/negative/neutral classification
- **Ticker extraction**: Identifies stock tickers mentioned
- **Discord alerts**: Rich embeds with color-coded signals
- **State persistence**: Tracks seen content to avoid duplicates

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure

Set environment variables:

```bash
# Required
export DISCORD_WEBHOOK="your_discord_webhook_url"

# Optional - Required for chat monitoring
export SUBSTACK_SID="your_substack_session_cookie"
```

### 3. Get Substack Session Cookie (for Chat access)

To monitor the paid subscriber chat, you need your Substack session cookie:

1. Log into Substack in your browser
2. Open Developer Tools (F12)
3. Go to Application → Cookies → substack.com
4. Find the `substack.sid` cookie
5. Copy the value and set as `SUBSTACK_SID`

**Note**: Without this cookie, posts and notes will still be monitored, but chat messages won't be accessible.

### 4. Run

```bash
# Run continuously (with market hours awareness)
python citrini_monitor.py

# Run once and exit
python citrini_monitor.py --once

# Test Discord webhook
python citrini_monitor.py --test
```

## Market Hours Behavior

| Time Period | Behavior |
|-------------|----------|
| 9:30 AM - 4:00 PM EST (Mon-Fri) | Polling every **2 seconds** |
| Outside market hours | **Sleeping** (no activity) |
| Weekends | **Sleeping** (no activity) |

The monitor sends Discord notifications when market hours start and end, including a count of how many checks were completed during the session.

## Command Line Options

```
--once      Run a single check and exit
--test      Send a test message to Discord
--webhook   Override Discord webhook URL
--cookie    Override Substack session cookie
```

## Railway Deployment

1. Create a new project on Railway
2. Connect your GitHub repo or upload files
3. Set environment variables:
   - `DISCORD_WEBHOOK`: Your Discord webhook URL
   - `SUBSTACK_SID`: Your Substack session cookie (optional)
4. Deploy!

**Estimated cost: ~$5/month** (Hobby plan covers this lightweight workload)

## Signal Detection

The monitor detects:

### Buy Signals
- Keywords: buy, buying, bought, long, adding, bullish, calls, upside
- Phrases: "going long", "starting a position", "attractive entry"

### Sell Signals  
- Keywords: sell, selling, sold, short, exiting, puts, downside
- Phrases: "taking profits", "put hedges", "reducing position"

### Sentiment
- **Positive**: bullish, outperform, rally, breakout, winner
- **Negative**: bearish, underperform, crash, breakdown, loser

## Discord Alert Format

Alerts include:
- 🟢 Green embed for BUY signals
- 🔴 Red embed for SELL signals
- 📈/📉 for positive/negative sentiment
- Tickers mentioned
- Signal keywords found
- Link to original content

## Configuration Options

Edit `Config` class in the script:

```python
poll_interval_posts: int = 300  # Check posts every 5 min
poll_interval_chat: int = 60    # Check chat every 1 min  
poll_interval_notes: int = 120  # Check notes every 2 min
```

## Troubleshooting

### Chat not working
- Ensure `SUBSTACK_SID` cookie is set correctly
- Cookie may have expired - get a fresh one
- You may need a paid subscription to access chat

### Missing alerts
- Check `citrini_state.json` - delete to reset seen content
- Verify Discord webhook is correct with `--test`

### Rate limiting
- Increase poll intervals if getting blocked
- The monitor includes rate limiting protection

## License

MIT
