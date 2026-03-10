#!/usr/bin/env python3
"""
Citrini Research Monitor - Enhanced Version with Web Scraping
Includes BeautifulSoup-based scraping as fallback for chat access.
Polls every 3 seconds during market hours (9:30 AM - 4:00 PM EST).
"""

import os
import re
import json
import time
import hashlib
import logging
import requests
import feedparser
import threading
import queue
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote

try:
    import websocket as ws_client
    HAS_WEBSOCKET = True
except ImportError:
    HAS_WEBSOCKET = False

try:
    import pytz
    EST = pytz.timezone('America/New_York')
except ImportError:
    EST = None

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class PublicationConfig:
    """Config for a single monitored publication."""
    name: str
    chat_id: str  # Empty string if no chat access
    user_handle: str
    user_id: str  # Numeric user ID for matching
    publication_url: str
    rss_feed_url: str
    match_keywords: list  # Keywords to match in thread name
    monitor_chat: bool = True  # Whether to monitor chat (requires subscription)
    monitor_posts: bool = True  # Whether to monitor posts/articles via RSS


# Publications to monitor
PUBLICATIONS = [
    PublicationConfig(
        name="Citrini Research",
        chat_id="836125",
        user_handle="citrini",
        user_id="86606269",
        publication_url="https://www.citriniresearch.com",
        rss_feed_url="https://www.citriniresearch.com/feed",
        match_keywords=["citrini"],
        monitor_chat=True,
        monitor_posts=True,
    ),
    PublicationConfig(
        name="Cassandra Unchained",
        chat_id="6819723",
        user_handle="michaeljburry",
        user_id="",  # Will be discovered
        publication_url="https://michaeljburry.substack.com",
        rss_feed_url="https://michaeljburry.substack.com/feed",
        match_keywords=["cassandra", "burry", "michaeljburry"],
        monitor_chat=True,
        monitor_posts=True,
    ),
    PublicationConfig(
        name="The Bear Cave",
        chat_id="",  # No subscription - posts only
        user_handle="thebearcave",
        user_id="",
        publication_url="https://thebearcave.substack.com",
        rss_feed_url="https://thebearcave.substack.com/feed",
        match_keywords=["bear cave"],
        monitor_chat=False,  # No sub
        monitor_posts=True,  # Watch for new articles
    ),
    PublicationConfig(
        name="Last Bear Standing",
        chat_id="416240",
        user_handle="lastbearstanding",
        user_id="",  # Will be discovered
        publication_url="https://lastbearstanding.substack.com",
        rss_feed_url="https://lastbearstanding.substack.com/feed",
        match_keywords=["last bear", "lastbear"],
        monitor_chat=True,
        monitor_posts=True,
    ),
    PublicationConfig(
        name="Multibagger Monitor",
        chat_id="2273648",
        user_handle="huntingmultibaggers",
        user_id="",  # Will be discovered
        publication_url="https://stocknarratives.substack.com",
        rss_feed_url="https://stocknarratives.substack.com/feed",
        match_keywords=["multibagger", "stocknarratives", "huntingmultibaggers"],
        monitor_chat=True,
        monitor_posts=True,
    ),
    PublicationConfig(
        name="Za's Market Terminal",
        chat_id="6459287",
        user_handle="zastocks",
        user_id="",  # Will be discovered
        publication_url="https://zastocks.substack.com",
        rss_feed_url="https://zastocks.substack.com/feed",
        match_keywords=["zastocks", "za's market", "za market"],
        monitor_chat=True,
        monitor_posts=True,
    ),
    PublicationConfig(
        name="Martin Shkreli",
        chat_id="",
        user_handle="martinshkreli",
        user_id="",
        publication_url="https://martinshkreli.substack.com",
        rss_feed_url="https://martinshkreli.substack.com/feed",
        match_keywords=["shkreli", "martinshkreli"],
        monitor_chat=False,
        monitor_posts=True,  # ⚡ Instant via user channel WebSocket (subscribed)
    ),
    PublicationConfig(
        name="Za's Market Terminal",
        chat_id="6459287",
        user_handle="zastocks",
        user_id="",  # Will be discovered
        publication_url="https://zastocks.com",
        rss_feed_url="https://zastocks.substack.com/feed",
        match_keywords=["zastocks", "za's market", "za market"],
        monitor_chat=True,
        monitor_posts=True,
    ),
    PublicationConfig(
        name="MVC Investing",
        chat_id="2098460",
        user_handle="mvcinvesting",
        user_id="",  # Will be discovered
        publication_url="https://mvcinvesting.substack.com",
        rss_feed_url="https://mvcinvesting.substack.com/feed",
        match_keywords=["mvc", "mvcinvesting"],
        monitor_chat=True,
        monitor_posts=True,
    ),
]


@dataclass
class Config:
    """Configuration settings."""
    discord_webhook: str = os.getenv(
        "DISCORD_WEBHOOK",
        "https://discordapp.com/api/webhooks/919672540237017138/Zga2QHBVwPUKXbCMNQ6hRXSsJaW8d136pOZNheRz1SK0YS5GIRnpjsGdN7trPul-zeXo"
    )
    substack_sid: str = os.getenv(
        "SUBSTACK_SID",
        "s%3AQJJmXar5y93BQEUsRGywS92ySuYdcdcT.godFCcejHCwFeME7%2FQoZKkwQKBKVJWtlTqXx6N5R1hg"
    )
    # Polling during market hours (9:30 AM - 4:00 PM EST)
    poll_interval_market_hours: int = 2  # 2 seconds during market hours
    
    # Market hours (EST)
    market_open_hour: int = 9
    market_open_minute: int = 30
    market_close_hour: int = 16  # 4 PM EST
    market_close_minute: int = 0
    
    state_file: str = "citrini_state.json"
    
    # DeepSeek API for AI analysis
    deepseek_api_key: str = os.getenv(
        "DEEPSEEK_API_KEY",
        "sk-1cf5b2ab46a14eb6978ff7ba7ce3f3e3"
    )
    deepseek_model: str = "deepseek-chat"
    
    # Substack user ID (from WebSocket discovery)
    substack_user_id: str = "74972770"
    
    # Legacy fields for backward compat
    publication_url: str = "https://www.citriniresearch.com"
    rss_feed_url: str = "https://www.citriniresearch.com/feed"
    chat_id: str = "836125"
    user_handle: str = "citrini"


def is_market_hours(config: Config) -> bool:
    """Check if current time is within market hours (9:30 AM - 4:00 PM EST, Mon-Fri)."""
    if EST:
        now = datetime.now(EST)
    else:
        # Fallback: assume UTC-5 for EST (doesn't handle DST perfectly)
        from datetime import timezone, timedelta
        est_offset = timezone(timedelta(hours=-5))
        now = datetime.now(est_offset)
    
    # Check if it's a weekday (Monday=0, Sunday=6)
    if now.weekday() >= 5:  # Saturday or Sunday
        return False
    
    # Create market open and close times for today
    market_open = now.replace(
        hour=config.market_open_hour,
        minute=config.market_open_minute,
        second=0,
        microsecond=0
    )
    market_close = now.replace(
        hour=config.market_close_hour,
        minute=config.market_close_minute,
        second=0,
        microsecond=0
    )
    
    return market_open <= now <= market_close


class SignalDetector:
    """Detects trading signals and sentiment."""
    
    TICKER_PATTERN = re.compile(r'\b([A-Z]{2,5})\b')
    TICKER_EXCLUSIONS = {
        'THE', 'AND', 'FOR', 'ARE', 'BUT', 'NOT', 'YOU', 'ALL', 'CAN', 'HAD',
        'HER', 'WAS', 'ONE', 'OUR', 'OUT', 'HAS', 'HIS', 'HOW', 'ITS', 'MAY',
        'NEW', 'NOW', 'OLD', 'SEE', 'WAY', 'WHO', 'BOY', 'DID', 'GET', 'LET',
        'PUT', 'SAY', 'SHE', 'TOO', 'USE', 'CEO', 'CFO', 'COO', 'IPO', 'ETF',
        'GDP', 'CPI', 'PPI', 'FED', 'SEC', 'NYSE', 'DOW', 'YTD', 'QTD', 'MTD',
        'EOD', 'ATH', 'ATL', 'RSI', 'EPS', 'ROE', 'ROA', 'NON', 'PRE', 'POST',
        'EST', 'PST', 'UTC', 'USA', 'IMO', 'TBH', 'FYI', 'AKA', 'ETC', 'TLDR',
        'LONG', 'SHORT', 'BUY', 'SELL', 'HOLD', 'THIS', 'THAT', 'WITH', 'FROM',
        'YOUR', 'HAVE', 'WILL', 'BEEN', 'MORE', 'WHEN', 'WHAT', 'SOME', 'THAN',
        'THEM', 'THEN', 'THESE', 'ONLY', 'INTO', 'JUST', 'ALSO', 'VERY', 'MUCH',
        'SUCH', 'MOST', 'WELL', 'BACK', 'EVEN', 'GOOD', 'OVER', 'LAST', 'MANY',
        'WEEK', 'YEAR', 'HERE', 'NEED', 'TAKE', 'KNOW', 'MAKE', 'LIKE', 'LOOK',
        'WANT', 'GIVE', 'CALL', 'HIGH', 'LOW', 'OPEN', 'RISK', 'TRADE', 'CHAT'
    }
    
    BUY_PATTERNS = [
        re.compile(r'\b(buy|buying|bought|long|adding|added|accumulating|bullish)\b', re.I),
        re.compile(r'\b(entering|entered|initiating|initiated)\b', re.I),
        re.compile(r'\b(call|calls|upside|breakout)\b', re.I),
        re.compile(r'\bgoing\s+long\b', re.I),
        re.compile(r'\bpicking\s+up\b', re.I),
        re.compile(r'\bstarting\s+(a\s+)?position\b', re.I),
        re.compile(r'\bincreasing\s+(exposure|position)', re.I),
    ]
    
    SELL_PATTERNS = [
        re.compile(r'\b(sell|selling|sold|short|shorting|shorted)\b', re.I),
        re.compile(r'\b(exiting|exited|closing|closed|trimming|trimmed)\b', re.I),
        re.compile(r'\b(put|puts|downside|breakdown)\b', re.I),
        re.compile(r'\b(reducing|reduced|cutting|cut)\s+(position|exposure)', re.I),
        re.compile(r'\bgoing\s+short\b', re.I),
        re.compile(r'\btaking\s+profits?\b', re.I),
        re.compile(r'\b(hedge|hedges|hedging|put\s+hedges?)\b', re.I),
        re.compile(r'\b(bearish|cautious|worried|concerned)\b', re.I),
    ]
    
    POSITIVE_PATTERNS = [
        re.compile(r'\b(bullish|optimistic|positive|strong|excellent|great)\b', re.I),
        re.compile(r'\b(outperform|beat|exceeded|rally|surge|soar)\b', re.I),
        re.compile(r'\b(upside|breakout|momentum|tailwind)\b', re.I),
        re.compile(r'\b(winner|winning|success|killed\s+it)\b', re.I),
        re.compile(r'\bup\s+\d+%', re.I),
    ]
    
    NEGATIVE_PATTERNS = [
        re.compile(r'\b(bearish|pessimistic|negative|weak|poor|terrible)\b', re.I),
        re.compile(r'\b(underperform|miss|disappoint|crash|dump|tank)\b', re.I),
        re.compile(r'\b(downside|breakdown|headwind|risky)\b', re.I),
        re.compile(r'\b(loser|losing|loss|failure|failed)\b', re.I),
        re.compile(r'\bnot\s+(inspiring|good|great)', re.I),
        re.compile(r'\bdown\s+\d+%', re.I),
    ]
    
    def extract_tickers(self, text: str) -> List[str]:
        matches = self.TICKER_PATTERN.findall(text)
        return list(set(m for m in matches if m not in self.TICKER_EXCLUSIONS))
    
    def detect_signals(self, text: str, patterns: List) -> List[str]:
        signals = []
        for p in patterns:
            matches = p.findall(text)
            if matches:
                if isinstance(matches[0], str):
                    signals.extend(matches)
        return list(set(signals))
    
    def analyze_content(self, text: str) -> Dict[str, Any]:
        tickers = self.extract_tickers(text)
        buy_signals = []
        sell_signals = []
        
        for p in self.BUY_PATTERNS:
            if p.search(text):
                buy_signals.append(p.pattern)
        
        for p in self.SELL_PATTERNS:
            if p.search(text):
                sell_signals.append(p.pattern)
        
        pos_count = sum(1 for p in self.POSITIVE_PATTERNS if p.search(text))
        neg_count = sum(1 for p in self.NEGATIVE_PATTERNS if p.search(text))
        
        if pos_count > neg_count:
            sentiment = "POSITIVE"
        elif neg_count > pos_count:
            sentiment = "NEGATIVE"
        else:
            sentiment = "NEUTRAL"
        
        if buy_signals and not sell_signals:
            signal_type = "BUY"
        elif sell_signals and not buy_signals:
            signal_type = "SELL"
        elif buy_signals and sell_signals:
            signal_type = "MIXED"
        else:
            signal_type = None
        
        return {
            "tickers": tickers,
            "buy_signals": bool(buy_signals),
            "sell_signals": bool(sell_signals),
            "signal_type": signal_type,
            "sentiment": {"sentiment": sentiment, "score": abs(pos_count - neg_count)},
            "is_actionable": bool(signal_type and tickers)
        }


class DeepSeekAnalyzer:
    """AI-powered analysis of Citrini's posts using DeepSeek API."""
    
    API_URL = "https://api.deepseek.com/v1/chat/completions"
    
    SYSTEM_PROMPT = """You are a Bloomberg terminal headline writer and financial analyst assistant. 
You analyze posts from financial analysts and newsletters on Substack, including:
- Citrini Research (known for tech/AI stock analysis)
- Cassandra Unchained / Michael Burry (known for contrarian macro/value plays)
- The Bear Cave (known for short-selling research, fraud exposés, corporate governance issues)
- Last Bear Standing (known for macro analysis, market commentary, bearish perspectives)
- Multibagger Monitor (known for microcap/small-cap stock picks, event-driven and reflexive situations, VC-style approach to public markets)
- Za's Market Terminal (known for high-conviction trade ideas combining technical analysis, fundamentals, and thematic trends)
- Martin Shkreli (known for pharma/biotech analysis, contrarian takes, and market commentary)
- MVC Investing (known for value investing, deep-dive research, and stock analysis)
- Za's Market Terminal (known for swing/position trading, technical analysis, thematic investing in tech/AI/growth stocks)

When given a post, you must output a concise analysis in this EXACT format:

**HEADLINE**: A Bloomberg-style one-line headline (e.g., "NVDA — BULLISH: Citrini Expects Q1 Guidance >$75B, Sees Rubin Orders as Key Catalyst")

**TICKERS**: List each ticker mentioned with the stance:
- TICKER — BUY/SELL/LONG/SHORT/HOLD/WATCH/NEUTRAL — brief reason

**KEY POINTS**:
- Most important takeaway (1 sentence)
- Second most important point (1 sentence)  
- Third point if relevant (1 sentence)

**CONVICTION**: LOW / MEDIUM / HIGH — based on language strength

**RISKS**: Key risks mentioned (1 sentence)

Rules:
- Be extremely concise. This is for a trader who needs to act fast.
- If no clear buy/sell signal, say "COMMENTARY" instead
- Extract EVERY ticker mentioned, even in passing (including HK-listed stocks like 9618 HK, 3690 HK etc)
- Pay attention to position sizing language ("adding", "full position", "trimming", "not large positions")
- If the post is not about stocks/markets, just summarize briefly
- Never hallucinate tickers that aren't in the post
- For Michael Burry posts, pay special attention to his contrarian thesis and macro views
- For Bear Cave posts, these are typically SHORT-SELL research - extract the target company and the fraud/issue alleged
- For Last Bear Standing posts, focus on macro thesis and market directional calls
- For Multibagger Monitor posts, focus on the specific stock pick, upside thesis, catalyst, and target price
- For Za's Market Terminal posts, focus on trade entries/exits, technical levels, thematic plays, and conviction level
- For Martin Shkreli posts, focus on pharma/biotech catalysts, drug pipeline analysis, and any stock picks
- For MVC Investing posts, focus on the valuation thesis, key metrics, and investment case"""

    def __init__(self, api_key: str, model: str = "deepseek-chat"):
        self.api_key = api_key
        self.model = model
    
    def analyze(self, content: str, source: str = "Chat") -> Optional[str]:
        """Analyze content with DeepSeek. Returns formatted analysis string."""
        if not self.api_key:
            return None
        
        # Clean HTML tags
        clean_content = re.sub(r'<[^>]+>', '', content)
        
        # Truncate if extremely long (save tokens)
        if len(clean_content) > 8000:
            clean_content = clean_content[:8000] + "... [truncated]"
        
        try:
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": self.SYSTEM_PROMPT},
                    {"role": "user", "content": f"Analyze this Citrini Research {source.lower()} post:\n\n{clean_content}"}
                ],
                "max_tokens": 800,
                "temperature": 0.3
            }
            
            r = requests.post(self.API_URL, headers=headers, json=payload, timeout=30)
            
            if r.status_code == 200:
                data = r.json()
                analysis = data["choices"][0]["message"]["content"]
                logger.info(f"DeepSeek analysis completed ({len(analysis)} chars)")
                return analysis
            else:
                logger.error(f"DeepSeek API error: {r.status_code} - {r.text[:200]}")
                return None
                
        except Exception as e:
            logger.error(f"DeepSeek analysis error: {e}")
            return None


class DiscordWebhook:
    """Discord webhook sender."""
    
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
    
    def send(self, content: str = None, embeds: List[Dict] = None) -> bool:
        payload = {}
        if content:
            payload["content"] = content
        if embeds:
            payload["embeds"] = embeds
        
        try:
            r = requests.post(self.webhook_url, json=payload, timeout=10)
            return r.status_code in [200, 204]
        except Exception as e:
            logger.error(f"Discord error: {e}")
            return False
    
    def send_alert(self, source: str, title: str, content: str, url: str, analysis: Dict):
        if analysis["signal_type"] == "BUY":
            color, emoji = 0x00FF00, "🟢"
        elif analysis["signal_type"] == "SELL":
            color, emoji = 0xFF0000, "🔴"
        elif analysis["sentiment"]["sentiment"] == "POSITIVE":
            color, emoji = 0x90EE90, "📈"
        elif analysis["sentiment"]["sentiment"] == "NEGATIVE":
            color, emoji = 0xFFCCCC, "📉"
        else:
            color, emoji = 0x808080, "📊"
        
        embed = {
            "title": f"{emoji} {title[:200]}",
            "description": content[:2000],
            "color": color,
            "url": url,
            "author": {"name": "Citrini Research"},
            "fields": [],
            "footer": {"text": f"Source: {source}"},
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        if analysis["signal_type"]:
            embed["fields"].append({"name": "🎯 Signal", "value": analysis["signal_type"], "inline": True})
        if analysis["tickers"]:
            embed["fields"].append({"name": "📌 Tickers", "value": ", ".join(analysis["tickers"][:15]), "inline": True})
        embed["fields"].append({"name": "💭 Sentiment", "value": analysis["sentiment"]["sentiment"], "inline": True})
        
        return self.send(embeds=[embed])
    
    def send_ai_alert(self, source: str, title: str, raw_content: str, url: str, ai_analysis: str, regex_analysis: Dict, author: str = "Research Alert") -> bool:
        """Send a rich AI-analyzed alert to Discord."""
        
        # Determine color from AI analysis text
        ai_lower = ai_analysis.lower()
        if any(w in ai_lower for w in ["— buy", "— long", "bullish"]):
            color, emoji = 0x00FF00, "🟢"
        elif any(w in ai_lower for w in ["— sell", "— short", "bearish"]):
            color, emoji = 0xFF0000, "🔴"
        elif "high" in ai_lower and "conviction" in ai_lower:
            color, emoji = 0x00FF00, "🟢"
        else:
            color, emoji = 0x3498DB, "📊"
        
        # Extract headline from AI analysis (first line after **HEADLINE**)
        headline = title
        for line in ai_analysis.split("\n"):
            if "HEADLINE" in line.upper() and ":" in line:
                headline = line.split(":", 1)[1].strip().strip("*").strip()
                break
        
        # Build Discord embed
        embed = {
            "title": f"{emoji} {headline[:250]}",
            "color": color,
            "url": url,
            "author": {"name": f"{author} — AI Analysis"},
            "fields": [],
            "footer": {"text": f"Source: {source} | Powered by DeepSeek"},
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        # Add AI analysis as the main description (truncate for Discord's 4096 limit)
        embed["description"] = ai_analysis[:4000]
        
        # Add raw post preview as a field
        clean_raw = re.sub(r'<[^>]+>', '', raw_content)[:500]
        if clean_raw:
            embed["fields"].append({
                "name": "📝 Raw Post Preview",
                "value": clean_raw + ("..." if len(raw_content) > 500 else ""),
                "inline": False
            })
        
        return self.send(embeds=[embed])


class StateManager:
    """Tracks seen content."""
    
    def __init__(self, state_file: str):
        self.state_file = Path(state_file)
        self.state = self._load()
    
    def _load(self) -> Dict:
        if self.state_file.exists():
            try:
                return json.load(open(self.state_file))
            except:
                pass
        return {"seen": {}}
    
    def save(self):
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f)
    
    def is_seen(self, key: str) -> bool:
        return key in self.state["seen"]
    
    def mark_seen(self, key: str):
        self.state["seen"][key] = time.time()
        # Keep only last 2000 items
        if len(self.state["seen"]) > 2000:
            sorted_keys = sorted(self.state["seen"].items(), key=lambda x: x[1])
            self.state["seen"] = dict(sorted_keys[-1500:])
        self.save()
    
    @staticmethod
    def hash(content: str) -> str:
        return hashlib.md5(content.encode()).hexdigest()[:16]


class ZyncWebSocket:
    """Real-time WebSocket connection to Substack's Zync system.
    Receives instant push notifications when new chat messages are posted.
    Zero polling - sub-second latency."""
    
    WS_URL = "wss://zyncrealtime.substack.com/"
    TOKEN_URL = "https://substack.com/api/v1/realtime/token"
    TOKEN_REFRESH_INTERVAL = 2700  # Refresh token every 45 min (expires in 1hr)
    
    def __init__(self, config: Config):
        self.config = config
        self.chat_queue = queue.Queue()  # Chat message pushes
        self.post_queue = queue.Queue()  # New article/post pushes
        self._ws = None
        self._thread = None
        self._running = False
        self._token = None
        self._token_time = 0
        self._channels = self._build_channels()
        self._connected = False
        self._reconnect_delay = 1
        
        # Build lookup for routing pushes
        self._chat_channels = set()
        for pub in PUBLICATIONS:
            if pub.monitor_chat and pub.chat_id:
                self._chat_channels.add(f"chat:{pub.chat_id}:only_paid")
                self._chat_channels.add(f"chat:{pub.chat_id}:all_subscribers")
        self._user_channel = f"user:{config.substack_user_id}"
        
        # HTTP session for token requests
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "*/*",
        })
        if config.substack_sid:
            from urllib.parse import unquote
            cookie_value = unquote(config.substack_sid)
            self._session.cookies.set("substack.sid", cookie_value, domain=".substack.com")
    
    def _build_channels(self) -> List[str]:
        """Build list of channels to subscribe to."""
        channels = [f"user:{self.config.substack_user_id}"]
        for pub in PUBLICATIONS:
            if pub.monitor_chat and pub.chat_id:
                channels.append(f"chat:{pub.chat_id}:only_paid")
                channels.append(f"chat:{pub.chat_id}:all_subscribers")
        return channels
    
    def _get_token(self) -> Optional[str]:
        """Fetch JWT token for WebSocket authentication."""
        try:
            channels_param = quote(",".join(self._channels))
            url = f"{self.TOKEN_URL}?channels={channels_param}"
            r = self._session.get(url, timeout=15)
            
            if r.status_code == 200:
                data = r.json()
                token = data.get("token")
                if token:
                    self._token = token
                    self._token_time = time.time()
                    logger.info(f"🔑 Zync token acquired ({len(self._channels)} channels)")
                    return token
                else:
                    logger.error(f"Token response missing 'token' key: {list(data.keys())}")
            else:
                logger.error(f"Token request failed: {r.status_code}")
        except Exception as e:
            logger.error(f"Token error: {e}")
        return None
    
    def _needs_token_refresh(self) -> bool:
        return not self._token or (time.time() - self._token_time) > self.TOKEN_REFRESH_INTERVAL
    
    def _on_open(self, ws):
        """WebSocket connected - subscribe to channels."""
        logger.info("🔌 Zync WebSocket connected")
        self._connected = True
        self._reconnect_delay = 1  # Reset reconnect delay on success
        
        # Subscribe to each channel
        for channel in self._channels:
            subscribe_msg = json.dumps({
                "token": self._token,
                "action": "subscribe",
                "channel": channel
            })
            ws.send(subscribe_msg)
        
        logger.info(f"📡 Subscribed to {len(self._channels)} channels")
    
    def _on_message(self, ws, message):
        """Handle incoming WebSocket message."""
        try:
            data = json.loads(message)
            
            # Subscription confirmation
            if isinstance(data, dict) and data.get("data", {}).get("status") == "OK":
                return
            
            # Error messages
            if isinstance(data, dict) and data.get("error"):
                logger.warning(f"Zync error: {data['error']}")
                return
            
            # Route by channel
            channel = data.get("channel", "") if isinstance(data, dict) else ""
            
            if channel in self._chat_channels:
                logger.info(f"⚡ Chat push [{channel}]: {str(data)[:200]}")
                self.chat_queue.put(data)
            elif channel == self._user_channel:
                logger.info(f"⚡ Post/notification push [{channel}]: {str(data)[:200]}")
                self.post_queue.put(data)
            else:
                # Unknown channel or no channel field - put in both to be safe
                logger.info(f"⚡ Zync push (unrouted): {str(data)[:200]}")
                self.chat_queue.put(data)
                self.post_queue.put(data)
            
        except json.JSONDecodeError:
            logger.debug(f"Non-JSON WebSocket message: {message[:100]}")
        except Exception as e:
            logger.error(f"WebSocket message error: {e}")
    
    def _on_error(self, ws, error):
        logger.warning(f"Zync WebSocket error: {error}")
        self._connected = False
    
    def _on_close(self, ws, close_status_code, close_msg):
        logger.info(f"Zync WebSocket closed (code={close_status_code})")
        self._connected = False
    
    def _ws_thread(self):
        """Background thread: maintain WebSocket connection with auto-reconnect."""
        while self._running:
            try:
                # Get/refresh token
                if self._needs_token_refresh():
                    token = self._get_token()
                    if not token:
                        logger.error("Failed to get Zync token - retrying in 30s")
                        time.sleep(30)
                        continue
                
                # Connect WebSocket
                self._ws = ws_client.WebSocketApp(
                    self.WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                
                # Run until disconnected (blocks)
                self._ws.run_forever(
                    ping_interval=30,
                    ping_timeout=10,
                    reconnect=0,  # We handle reconnect ourselves
                )
                
            except Exception as e:
                logger.error(f"WebSocket thread error: {e}")
            
            # Disconnected - reconnect with backoff
            if self._running:
                logger.info(f"Reconnecting in {self._reconnect_delay}s...")
                time.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, 60)
    
    def start(self):
        """Start the WebSocket connection in a background thread."""
        if not HAS_WEBSOCKET:
            logger.warning("websocket-client not installed - WebSocket disabled")
            return False
        
        if not self.config.substack_sid:
            logger.warning("No session cookie - WebSocket disabled")
            return False
        
        self._running = True
        self._thread = threading.Thread(target=self._ws_thread, daemon=True)
        self._thread.start()
        logger.info("🚀 Zync WebSocket thread started")
        return True
    
    def stop(self):
        """Stop the WebSocket connection."""
        self._running = False
        if self._ws:
            self._ws.close()
    
    def is_connected(self) -> bool:
        return self._connected
    
    def get_chat_pushes(self) -> List[Dict]:
        """Drain chat push notifications."""
        messages = []
        while not self.chat_queue.empty():
            try:
                messages.append(self.chat_queue.get_nowait())
            except queue.Empty:
                break
        return messages
    
    def get_post_pushes(self) -> List[Dict]:
        """Drain post/article push notifications."""
        messages = []
        while not self.post_queue.empty():
            try:
                messages.append(self.post_queue.get_nowait())
            except queue.Empty:
                break
        return messages
    
    def has_any_push(self) -> bool:
        """Check if there are any pending pushes without draining."""
        return not self.chat_queue.empty() or not self.post_queue.empty()


class SubstackScraper:
    """Web scraper for Substack content."""
    
    def __init__(self, config: Config):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        if config.substack_sid:
            # URL-decode the cookie value if it's encoded
            from urllib.parse import unquote
            cookie_value = unquote(config.substack_sid)
            self.session.cookies.set("substack.sid", cookie_value, domain=".substack.com")
            logger.info(f"Session cookie set (length: {len(cookie_value)})")
    
    def get_rss_posts(self) -> List[Dict]:
        """Get posts from RSS feed."""
        try:
            feed = feedparser.parse(self.config.rss_feed_url)
            posts = []
            for entry in feed.entries[:20]:
                posts.append({
                    "id": entry.get("id", entry.get("link", "")),
                    "title": entry.get("title", ""),
                    "content": entry.get("summary", ""),
                    "url": entry.get("link", ""),
                    "published": entry.get("published", "")
                })
            return posts
        except Exception as e:
            logger.error(f"RSS error: {e}")
            return []
    
    def get_api_posts(self) -> List[Dict]:
        """Get posts via API."""
        try:
            url = f"{self.config.publication_url}/api/v1/posts"
            r = self.session.get(url, params={"limit": 20}, timeout=15)
            if r.status_code == 200:
                posts = []
                for p in r.json():
                    posts.append({
                        "id": str(p.get("id", "")),
                        "title": p.get("title", ""),
                        "content": p.get("truncated_body_text", p.get("subtitle", "")),
                        "url": p.get("canonical_url", ""),
                        "published": p.get("post_date", "")
                    })
                return posts
        except Exception as e:
            logger.error(f"API posts error: {e}")
        return []
    
    def get_notes(self) -> List[Dict]:
        """Get user notes via API."""
        try:
            url = f"https://substack.com/api/v1/user/{self.config.user_handle}/activity"
            r = self.session.get(url, params={"limit": 30}, timeout=15)
            if r.status_code == 200:
                notes = []
                for item in r.json().get("items", []):
                    if item.get("type") in ["note", "comment"]:
                        notes.append({
                            "id": str(item.get("id", "")),
                            "content": item.get("body", item.get("body_text", "")),
                            "url": f"https://substack.com/@{self.config.user_handle}/note/c-{item.get('id', '')}",
                            "published": item.get("post_date", "")
                        })
                return notes
        except Exception as e:
            logger.error(f"Notes error: {e}")
        return []
    
    def get_chat_messages(self) -> List[Dict]:
        """Get latest chat posts from ALL monitored publications. Single inbox call."""
        if not self.config.substack_sid:
            return []
        
        # Back off if rate limited
        if hasattr(self, '_rate_limited_until') and time.time() < self._rate_limited_until:
            return []
        
        try:
            # If we have a working endpoint, use it directly (fast path)
            if hasattr(self, '_working_endpoint') and self._working_endpoint:
                return self._poll_endpoint(self._working_endpoint)
            
            # Discovery: try endpoints
            if not hasattr(self, '_endpoints_to_try'):
                self._endpoints_to_try = [
                    "https://substack.com/api/v1/messages/inbox?paginate=true&tab=all",
                ]
                self._try_idx = 0
            
            while self._try_idx < len(self._endpoints_to_try):
                endpoint = self._endpoints_to_try[self._try_idx]
                
                r = self.session.get(endpoint, timeout=10)
                
                if r.status_code == 429:
                    logger.info(f"429 on {endpoint}, trying next")
                    self._try_idx += 1
                    continue
                
                if r.status_code in [404, 401, 403]:
                    logger.info(f"{r.status_code} on {endpoint}, trying next")
                    self._try_idx += 1
                    continue
                
                if r.status_code != 200:
                    logger.info(f"{r.status_code} on {endpoint}")
                    self._try_idx += 1
                    continue
                
                data = r.json()
                
                if isinstance(data, dict):
                    logger.info(f"✅ 200 from {endpoint}")
                
                messages = self._extract_monitored_posts(data, endpoint)
                
                if messages is not None:
                    self._working_endpoint = endpoint
                    logger.info(f"🎯 Locked in endpoint: {endpoint}")
                    return messages
                
                self._try_idx += 1
            
            logger.warning("All endpoints exhausted during discovery")
            self._try_idx = 0
            self._rate_limited_until = time.time() + 30
            return []
            
        except Exception as e:
            logger.error(f"Chat error: {e}")
            return []
    
    def _poll_endpoint(self, endpoint: str) -> List[Dict]:
        """Fast-path: poll a known-good endpoint with exponential backoff."""
        try:
            r = self.session.get(endpoint, timeout=10)
            
            if r.status_code == 429:
                # Exponential backoff: 30s, 60s, 120s, 240s, max 300s
                if not hasattr(self, '_consecutive_429s'):
                    self._consecutive_429s = 0
                self._consecutive_429s += 1
                
                backoff = min(30 * (2 ** (self._consecutive_429s - 1)), 300)
                
                # Only log every few times to avoid spam
                if self._consecutive_429s <= 3 or self._consecutive_429s % 10 == 0:
                    logger.warning(f"Rate limited (attempt #{self._consecutive_429s}) - backing off {backoff}s")
                
                self._rate_limited_until = time.time() + backoff
                return []
            
            if r.status_code != 200:
                return []
            
            # Success! Reset backoff counter
            if hasattr(self, '_consecutive_429s') and self._consecutive_429s > 0:
                logger.info(f"✅ Rate limit cleared after {self._consecutive_429s} retries - resuming fast polling")
                self._consecutive_429s = 0
            
            data = r.json()
            messages = self._extract_monitored_posts(data, endpoint)
            return messages if messages else []
            
        except Exception as e:
            logger.error(f"Poll error: {e}")
            return []
    
    def _match_thread_to_publication(self, thread: dict) -> Optional[PublicationConfig]:
        """Check if a thread matches any monitored publication."""
        thread_id = str(thread.get("id", ""))
        thread_name = (thread.get("name", "") or thread.get("title", "") or "").lower()
        
        for pub in PUBLICATIONS:
            # Match by chat ID (skip if no chat_id)
            if pub.chat_id and pub.chat_id in thread_id:
                return pub
            # Match by keywords in thread name
            for kw in pub.match_keywords:
                if kw in thread_name:
                    return pub
        return None
    
    def _extract_monitored_posts(self, data, endpoint: str):
        """Extract community posts from ALL monitored publications.
        Returns list of messages, or None if format not recognized."""
        
        posts = []
        
        if isinstance(data, dict):
            threads = data.get("threads", []) or data.get("conversations", []) or data.get("items", []) or []
            
            if threads:
                for thread in threads:
                    pub = self._match_thread_to_publication(thread)
                    if pub:
                        cp = thread.get("communityPost")
                        if cp and isinstance(cp, dict):
                            post = self._parse_community_post(cp, pub)
                            if post:
                                posts.append(post)
                                # Log discovery once per publication
                                log_key = f'_discovered_{pub.name}'
                                if not hasattr(self, log_key):
                                    setattr(self, log_key, True)
                                    logger.info(f"=== {pub.name.upper()} THREAD DISCOVERED ===")
                                    logger.info(f"  Thread ID: {thread.get('id')}")
                                    logger.info(f"  Post ID: {post['id']}")
                                    logger.info(f"  Author: {post['author']}")
                                    logger.info(f"  Preview: {post['content'][:200]}")
                                    # Discover user_id if not set
                                    cp_user = cp.get("user", {}) or {}
                                    if cp_user.get("id") and not pub.user_id:
                                        pub.user_id = str(cp_user["id"])
                                        logger.info(f"  Discovered user_id: {pub.user_id}")
                                    logger.info(f"=== END {pub.name.upper()} ===")
                return posts
            
            else:
                if not hasattr(self, '_format_logged'):
                    self._format_logged = True
                    logger.info(f"Unknown response format from {endpoint}: keys={list(data.keys())[:15]}")
                return None
        
        return None
    
    def _parse_community_post(self, cp: dict, pub: PublicationConfig) -> Optional[Dict]:
        """Parse a single community post into our message format."""
        user = cp.get("user", {}) or cp.get("author", {}) or {}
        handle = (user.get("handle") or "").lower()
        name = (user.get("name") or "").lower()
        user_id = str(cp.get("user_id", ""))
        
        # Check if this post is from the publication's author
        is_author = False
        for kw in pub.match_keywords:
            if kw in handle or kw in name:
                is_author = True
                break
        if pub.user_id and user_id == pub.user_id:
            is_author = True
        # Also match by publication_id
        pub_id = str(cp.get("publication_id", ""))
        if pub_id == pub.chat_id:
            is_author = True
        
        if not is_author:
            return None
        
        body = cp.get("body", "") or cp.get("body_html", "") or ""
        if not body:
            return None
        
        cp_id = str(cp.get("id", ""))
        author_name = user.get("name", pub.name)
        
        return {
            "id": cp_id,
            "content": body,
            "url": f"https://substack.com/chat/{pub.chat_id}",
            "published": cp.get("created_at", "") or cp.get("updated_at", ""),
            "author": author_name,
            "title": "",
            "source_pub": pub.name  # Track which publication this is from
        }
    
    def scrape_chat_page(self) -> List[Dict]:
        """Fallback: scrape chat page HTML."""
        if not HAS_BS4:
            return []
        
        try:
            url = f"https://substack.com/chat/{self.config.chat_id}"
            r = self.session.get(url, timeout=15)
            if r.status_code != 200:
                return []
            
            soup = BeautifulSoup(r.text, 'html.parser')
            messages = []
            
            # Look for message elements (structure may vary)
            for msg_div in soup.select('[data-testid="chat-message"], .chat-message, .message'):
                author_el = msg_div.select_one('.author, [data-testid="author"]')
                content_el = msg_div.select_one('.body, .content, [data-testid="body"]')
                
                if not author_el or not content_el:
                    continue
                
                author = author_el.get_text(strip=True).lower()
                if "citrini" in author:
                    content = content_el.get_text(strip=True)
                    messages.append({
                        "id": StateManager.hash(content),
                        "content": content,
                        "url": url,
                        "published": ""
                    })
            
            return messages
        except Exception as e:
            logger.error(f"Chat scrape error: {e}")
        return []


class CitriniMonitor:
    """Main monitor class."""
    
    def __init__(self, config: Config):
        self.config = config
        self.state = StateManager(config.state_file)
        self.detector = SignalDetector()
        self.discord = DiscordWebhook(config.discord_webhook)
        self.scraper = SubstackScraper(config)
        self.ai = DeepSeekAnalyzer(config.deepseek_api_key, config.deepseek_model)
        self.zync = ZyncWebSocket(config)
        self._ws_active = False
    
    def _suppress_existing(self):
        """Mark all existing content as seen without sending alerts (first-run only)."""
        count = 0
        
        # Suppress existing posts from all publications
        for pub in PUBLICATIONS:
            if not pub.monitor_posts:
                continue
            try:
                url = f"{pub.publication_url}/api/v1/posts"
                r = self.scraper.session.get(url, params={"limit": 20}, timeout=15)
                if r.status_code == 200:
                    for p in r.json():
                        item_id = str(p.get("id", ""))
                        self.state.mark_seen(f"Posts-{pub.name}:{item_id}")
                        count += 1
            except Exception as e:
                logger.error(f"Suppress posts error ({pub.name}): {e}")
        
        # Suppress existing chat messages (covers all publications via inbox)
        try:
            messages = self.scraper.get_chat_messages()
            for item in messages:
                content = f"{item.get('title', '')} {item.get('content', '')}"
                item_id = item.get("id") or StateManager.hash(content)
                self.state.mark_seen(f"Chat:{item_id}")
                count += 1
        except Exception as e:
            logger.error(f"Suppress chat error: {e}")
        
        logger.info(f"📋 Suppressed {count} existing items on first run")
    
    def process_items(self, items: List[Dict], source: str) -> int:
        new_alerts = 0
        
        for item in items:
            content = f"{item.get('title', '')} {item.get('content', '')}"
            item_id = item.get("id") or StateManager.hash(content)
            
            if self.state.is_seen(f"{source}:{item_id}"):
                continue
            
            # New item detected — run AI analysis
            raw_content = item.get("content", "")
            title = item.get("title", f"New {source}")
            url = item.get("url", "")
            
            logger.info(f"🆕 New {source} detected: {title[:80] or item_id[:20]}")
            
            # Run DeepSeek AI analysis
            ai_result = self.ai.analyze(raw_content, source)
            
            # Also run regex analysis as fallback
            regex_analysis = self.detector.analyze_content(content)
            
            # Determine author for Discord embed
            item_author = item.get("source_pub", item.get("author", source))
            
            if ai_result:
                # Send AI-powered alert
                if self.discord.send_ai_alert(source, title, raw_content, url, ai_result, regex_analysis, author=item_author):
                    new_alerts += 1
                    logger.info(f"✅ AI alert sent: {source} - {item_author} - {title[:50] or item_id[:20]}")
            else:
                # Fallback to regex-based alert if DeepSeek fails
                clean_content = re.sub(r'<[^>]+>', '', raw_content)[:1500]
                if self.discord.send_alert(source, title, clean_content, url, regex_analysis):
                    new_alerts += 1
                    logger.info(f"⚠️ Fallback alert sent (AI unavailable): {source} - {title[:50]}")
            
            # Rate limit Discord
            time.sleep(1)
            
            self.state.mark_seen(f"{source}:{item_id}")
        
        return new_alerts
    
    def check_posts(self, verbose: bool = True, only_unsubscribed: bool = False) -> int:
        if verbose:
            logger.info("Checking posts...")
        total = 0
        for pub in PUBLICATIONS:
            if not pub.monitor_posts:
                continue
            # If only_unsubscribed, skip pubs with chat access (they get WS pushes)
            if only_unsubscribed and pub.monitor_chat:
                continue
            # Get posts from each publication
            try:
                url = f"{pub.publication_url}/api/v1/posts"
                r = self.scraper.session.get(url, params={"limit": 10}, timeout=15)
                if r.status_code == 200:
                    posts = []
                    for p in r.json():
                        posts.append({
                            "id": str(p.get("id", "")),
                            "title": p.get("title", ""),
                            "content": p.get("truncated_body_text", p.get("subtitle", "")),
                            "url": p.get("canonical_url", ""),
                            "published": p.get("post_date", ""),
                            "author": pub.name,
                            "source_pub": pub.name
                        })
                    total += self.process_items(posts, f"Posts-{pub.name}")
            except Exception as e:
                if verbose:
                    logger.error(f"Posts error for {pub.name}: {e}")
        return total
    
    def check_notes(self, verbose: bool = True) -> int:
        if verbose:
            logger.info("Checking notes...")
        total = 0
        for pub in PUBLICATIONS:
            try:
                url = f"https://substack.com/api/v1/user/{pub.user_handle}/activity"
                r = self.scraper.session.get(url, params={"limit": 10}, timeout=15)
                if r.status_code == 200:
                    notes = []
                    for item in r.json().get("items", []):
                        if item.get("type") in ["note", "comment"]:
                            notes.append({
                                "id": str(item.get("id", "")),
                                "content": item.get("body", item.get("body_text", "")),
                                "url": f"https://substack.com/@{pub.user_handle}/note/c-{item.get('id', '')}",
                                "published": item.get("post_date", ""),
                                "author": pub.name,
                                "source_pub": pub.name
                            })
                    total += self.process_items(notes, f"Notes-{pub.name}")
            except Exception as e:
                if verbose:
                    logger.error(f"Notes error for {pub.name}: {e}")
        return total
    
    def check_chat(self, verbose: bool = True) -> int:
        if verbose:
            logger.info("Checking chat...")
        
        # Strategy: WebSocket tells us WHEN, HTTP tells us WHAT
        if self._ws_active and self.zync.is_connected():
            # Check for WebSocket push notifications
            ws_pushes = self.zync.get_chat_pushes()
            
            if ws_pushes:
                # Got push notification(s) - fetch full content via HTTP
                logger.info(f"⚡ {len(ws_pushes)} chat push(es) - fetching content")
                messages = self.scraper.get_chat_messages()
                return self.process_items(messages, "Chat")
            else:
                # WebSocket connected but no new pushes - nothing to do
                return 0
        else:
            # WebSocket not available - fall back to HTTP polling
            messages = self.scraper.get_chat_messages()
            return self.process_items(messages, "Chat")
    
    def run_once(self) -> Dict[str, int]:
        return {
            "posts": self.check_posts(),
            "notes": self.check_notes(),
            "chat": self.check_chat()
        }
    
    def run_forever(self):
        logger.info("🚀 Starting Substack Research Monitor")
        for pub in PUBLICATIONS:
            modes = []
            if pub.monitor_chat and pub.chat_id:
                modes.append("⚡chat")
            if pub.monitor_posts:
                modes.append("📰posts")
            logger.info(f"  📡 {pub.name} (chat={pub.chat_id or 'none'}, {', '.join(modes)})")
        logger.info(f"Chat access: {'Enabled' if self.config.substack_sid else 'Limited (no cookie)'}")
        logger.info(f"WebSocket: {'Available' if HAS_WEBSOCKET else 'Not installed (pip install websocket-client)'}")
        logger.info(f"HTTP fallback polling: {self.config.poll_interval_market_hours}s")
        logger.info("Off-hours: sleeping (no polling)")
        
        # First-run suppression: if no state file exists (fresh deploy),
        # do an initial scan and mark everything as seen without alerting
        if not self.state.state["seen"]:
            logger.info("📋 First run detected - marking existing content as seen (no alerts)")
            self._suppress_existing()
        
        pub_list = "\n".join([f"  📡 {p.name}" for p in PUBLICATIONS])
        ws_status = "⚡ WebSocket (instant)" if HAS_WEBSOCKET else "🔄 HTTP polling (2s)"
        self.discord.send(
            "🚀 **Substack Research Monitor Started**\n"
            f"Monitoring:\n{pub_list}\n"
            f"⏰ Market hours: {ws_status}\n"
            "🌙 Off-hours: sleeping\n"
            "🤖 AI analysis: DeepSeek enabled"
        )
        
        last_check = 0
        last_notes_check = 0
        last_posts_check = 0  # Full safety net check
        last_unsub_posts_check = 0  # Fast poll for unsubscribed pubs (Bear Cave)
        was_market_hours = None
        check_count = 0
        
        UNSUB_POSTS_INTERVAL = 60  # Poll unsubscribed pubs every 60s
        SAFETY_NET_INTERVAL = 300  # Full check every 5 min as safety net
        
        # If starting during market hours, start WebSocket immediately
        if is_market_hours(self.config) and HAS_WEBSOCKET:
            logger.info("📈 Starting during market hours - launching WebSocket")
            self._ws_active = self.zync.start()
            if self._ws_active:
                logger.info("⚡ WebSocket mode: instant push notifications")
                # Give WebSocket a moment to connect before first check
                time.sleep(2)
        
        while True:
            try:
                in_market_hours = is_market_hours(self.config)
                
                # Log when market hours status changes
                if was_market_hours is not None and in_market_hours != was_market_hours:
                    if in_market_hours:
                        # Market open - start WebSocket
                        logger.info("📈 Market hours started")
                        if HAS_WEBSOCKET:
                            self._ws_active = self.zync.start()
                            if self._ws_active:
                                logger.info("⚡ WebSocket mode: instant push notifications")
                                self.discord.send("📈 **Market Open** - ⚡ WebSocket connected (instant alerts)")
                            else:
                                logger.info("🔄 WebSocket failed - falling back to HTTP polling (2s)")
                                self.discord.send("📈 **Market Open** - 🔄 HTTP polling (every 2s)")
                        else:
                            self.discord.send("📈 **Market Open** - 🔄 HTTP polling (every 2s)")
                        check_count = 0
                    else:
                        # Market close - stop WebSocket
                        logger.info(f"🌙 Market closed (completed {check_count} checks)")
                        if self._ws_active:
                            self.zync.stop()
                            self._ws_active = False
                        self.discord.send(f"🌙 **Market Closed** - Sleeping\n📊 {check_count} checks this session")
                
                was_market_hours = in_market_hours
                
                # Only poll during market hours
                if in_market_hours:
                    now = time.time()
                    
                    if self._ws_active and self.zync.is_connected():
                        # === WebSocket mode ===
                        check_count += 1
                        verbose = (check_count % 3000 == 0)
                        
                        # 1) Chat pushes → instant
                        self.check_chat(verbose)
                        
                        # 2) Post pushes (user channel) → instant for subscribed pubs
                        post_pushes = self.zync.get_post_pushes()
                        if post_pushes:
                            logger.info(f"⚡ {len(post_pushes)} post push(es) - checking all subscribed articles")
                            self.check_posts(verbose=True)
                            last_posts_check = now  # Reset safety net timer
                        
                        # 3) Fast-poll unsubscribed pubs (Bear Cave) every 60s
                        if now - last_unsub_posts_check >= UNSUB_POSTS_INTERVAL:
                            self.check_posts(verbose=False, only_unsubscribed=True)
                            last_unsub_posts_check = now
                        
                        # 4) Safety net: full check every 5 min
                        if now - last_posts_check >= SAFETY_NET_INTERVAL:
                            self.check_posts(verbose=False)
                            last_posts_check = now
                        if now - last_notes_check >= SAFETY_NET_INTERVAL:
                            self.check_notes(False)
                            last_notes_check = now
                        
                        if verbose:
                            logger.info(f"📊 {check_count} checks | ⚡ WebSocket active")
                        
                        time.sleep(0.1)  # 100ms - just checking in-memory queues
                    else:
                        # === HTTP fallback mode ===
                        if self._ws_active and not self.zync.is_connected():
                            if check_count % 100 == 0:
                                logger.warning("⚠️ WebSocket disconnected - using HTTP polling")
                        
                        if now - last_check >= self.config.poll_interval_market_hours:
                            check_count += 1
                            verbose = (check_count % 100 == 0)
                            
                            self.check_chat(verbose)
                            
                            # Fast-poll unsubscribed pubs every 60s
                            if now - last_unsub_posts_check >= UNSUB_POSTS_INTERVAL:
                                self.check_posts(verbose=False, only_unsubscribed=True)
                                last_unsub_posts_check = now
                            
                            if now - last_notes_check >= SAFETY_NET_INTERVAL:
                                self.check_notes(verbose)
                                last_notes_check = now
                            if now - last_posts_check >= SAFETY_NET_INTERVAL:
                                self.check_posts(verbose)
                                last_posts_check = now
                            
                            last_check = now
                            
                            if verbose:
                                logger.info(f"📊 {check_count} checks | 🔄 HTTP polling")
                        
                        time.sleep(0.5)
                else:
                    # Outside market hours - sleep for 60 seconds then check again
                    time.sleep(60)
                
            except KeyboardInterrupt:
                logger.info("Shutting down...")
                break
            except Exception as e:
                logger.error(f"Error: {e}")
                time.sleep(60)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Citrini Research Monitor")
    parser.add_argument("--once", action="store_true", help="Run once")
    parser.add_argument("--test", action="store_true", help="Test Discord webhook")
    parser.add_argument("--webhook", help="Discord webhook URL")
    parser.add_argument("--cookie", help="Substack session cookie")
    args = parser.parse_args()
    
    config = Config()
    if args.webhook:
        config.discord_webhook = args.webhook
    if args.cookie:
        config.substack_sid = args.cookie
    
    monitor = CitriniMonitor(config)
    
    if args.test:
        print("Testing Discord webhook...")
        if monitor.discord.send("🧪 **Test** - Citrini Monitor webhook working!"):
            print("✅ Success!")
        else:
            print("❌ Failed")
        return
    
    if args.once:
        results = monitor.run_once()
        print(f"Results: {results}")
    else:
        monitor.run_forever()


if __name__ == "__main__":
    main()
