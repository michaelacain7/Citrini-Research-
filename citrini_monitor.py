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
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any
from dataclasses import dataclass
from pathlib import Path

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
    chat_id: str
    user_handle: str
    user_id: str  # Numeric user ID for matching
    publication_url: str
    rss_feed_url: str
    match_keywords: list  # Keywords to match in thread name


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
    ),
    PublicationConfig(
        name="Cassandra Unchained",
        chat_id="6819723",
        user_handle="michaeljburry",
        user_id="",  # Will be discovered
        publication_url="https://michaeljburry.substack.com",
        rss_feed_url="https://michaeljburry.substack.com/feed",
        match_keywords=["cassandra", "burry", "michaeljburry"],
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
    market_close_hour: int = 23  # 11 PM EST (change to 16 for production)
    market_close_minute: int = 0
    
    state_file: str = "citrini_state.json"
    
    # DeepSeek API for AI analysis
    deepseek_api_key: str = os.getenv(
        "DEEPSEEK_API_KEY",
        "sk-1cf5b2ab46a14eb6978ff7ba7ce3f3e3"
    )
    deepseek_model: str = "deepseek-chat"
    
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
- For Michael Burry posts, pay special attention to his contrarian thesis and macro views"""

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
        """Fast-path: poll a known-good endpoint."""
        try:
            r = self.session.get(endpoint, timeout=10)
            
            if r.status_code == 429:
                logger.warning(f"Rate limited on {endpoint} - backing off 15s")
                self._rate_limited_until = time.time() + 15
                return []
            
            if r.status_code != 200:
                return []
            
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
            # Match by chat ID
            if pub.chat_id in thread_id:
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
    
    def _suppress_existing(self):
        """Mark all existing content as seen without sending alerts (first-run only)."""
        count = 0
        
        # Suppress existing posts from all publications
        for pub in PUBLICATIONS:
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
    
    def check_posts(self, verbose: bool = True) -> int:
        if verbose:
            logger.info("Checking posts...")
        total = 0
        for pub in PUBLICATIONS:
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
            logger.info(f"  📡 {pub.name} (chat={pub.chat_id}, handle={pub.user_handle})")
        logger.info(f"Chat access: {'Enabled' if self.config.substack_sid else 'Limited (no cookie)'}")
        logger.info(f"Market hours polling: {self.config.poll_interval_market_hours}s")
        logger.info("Off-hours: sleeping (no polling)")
        
        # First-run suppression: if no state file exists (fresh deploy),
        # do an initial scan and mark everything as seen without alerting
        if not self.state.state["seen"]:
            logger.info("📋 First run detected - marking existing content as seen (no alerts)")
            self._suppress_existing()
        
        pub_list = "\n".join([f"  📡 {p.name}" for p in PUBLICATIONS])
        self.discord.send(
            "🚀 **Substack Research Monitor Started**\n"
            f"Monitoring:\n{pub_list}\n"
            f"⏰ Market hours: polling every {self.config.poll_interval_market_hours}s\n"
            "🌙 Off-hours: sleeping\n"
            "🤖 AI analysis: DeepSeek enabled"
        )
        
        last_check = 0
        last_notes_check = 0
        last_posts_check = 0
        was_market_hours = None
        check_count = 0
        
        while True:
            try:
                in_market_hours = is_market_hours(self.config)
                
                # Log when market hours status changes
                if was_market_hours is not None and in_market_hours != was_market_hours:
                    if in_market_hours:
                        logger.info("📈 Market hours started - beginning fast polling (2s)")
                        self.discord.send("📈 **Market Open** - Starting fast polling (every 2 seconds)")
                        check_count = 0  # Reset check count for new session
                    else:
                        logger.info(f"🌙 Market closed - sleeping until next open (completed {check_count} checks)")
                        self.discord.send(f"🌙 **Market Closed** - Sleeping until next market open\n📊 Completed {check_count} checks this session")
                
                was_market_hours = in_market_hours
                
                # Only poll during market hours
                if in_market_hours:
                    now = time.time()
                    
                    if now - last_check >= self.config.poll_interval_market_hours:
                        check_count += 1
                        # Only log every 100 checks (~3.3 minutes)
                        verbose = (check_count % 100 == 0)
                        
                        # Chat every 2s - single API call
                        self.check_chat(verbose)
                        
                        # Notes every 5 min
                        if now - last_notes_check >= 300:
                            self.check_notes(verbose)
                            last_notes_check = now
                        
                        # Posts every 5 min
                        if now - last_posts_check >= 300:
                            self.check_posts(verbose)
                            last_posts_check = now
                        
                        last_check = now
                        
                        if verbose:
                            logger.info(f"📊 Completed {check_count} checks this session")
                    
                    time.sleep(0.5)  # Short sleep for responsive 2s checks
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
