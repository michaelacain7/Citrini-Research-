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
from datetime import datetime
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
class Config:
    """Configuration settings."""
    discord_webhook: str = os.getenv(
        "DISCORD_WEBHOOK",
        "https://discordapp.com/api/webhooks/919672540237017138/Zga2QHBVwPUKXbCMNQ6hRXSsJaW8d136pOZNheRz1SK0YS5GIRnpjsGdN7trPul-zeXo"
    )
    publication_url: str = "https://www.citriniresearch.com"
    rss_feed_url: str = "https://www.citriniresearch.com/feed"
    chat_id: str = "836125"
    user_handle: str = "citrini"
    substack_sid: str = os.getenv(
        "SUBSTACK_SID",
        "s%3AQJJmXar5y93BQEUsRGywS92ySuYdcdcT.godFCcejHCwFeME7%2FQoZKkwQKBKVJWtlTqXx6N5R1hg"
    )
    # Polling during market hours (9:30 AM - 4:00 PM EST)
    poll_interval_market_hours: int = 2  # 2 seconds during market hours
    
    # Market hours (EST)
    market_open_hour: int = 9
    market_open_minute: int = 30
    market_close_hour: int = 16
    market_close_minute: int = 0
    
    state_file: str = "citrini_state.json"


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
            "timestamp": datetime.utcnow().isoformat()
        }
        
        if analysis["signal_type"]:
            embed["fields"].append({"name": "🎯 Signal", "value": analysis["signal_type"], "inline": True})
        if analysis["tickers"]:
            embed["fields"].append({"name": "📌 Tickers", "value": ", ".join(analysis["tickers"][:15]), "inline": True})
        embed["fields"].append({"name": "💭 Sentiment", "value": analysis["sentiment"]["sentiment"], "inline": True})
        
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
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        if config.substack_sid:
            self.session.cookies.set("substack.sid", config.substack_sid, domain=".substack.com")
    
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
        """Get chat messages via API."""
        if not self.config.substack_sid:
            logger.warning("No session cookie - chat access limited")
            return []
        
        try:
            url = f"https://substack.com/api/v1/chat/{self.config.chat_id}/messages"
            r = self.session.get(url, params={"limit": 50}, timeout=15)
            if r.status_code == 200:
                messages = []
                for msg in r.json().get("messages", []):
                    author = msg.get("author", {})
                    handle = author.get("handle", "").lower()
                    name = author.get("name", "").lower()
                    
                    # Filter to only Citrini's messages
                    if self.config.user_handle.lower() in [handle, name] or "citrini" in name:
                        messages.append({
                            "id": str(msg.get("id", "")),
                            "content": msg.get("body", ""),
                            "url": f"https://substack.com/chat/{self.config.chat_id}",
                            "published": msg.get("created_at", ""),
                            "author": author.get("name", "Citrini")
                        })
                return messages
            elif r.status_code == 401:
                logger.warning("Chat access denied - check session cookie")
        except Exception as e:
            logger.error(f"Chat error: {e}")
        return []
    
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
    
    def process_items(self, items: List[Dict], source: str) -> int:
        new_alerts = 0
        
        for item in items:
            content = f"{item.get('title', '')} {item.get('content', '')}"
            item_id = item.get("id") or StateManager.hash(content)
            
            if self.state.is_seen(f"{source}:{item_id}"):
                continue
            
            analysis = self.detector.analyze_content(content)
            
            # Alert if actionable OR has sentiment OR mentions tickers
            if analysis["is_actionable"] or analysis["tickers"] or analysis["sentiment"]["sentiment"] != "NEUTRAL":
                # Clean HTML
                clean_content = re.sub(r'<[^>]+>', '', item.get("content", ""))[:1500]
                title = item.get("title", f"New {source}")
                
                if self.discord.send_alert(source, title, clean_content, item.get("url", ""), analysis):
                    new_alerts += 1
                    logger.info(f"Alert sent: {source} - {title[:50]}")
                
                # Rate limit
                time.sleep(1)
            
            self.state.mark_seen(f"{source}:{item_id}")
        
        return new_alerts
    
    def check_posts(self, verbose: bool = True) -> int:
        if verbose:
            logger.info("Checking posts...")
        posts = self.scraper.get_api_posts() or self.scraper.get_rss_posts()
        return self.process_items(posts, "Posts")
    
    def check_notes(self, verbose: bool = True) -> int:
        if verbose:
            logger.info("Checking notes...")
        notes = self.scraper.get_notes()
        return self.process_items(notes, "Notes")
    
    def check_chat(self, verbose: bool = True) -> int:
        if verbose:
            logger.info("Checking chat...")
        messages = self.scraper.get_chat_messages()
        if not messages:
            messages = self.scraper.scrape_chat_page()
        return self.process_items(messages, "Chat")
    
    def run_once(self) -> Dict[str, int]:
        return {
            "posts": self.check_posts(),
            "notes": self.check_notes(),
            "chat": self.check_chat()
        }
    
    def run_forever(self):
        logger.info("🚀 Starting Citrini Research Monitor")
        logger.info(f"Publication: {self.config.publication_url}")
        logger.info(f"Chat access: {'Enabled' if self.config.substack_sid else 'Limited (no cookie)'}")
        logger.info(f"Market hours polling: {self.config.poll_interval_market_hours}s")
        logger.info("Off-hours: sleeping (no polling)")
        
        self.discord.send(
            "🚀 **Citrini Monitor Started**\n"
            "Monitoring posts, notes, and chat for trading signals.\n"
            f"⏰ Market hours (9:30 AM - 4:00 PM EST): polling every {self.config.poll_interval_market_hours}s\n"
            "🌙 Off-hours: sleeping (no activity)"
        )
        
        last_check = 0
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
                        
                        self.check_chat(verbose)   # Chat is most time-sensitive
                        self.check_notes(verbose)  # Notes second
                        self.check_posts(verbose)  # Posts less frequent
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
