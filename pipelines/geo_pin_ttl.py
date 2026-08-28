"""Expire geo pins 48h from the article timestamp, not from pin time."""
import json
import os
from datetime import datetime, timezone
from dateutil import parser as dateutil_parser

from utils.logger import pulse_logger


def apply_pin_ttl(pipeline):
    """Patch GeopoliticalPipeline so pins expire 48h from article time."""
    cls = type(pipeline)
    if getattr(cls, "_pin_ttl_patched", False):
        return

    from pipelines.geopolitical import MAX_ARTICLE_AGE_HOURS

    def _pin_ttl_timestamp(story):
        for field in ("published_at", "timestamp", "date"):
            val = (story.get(field) or "").strip()
            if val:
                return val
        return ""

    def _pin_is_expired(self, story):
        ts = _pin_ttl_timestamp(story)
        if not ts:
            return True
        try:
            dt = dateutil_parser.parse(
                ts,
                default=datetime.now(timezone.utc),
                tzinfos={"EST": -18000, "EDT": -14400},
            )
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            age_hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
            return age_hours > MAX_ARTICLE_AGE_HOURS
        except Exception:
            return True

    def load_pinned_stories(self):
        try:
            if not os.path.exists(self.pinned_store_file):
                return []
            with open(self.pinned_store_file, "r") as f:
                pinned = json.load(f)
            manual_blocked = self._load_manual_blocklist_titles()
            blocklist = self._load_blocklist_strings()
            valid = []
            dirty = False
            for story in pinned:
                headline = story.get("headline", "")
                if manual_blocked and headline.lower() in manual_blocked:
                    pulse_logger.log(f"🚫 Manually blocked by user (pinned): {headline[:80]}")
                    dirty = True
                    continue
                if blocklist:
                    headline_lower = headline.lower()
                    matched = [b for b in blocklist if b in headline_lower]
                    if matched:
                        pulse_logger.log(
                            f"🚫 Blocked by blocklist (pinned): {headline[:80]} | matched: {matched[0][:60]}"
                        )
                        dirty = True
                        continue
                if self._pin_is_expired(story):
                    pulse_logger.log(f"🕐 Age cutoff (pinned, article timestamp): {headline[:80]}")
                    dirty = True
                    continue
                valid.append(story)
            if dirty:
                self.save_pinned_stories(valid)
            return valid
        except Exception as e:
            pulse_logger.log(f"⚠️ Failed to load pinned stories: {e}", level="WARNING")
            return []

    orig_update = cls.update_pinned_store

    def update_pinned_store(self, new_items, classifications):
        orig_update(self, new_items, classifications)
        by_hl = {}
        for article in new_items or []:
            hl = article.get("headline", "")
            if not hl:
                continue
            by_hl[hl] = (
                (article.get("published_at") or "").strip()
                or (article.get("timestamp") or "").strip()
                or (article.get("date") or "").strip()
            )
        try:
            if not os.path.exists(self.pinned_store_file):
                return
            with open(self.pinned_store_file, "r") as f:
                pinned = json.load(f)
            dirty = False
            for pin in pinned:
                if (pin.get("published_at") or "").strip():
                    continue
                val = by_hl.get(pin.get("headline", ""), "")
                if val:
                    pin["published_at"] = val
                    dirty = True
            if dirty:
                self.save_pinned_stories(pinned)
        except Exception as e:
            pulse_logger.log(f"⚠️ Pin published_at backfill failed: {e}", level="WARNING")

    orig_purge = cls._purge_blocked_from_cache

    def _purge_blocked_from_cache(self):
        orig_purge(self)
        try:
            if not os.path.exists(self.pinned_store_file):
                return
            with open(self.pinned_store_file, "r") as f:
                pinned = json.load(f)
            kept = []
            aged = 0
            for story in pinned:
                if self._pin_is_expired(story):
                    pulse_logger.log(
                        f"🗑️ Force-removed pinned article (>48h from article timestamp): {story.get('headline', '')}"
                    )
                    aged += 1
                    continue
                kept.append(story)
            if aged:
                self.save_pinned_stories(kept)
                pulse_logger.log(
                    f"🧹 Pin TTL patch — aged out {aged} pin(s) from article timestamp"
                )
        except Exception as e:
            pulse_logger.log(f"⚠️ Pin TTL extra purge failed: {e}", level="WARNING")

    orig_fetch = cls.fetch

    def fetch(self, *args, **kwargs):
        data = orig_fetch(self, *args, **kwargs)
        if not isinstance(data, dict):
            return data
        for key in ("all_items", "news_items"):
            for item in data.get(key) or []:
                if not (item.get("published_at") or "").strip():
                    item["published_at"] = (
                        (item.get("timestamp") or "").strip()
                        or (item.get("date") or "").strip()
                    )
        return data

    cls._pin_ttl_timestamp = staticmethod(_pin_ttl_timestamp)
    cls._pin_is_expired = _pin_is_expired
    cls.load_pinned_stories = load_pinned_stories
    cls.update_pinned_store = update_pinned_store
    cls._purge_blocked_from_cache = _purge_blocked_from_cache
    cls.fetch = fetch
    cls._pin_ttl_patched = True
    pulse_logger.log("📌 Geo pin TTL — expire 48h from article timestamp, not pin time")
