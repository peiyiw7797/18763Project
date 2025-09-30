from __future__ import annotations

import json
import logging
import re
from collections import Counter
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

LOGGER = logging.getLogger(__name__)

DEFAULT_SEARCH_TERMS = [
    "fifa world cup",
    "soccer",
    "football",
    "fifa",
]


@dataclass
class Comment:
    """Representation of a YouTube comment."""

    video_id: str
    comment_id: str
    author: str
    text: str

    def to_json(self) -> str:
        return json.dumps(self.__dict__, ensure_ascii=False)


class CommentPublisher:
    """Fetch comments from YouTube and publish them to subscribers."""

    def __init__(
        self,
        *,
        api_key: str,
        search_terms: Optional[Iterable[str]] = None,
        max_results_per_term: int = 25,
    ) -> None:
        self.api_key = api_key
        self.search_terms = list(search_terms or DEFAULT_SEARCH_TERMS)
        self.max_results_per_term = max_results_per_term
        self.youtube = build("youtube", "v3", developerKey=api_key, cache_discovery=False)

    def iter_video_ids(self) -> Iterator[str]:
        seen: set[str] = set()
        for term in self.search_terms:
            try:
                request = self.youtube.search().list(
                    q=term,
                    part="id",
                    type="video",
                    maxResults=min(self.max_results_per_term, 50),
                    order="date",
                    relevanceLanguage="en",
                )
                response = request.execute()
            except HttpError as exc:
                LOGGER.warning("YouTube search failed for term '%s': %s", term, exc)
                continue
            for item in response.get("items", []):
                video_id = item.get("id", {}).get("videoId")
                if video_id and video_id not in seen:
                    seen.add(video_id)
                    yield video_id

    def iter_comments(self, limit: Optional[int] = None) -> Iterator[Comment]:
        total = 0
        for video_id in self.iter_video_ids():
            page_token: Optional[str] = None
            while True:
                try:
                    request = self.youtube.commentThreads().list(
                        part="snippet",
                        videoId=video_id,
                        maxResults=100,
                        pageToken=page_token,
                        textFormat="plainText",
                        order="relevance",
                    )
                    response = request.execute()
                except HttpError as exc:
                    LOGGER.warning("Failed to fetch comments for %s: %s", video_id, exc)
                    break

                for item in response.get("items", []):
                    snippet = item.get("snippet", {}).get("topLevelComment", {}).get("snippet", {})
                    comment = Comment(
                        video_id=video_id,
                        comment_id=item.get("id", ""),
                        author=snippet.get("authorDisplayName", "unknown"),
                        text=snippet.get("textDisplay", ""),
                    )
                    yield comment
                    total += 1
                    if limit is not None and total >= limit:
                        return

                page_token = response.get("nextPageToken")
                if not page_token:
                    break


class CommentSubscriber:
    """Consume comments and count mentions of player aliases."""

    def __init__(
        self,
        *,
        alias_map: Dict[str, str],
        dump_path: Optional[str] = None,
    ) -> None:
        self.alias_map = {
            alias.lower(): canonical for alias, canonical in alias_map.items()
        }
        self.patterns: Dict[str, re.Pattern[str]] = {
            alias: re.compile(rf"\b{re.escape(alias)}\b", re.IGNORECASE)
            for alias in self.alias_map
        }
        self.dump_path = Path(dump_path) if dump_path else None

    def process(self, comments: Iterable[Comment]) -> Counter:
        counts: Counter = Counter()
        with ExitStack() as stack:
            handle = (
                stack.enter_context(self.dump_path.open("w", encoding="utf-8"))
                if self.dump_path
                else None
            )
            for comment in comments:
                if handle:
                    handle.write(comment.to_json() + "\n")
                text = comment.text.lower()
                for alias, pattern in self.patterns.items():
                    if pattern.search(text):
                        canonical = self.alias_map[alias]
                        counts[canonical] += 1
        return counts


__all__ = [
    "Comment",
    "CommentPublisher",
    "CommentSubscriber",
]
