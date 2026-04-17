"""Concrete source adapters for Leads Finder."""

from .base import BaseSourceAdapter
from .dev_to import DevToAdapter
from .github_issues import GitHubIssuesAdapter
from .hacker_news import HackerNewsAdapter
from .reddit import RedditAdapter
from .x import XAdapter

__all__ = [
    "BaseSourceAdapter",
    "DevToAdapter",
    "GitHubIssuesAdapter",
    "HackerNewsAdapter",
    "RedditAdapter",
    "XAdapter",
]
