from __future__ import annotations

from dataclasses import dataclass
import os

from backend.adapters import (
    DevToAdapter,
    GitHubIssuesAdapter,
    HackerNewsAdapter,
    RedditAdapter,
    XAdapter,
)
from backend.contracts.enums import LeadSource
from backend.contracts.source_adapter import SourceAdapterConfig


@dataclass(slots=True)
class SourceRegistry:
    reddit: RedditAdapter | None = None
    x: XAdapter | None = None
    github_issues: GitHubIssuesAdapter | None = None
    hacker_news: HackerNewsAdapter | None = None
    dev_to: DevToAdapter | None = None

    def enabled_adapters(self) -> list[object]:
        adapters: list[object] = []
        for adapter in [
            self.reddit,
            self.x,
            self.github_issues,
            self.hacker_news,
            self.dev_to,
        ]:
            if adapter is not None:
                adapters.append(adapter)
        return adapters


def build_default_registry() -> SourceRegistry:
    def env_enabled(name: str, default: bool) -> bool:
        value = os.getenv(name)
        if value is None:
            return default
        return value.strip().lower() in {"1", "true", "yes", "on"}

    return SourceRegistry(
        reddit=RedditAdapter(
            SourceAdapterConfig(
                source=LeadSource.REDDIT,
                enabled=env_enabled("LEADS_ENABLE_REDDIT", True),
            )
        ),
        x=XAdapter(
            SourceAdapterConfig(
                source=LeadSource.X,
                enabled=env_enabled("LEADS_ENABLE_X", False),
            )
        ),
        github_issues=GitHubIssuesAdapter(
            SourceAdapterConfig(
                source=LeadSource.GITHUB_ISSUES,
                enabled=env_enabled("LEADS_ENABLE_GITHUB_ISSUES", False),
            )
        ),
        hacker_news=HackerNewsAdapter(
            SourceAdapterConfig(
                source=LeadSource.HACKER_NEWS,
                enabled=env_enabled("LEADS_ENABLE_HACKER_NEWS", False),
            )
        ),
        dev_to=DevToAdapter(
            SourceAdapterConfig(
                source=LeadSource.DEV_TO,
                enabled=env_enabled("LEADS_ENABLE_DEV_TO", False),
            )
        ),
    )