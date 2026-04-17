from __future__ import annotations

from dataclasses import dataclass

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
    return SourceRegistry(
        reddit=RedditAdapter(SourceAdapterConfig(source=LeadSource.REDDIT, enabled=True)),
        x=XAdapter(SourceAdapterConfig(source=LeadSource.X, enabled=False)),
        github_issues=GitHubIssuesAdapter(
            SourceAdapterConfig(source=LeadSource.GITHUB_ISSUES, enabled=True)
        ),
        hacker_news=HackerNewsAdapter(
            SourceAdapterConfig(source=LeadSource.HACKER_NEWS, enabled=True)
        ),
        dev_to=DevToAdapter(SourceAdapterConfig(source=LeadSource.DEV_TO, enabled=True)),
    )