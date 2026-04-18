from enum import Enum


class LeadSource(str, Enum):
    REDDIT = "reddit"
    X = "x"
    GITHUB_ISSUES = "github_issues"
    HACKER_NEWS = "hacker_news"
    DEV_TO = "dev_to"


class LeadStatus(str, Enum):
    NEW = "new"
    QUEUED = "queued"
    SCORED = "scored"
    FAILED = "failed"
    REVIEWED = "reviewed"
    ARCHIVED = "archived"
    EXPORTED = "exported"


class SourceHealth(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    RATE_LIMITED = "rate_limited"
    COOLING_DOWN = "cooling_down"
    OFFLINE = "offline"
