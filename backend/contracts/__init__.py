"""Core product contracts for Leads Finder Phase 1."""

from .enums import LeadSource, LeadStatus, SourceHealth
from .lead_schema import (
    DedupMetadata,
    ExtractedEntities,
    LeadRecord,
    LeadTrace,
    ManualReviewState,
)
from .ranking import LeadScoreBreakdown, RankingWeights
from .source_adapter import (
    FetchPageRequest,
    FetchPageResult,
    RateLimitConfig,
    RetryPolicy,
    SourceAdapter,
    SourceAdapterConfig,
    SourceHealthStatus,
)

__all__ = [
    "DedupMetadata",
    "ExtractedEntities",
    "FetchPageRequest",
    "FetchPageResult",
    "LeadRecord",
    "LeadScoreBreakdown",
    "LeadSource",
    "LeadStatus",
    "LeadTrace",
    "ManualReviewState",
    "RankingWeights",
    "RateLimitConfig",
    "RetryPolicy",
    "SourceAdapter",
    "SourceAdapterConfig",
    "SourceHealth",
    "SourceHealthStatus",
]
