"""Persistence layer for Leads Finder."""

from .repository import InMemoryLeadRepository, LeadRepository

__all__ = ["InMemoryLeadRepository", "LeadRepository"]
