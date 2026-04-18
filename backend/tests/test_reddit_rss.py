from __future__ import annotations

import unittest

from backend.adapters.reddit import _parse_atom_feed
from backend.ingestion.registry import build_default_registry


class RedditRssTests(unittest.TestCase):
    def test_parse_atom_feed_extracts_posts(self) -> None:
        feed_xml = """<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>r/forhire</title>
  <entry>
    <id>tag:reddit.com,2005:/r/forhire/comments/abc123/python_developer_needed/</id>
    <title>[Hiring] Python developer for API integration</title>
    <updated>2026-04-19T12:00:00+00:00</updated>
    <author><name>alice</name></author>
    <link rel="alternate" href="https://www.reddit.com/r/forhire/comments/abc123/python_developer_needed/" />
    <summary type="html"><![CDATA[Need help with a FastAPI integration bug and want a developer.]]></summary>
  </entry>
  <entry>
    <id>tag:reddit.com,2005:/r/webdev/comments/def456/frontend_contract_role/</id>
    <title>[For Hire] Frontend contractor available</title>
    <updated>2026-04-19T10:30:00+00:00</updated>
    <author><name>bob</name></author>
    <link rel="alternate" href="https://www.reddit.com/r/webdev/comments/def456/frontend_contract_role/" />
    <summary type="html"><![CDATA[Looking for contract work on React and Next.js projects.]]></summary>
  </entry>
</feed>
"""

        items = _parse_atom_feed(feed_xml, subreddit="forhire", feed_url="https://www.reddit.com/r/forhire/new/.rss")

        self.assertEqual(len(items), 2)
        self.assertEqual(items[0]["title"], "[Hiring] Python developer for API integration")
        self.assertEqual(items[0]["author"], "alice")
        self.assertIn("FastAPI integration bug", items[0]["summary"])
        self.assertEqual(items[1]["subreddit"], "forhire")
        self.assertEqual(items[1]["feed_url"], "https://www.reddit.com/r/forhire/new/.rss")

    def test_default_registry_uses_reddit_and_not_x(self) -> None:
        registry = build_default_registry()

        self.assertIsNotNone(registry.reddit)
        self.assertIsNone(registry.x)


if __name__ == "__main__":
    unittest.main()
