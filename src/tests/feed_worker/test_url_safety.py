from __future__ import annotations

import importlib.util
import ipaddress
from pathlib import Path
import socket
import sys
import unittest
from unittest.mock import patch

BACKEND_SRC = Path(__file__).resolve().parents[2]
URL_SAFETY_PATH = BACKEND_SRC / "feeds" / "url_safety.py"

url_safety_spec = importlib.util.spec_from_file_location(
    "url_safety_for_tests",
    URL_SAFETY_PATH,
)
if url_safety_spec is None or url_safety_spec.loader is None:
    raise RuntimeError("Failed to load url_safety module for tests.")

url_safety = importlib.util.module_from_spec(url_safety_spec)
sys.modules[url_safety_spec.name] = url_safety
url_safety_spec.loader.exec_module(url_safety)


class UrlSafetyTests(unittest.TestCase):
    def setUp(self) -> None:
        url_safety._cached_successful_hostname_resolution.cache_clear()

    def tearDown(self) -> None:
        url_safety._cached_successful_hostname_resolution.cache_clear()

    def test_is_public_http_url_accepts_global_literal_ip(self) -> None:
        self.assertTrue(url_safety.is_public_http_url("https://8.8.8.8/feed.xml"))

    def test_is_public_http_url_rejects_localhost(self) -> None:
        self.assertFalse(url_safety.is_public_http_url("http://localhost/feed.xml"))
        self.assertEqual(
            url_safety.explain_public_http_url_block("http://localhost/feed.xml"),
            "Blocked local hostname: localhost",
        )

    def test_is_public_http_url_rejects_private_literal_ip(self) -> None:
        self.assertFalse(url_safety.is_public_http_url("https://10.0.0.5/feed.xml"))
        self.assertEqual(
            url_safety.explain_public_http_url_block("https://10.0.0.5/feed.xml"),
            "Blocked non-public IP literal: 10.0.0.5",
        )

    def test_is_public_http_url_uses_dns_resolution_for_hostnames(self) -> None:
        with patch.object(
            url_safety,
            "_cached_successful_hostname_resolution",
            return_value=(ipaddress.ip_address("8.8.8.8"),),
        ):
            self.assertTrue(url_safety.is_public_http_url("https://public.example/feed.xml"))

        with patch.object(
            url_safety,
            "_cached_successful_hostname_resolution",
            return_value=(ipaddress.ip_address("10.0.0.7"),),
        ):
            self.assertFalse(url_safety.is_public_http_url("https://internal.example/feed.xml"))
            self.assertEqual(
                url_safety.explain_public_http_url_block("https://internal.example/feed.xml"),
                "Blocked non-public resolved address for internal.example: 10.0.0.7",
            )

    def test_dns_failures_are_not_cached(self) -> None:
        hostname = "flaky.example"
        call_count = 0

        def fake_getaddrinfo(name: str, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise socket.gaierror("simulated transient DNS failure")
            return [
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 0)),
            ]

        url = f"https://{hostname}/feed.xml"
        failure_reason = f"Blocked URL after DNS resolution failure: {hostname}"

        with patch.object(url_safety.socket, "getaddrinfo", side_effect=fake_getaddrinfo):
            self.assertEqual(url_safety.explain_public_http_url_block(url), failure_reason)
            self.assertEqual(url_safety.explain_public_http_url_block(url), failure_reason)
            self.assertEqual(call_count, 2)

            self.assertIsNone(url_safety.explain_public_http_url_block(url))
            self.assertTrue(url_safety.is_public_http_url(url))
            self.assertIsNone(url_safety.explain_public_http_url_block(url))
            self.assertEqual(call_count, 3)

    def test_successful_dns_lookups_are_cached(self) -> None:
        call_count = 0

        def fake_getaddrinfo(name: str, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            return [
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 0)),
            ]

        url = "https://cached.example/feed.xml"

        with patch.object(url_safety.socket, "getaddrinfo", side_effect=fake_getaddrinfo):
            self.assertTrue(url_safety.is_public_http_url(url))
            self.assertTrue(url_safety.is_public_http_url(url))
            self.assertEqual(call_count, 1)


if __name__ == "__main__":
    unittest.main()
