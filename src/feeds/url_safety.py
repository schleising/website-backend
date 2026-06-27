from __future__ import annotations

from functools import lru_cache
import ipaddress
import socket
from urllib.parse import urlparse

LOCAL_HOSTNAMES = {
    "localhost",
    "localhost.localdomain",
    "ip6-localhost",
}


class _DnsResolutionUnavailable(LookupError):
    """DNS lookup failed or returned no usable addresses.

    Raised from the LRU-backed resolver so transient failures are not cached.
    """


def _parse_ip_address(candidate: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    """Return an IP address object when candidate is a literal address."""

    try:
        return ipaddress.ip_address(candidate)
    except ValueError:
        return None


@lru_cache(maxsize=2048)
def _cached_successful_hostname_resolution(
    normalized_hostname: str,
) -> tuple[ipaddress.IPv4Address | ipaddress.IPv6Address, ...]:
    """Resolve hostnames and cache only successful lookups."""

    try:
        address_info = socket.getaddrinfo(normalized_hostname, None, proto=socket.IPPROTO_TCP)
    except (socket.gaierror, OSError) as exc:
        raise _DnsResolutionUnavailable(normalized_hostname) from exc

    resolved_addresses: list[ipaddress.IPv4Address | ipaddress.IPv6Address] = []
    seen_addresses: set[str] = set()

    for _family, _socktype, _proto, _canonname, sockaddr in address_info:
        if not isinstance(sockaddr, tuple) or len(sockaddr) == 0:
            continue

        host_value = str(sockaddr[0]).strip()
        parsed_ip = _parse_ip_address(host_value)
        if parsed_ip is None:
            continue

        canonical_ip = str(parsed_ip)
        if canonical_ip in seen_addresses:
            continue

        seen_addresses.add(canonical_ip)
        resolved_addresses.append(parsed_ip)

    if len(resolved_addresses) == 0:
        raise _DnsResolutionUnavailable(normalized_hostname)

    return tuple(resolved_addresses)


def _resolve_hostname_ip_addresses(hostname: str) -> tuple[ipaddress.IPv4Address | ipaddress.IPv6Address, ...]:
    """Resolve hostnames to unique IP addresses for allow-list checks."""

    normalized_hostname = hostname.strip().rstrip(".").lower()
    if normalized_hostname == "":
        return ()

    try:
        return _cached_successful_hostname_resolution(normalized_hostname)
    except _DnsResolutionUnavailable:
        return ()


def is_public_network_hostname(hostname: str, *, require_dns_resolution: bool = True) -> bool:
    """Return True when hostname is not local/private/reserved network space."""

    return explain_public_network_hostname_block(hostname, require_dns_resolution=require_dns_resolution) is None


def explain_public_network_hostname_block(
    hostname: str,
    *,
    require_dns_resolution: bool = True,
) -> str | None:
    """Return a rejection reason, or None when the hostname is allowed."""

    normalized_hostname = str(hostname).strip().rstrip(".").lower()
    if normalized_hostname == "":
        return "Blocked URL with missing hostname."

    if normalized_hostname in LOCAL_HOSTNAMES:
        return f"Blocked local hostname: {normalized_hostname}"

    parsed_ip = _parse_ip_address(normalized_hostname)
    if parsed_ip is not None:
        if parsed_ip.is_global:
            return None
        return f"Blocked non-public IP literal: {normalized_hostname}"

    if not require_dns_resolution:
        return None

    resolved_addresses = _resolve_hostname_ip_addresses(normalized_hostname)
    if len(resolved_addresses) == 0:
        return f"Blocked URL after DNS resolution failure: {normalized_hostname}"

    for address in resolved_addresses:
        if not address.is_global:
            return f"Blocked non-public resolved address for {normalized_hostname}: {address}"

    return None


def is_public_http_url(url: str, *, require_dns_resolution: bool = True) -> bool:
    """Return True for HTTP(S) URLs that target public network hosts."""

    return explain_public_http_url_block(url, require_dns_resolution=require_dns_resolution) is None


def explain_public_http_url_block(url: str, *, require_dns_resolution: bool = True) -> str | None:
    """Return a rejection reason, or None when the URL is allowed."""

    parsed = urlparse(str(url).strip())
    if parsed.scheme.lower() not in {"http", "https"}:
        return f"Blocked unsupported URL scheme: {parsed.scheme or '(missing)'}"

    hostname = (parsed.hostname or "").strip()
    if hostname == "":
        return "Blocked URL with missing hostname."

    try:
        _ = parsed.port
    except ValueError:
        return f"Blocked URL with invalid port: {url}"

    hostname_reason = explain_public_network_hostname_block(
        hostname,
        require_dns_resolution=require_dns_resolution,
    )
    if hostname_reason is not None:
        return hostname_reason

    return None
