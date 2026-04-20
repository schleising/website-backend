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


def _parse_ip_address(candidate: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    """Return an IP address object when candidate is a literal address."""

    try:
        return ipaddress.ip_address(candidate)
    except ValueError:
        return None


@lru_cache(maxsize=2048)
def _resolve_hostname_ip_addresses(
    hostname: str,
) -> tuple[ipaddress.IPv4Address | ipaddress.IPv6Address, ...]:
    """Resolve hostnames to unique IP addresses for allow-list checks."""

    normalized_hostname = hostname.strip().rstrip(".").lower()
    if normalized_hostname == "":
        return ()

    try:
        address_info = socket.getaddrinfo(normalized_hostname, None, proto=socket.IPPROTO_TCP)
    except (socket.gaierror, OSError):
        return ()

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

    return tuple(resolved_addresses)


def is_public_network_hostname(hostname: str, *, require_dns_resolution: bool = True) -> bool:
    """Return True when hostname is not local/private/reserved network space."""

    normalized_hostname = str(hostname).strip().rstrip(".").lower()
    if normalized_hostname == "":
        return False

    if normalized_hostname in LOCAL_HOSTNAMES:
        return False

    parsed_ip = _parse_ip_address(normalized_hostname)
    if parsed_ip is not None:
        return bool(parsed_ip.is_global)

    if not require_dns_resolution:
        return True

    resolved_addresses = _resolve_hostname_ip_addresses(normalized_hostname)
    if len(resolved_addresses) == 0:
        return False

    return all(address.is_global for address in resolved_addresses)


def is_public_http_url(url: str, *, require_dns_resolution: bool = True) -> bool:
    """Return True for HTTP(S) URLs that target public network hosts."""

    parsed = urlparse(str(url).strip())
    if parsed.scheme.lower() not in {"http", "https"}:
        return False

    hostname = (parsed.hostname or "").strip()
    if hostname == "":
        return False

    try:
        _ = parsed.port
    except ValueError:
        return False

    return is_public_network_hostname(hostname, require_dns_resolution=require_dns_resolution)
