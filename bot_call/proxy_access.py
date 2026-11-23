"""Socket peer policy and bounded, credential-free access rejection diagnostics."""

import ipaddress
import logging
import re
import time
from urllib.parse import urlsplit


def listen_address(config):
    return "127.0.0.1" if config["access_mode"] == "quick_tunnel" else config["listen_host"]


def address(value):
    if not isinstance(value, str) or "%" in value:
        raise ValueError
    return ipaddress.ip_address(value)


def proxy_networks(values):
    if type(values) is not list or not 1 <= len(values) <= 64:
        raise ValueError
    networks = []
    for value in values:
        if type(value) is not str or "%" in value or len(value) > 80:
            raise ValueError
        network = ipaddress.ip_network(value, strict=False)
        # Also forbid the mapped-IPv4 equivalent of trusting every IPv4 peer.
        if network.prefixlen == 0 or (
            network.version == 6 and ipaddress.ip_network("::ffff:0:0/96").subnet_of(network)
        ):
            raise ValueError
        networks.append(network)
    return tuple(networks)


def peer_address(scope):
    peer = address(scope["client"][0])
    return (
        peer.ipv4_mapped if isinstance(peer, ipaddress.IPv6Address) and peer.ipv4_mapped else peer
    )


def trusted_peer(scope, networks):
    try:
        peer = peer_address(scope)
        return any(peer.version == n.version and peer in n for n in networks)
    except (ValueError, KeyError, TypeError, IndexError):
        return False


def forwarded_proto(scope):
    return [v for k, v in scope.get("headers", []) if k.lower() == b"x-forwarded-proto"]


def safe_host(value):
    # Host is attacker-controlled: never echo userinfo, paths, queries or controls.
    if len(value) <= 255 and re.fullmatch(r"[A-Za-z0-9.:[\]-]+", value):
        return value
    return "[invalid]"


class Rejections:
    def __init__(self):
        self.latest = None
        self._last_log = float("-inf")

    def record(self, scope, reason, origin=""):
        headers = scope.get("headers", [])
        hosts = [v.decode("ascii", "replace") for k, v in headers if k.lower() == b"host"]
        origins = [v.decode("ascii", "replace") for k, v in headers if k.lower() == b"origin"]
        try:
            peer = str(peer_address(scope))
        except (ValueError, KeyError, TypeError, IndexError):
            peer = "[unknown]"
        proto = forwarded_proto(scope)
        self.latest = dict(
            at=int(time.time()),
            reason=reason,
            peer=peer,
            forwarded_proto_count=len(proto),
            forwarded_proto_https=proto == [b"https"],
            host=safe_host(hosts[0]) if len(hosts) == 1 else "[missing/duplicate]",
            expected_host=safe_host(urlsplit(origin).netloc) if origin else "",
            origin_present=bool(origins),
            origin_matches=(origins == [origin]) if origin else None,
        )
        now = time.monotonic()
        if now - self._last_log >= 10:
            self._last_log = now
            logging.getLogger("bot_call.access").warning("bot_call.access_rejected %s", self.latest)
