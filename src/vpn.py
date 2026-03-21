"""
Auto-connect OpenVPN when running locally.
Controlled by env vars:
  USE_VPN=true       -> enable VPN auto-connect (default: false)
  VPN_CONFIG=path    -> path to .ovpn file (default: vpn.ovpn in project root)
"""
import logging
import os
import socket
import subprocess
import time

log = logging.getLogger(__name__)

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _host_reachable(host: str, port: int = 8123, timeout: float = 3.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, TimeoutError):
        return False


def ensure_vpn(ch_host: str, ch_port: int = 8123):
    """
    If USE_VPN is truthy and the ClickHouse host is unreachable,
    start OpenVPN and wait until the host becomes reachable.
    Does nothing when USE_VPN is false/unset (e.g. on the VM).
    """
    use_vpn = os.getenv('USE_VPN', 'false').lower() in ('1', 'true', 'yes')
    if not use_vpn:
        return

    if not ch_host:
        log.warning('VPN check skipped: no CH_DB_Host configured.')
        return

    if _host_reachable(ch_host, ch_port):
        log.info('VPN check: %s:%s already reachable.', ch_host, ch_port)
        return

    ovpn_path = os.getenv('VPN_CONFIG', os.path.join(_PROJECT_ROOT, 'vpn.ovpn'))
    if not os.path.isfile(ovpn_path):
        raise FileNotFoundError(
            f'VPN config not found at {ovpn_path}. '
            f'Set VPN_CONFIG env var or place vpn.ovpn in the project root.'
        )

    log.info('Starting OpenVPN with %s ...', ovpn_path)
    subprocess.Popen(
        ['openvpn', '--config', ovpn_path],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    max_wait = 30
    for i in range(max_wait):
        time.sleep(1)
        if _host_reachable(ch_host, ch_port):
            log.info('VPN connected — %s:%s reachable after %ds.', ch_host, ch_port, i + 1)
            return

    raise TimeoutError(
        f'VPN started but {ch_host}:{ch_port} still unreachable after {max_wait}s. '
        f'Check VPN logs or connect manually.'
    )
