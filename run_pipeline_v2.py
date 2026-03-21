"""Run the v2 analytics pipeline (bootstrap / full rebuild)."""
import logging, sys, os

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# project root
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from src.config import load_env
load_env('.env')

# VPN auto-connect (local dev only — disabled on VM via USE_VPN=false)
from src.vpn import ensure_vpn
ensure_vpn(ch_host=os.getenv('CH_DB_Host', ''), ch_port=int(os.getenv('CH_DB_Port', 8123)))

from src.analytics_pipeline_v2_sql import run_full_analytics_v2

if __name__ == '__main__':
    result = run_full_analytics_v2()
    print('\n=== V2 Pipeline Result ===')
    for k, v in result.items():
        print(f'  {k}: {v}')
