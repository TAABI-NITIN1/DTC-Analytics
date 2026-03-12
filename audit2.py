import sys; sys.path.insert(0, '.')
from src.config import load_env; load_env('.env')
from src.clickhouse_utils import get_clickhouse_client
from src.clickhouse_utils_v2 import V2_TABLES

c = get_clickhouse_client()
vfm = V2_TABLES['vehicle_fault_master']
dm  = V2_TABLES['dtc_master']
fsh = V2_TABLES['fleet_system_health']

empty_sys = c.execute(f"SELECT count() FROM {vfm} WHERE system='' AND is_resolved=0")[0][0]
nan_sys   = c.execute(f"SELECT count() FROM {vfm} WHERE system='nan' AND is_resolved=0")[0][0]
print(f'VFM unresolved: empty system={empty_sys}, nan system={nan_sys}, total missing={empty_sys+nan_sys}')

dm_nan   = c.execute(f"SELECT count() FROM {dm} WHERE system='nan'")[0][0]
dm_empty = c.execute(f"SELECT count() FROM {dm} WHERE system=''")[0][0]
print(f'dtc_master: nan system={dm_nan}, empty system={dm_empty}')

print('Top 5 DTC codes with empty/nan system in VFM (unresolved):')
rows = c.execute(f"SELECT dtc_code, count() FROM {vfm} WHERE is_resolved=0 AND (system='' OR system='nan') GROUP BY dtc_code ORDER BY count() DESC LIMIT 5")
for r in rows: print(f'  {r[0]}: {r[1]}')

# active_fault_trend - check if 'stable' is always returned
fhs = V2_TABLES['fleet_health_summary']
print()
print('fleet_health_summary active_fault_trend values:')
rows = c.execute(f"SELECT active_fault_trend, count() FROM {fhs} GROUP BY active_fault_trend")
for r in rows: print(f'  {repr(r[0])}: {r[1]} clients')

# Check fleet_dtc_distribution usage - does any resolver query it?
print()
fdd = V2_TABLES['fleet_dtc_distribution']
r = c.execute(f"SELECT count(), uniqExact(clientLoginId), uniqExact(dtc_code) FROM {fdd}")[0]
print(f'fleet_dtc_distribution: {r[0]} rows, {r[1]} clients, {r[2]} DTC codes')
print('  (No GraphQL resolver uses this table directly - it is unused by frontend)')

# VFM: max resolution_time_sec sanity check
print()
print('VFM resolution_time_sec distribtion (resolved episodes):')
rows = c.execute(f"SELECT multiIf(resolution_time_sec<3600,'<1h',resolution_time_sec<86400,'<1d',resolution_time_sec<604800,'<1w','>=1w') as b, count() FROM {vfm} WHERE is_resolved=1 AND resolution_time_sec>0 GROUP BY b ORDER BY b")
for r in rows: print(f'  {r[0]}: {r[1]:,}')
