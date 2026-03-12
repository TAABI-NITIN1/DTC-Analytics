import sys; sys.path.insert(0, '.')
from src.config import load_env; load_env('.env')
from src.clickhouse_utils import get_clickhouse_client
from src.clickhouse_utils_v2 import V2_TABLES

c = get_clickhouse_client()
vfm = V2_TABLES['vehicle_fault_master']
fsh = V2_TABLES['fleet_system_health']
fhs = V2_TABLES['fleet_health_summary']

print('=== Fix 5: No more "nan" system in VFM ===')
nan_count = c.execute(f"SELECT count() FROM {vfm} WHERE system='nan' AND is_resolved=0")[0][0]
empty_count = c.execute(f"SELECT count() FROM {vfm} WHERE system='' AND is_resolved=0")[0][0]
total_unresolved = c.execute(f"SELECT count() FROM {vfm} WHERE is_resolved=0")[0][0]
print(f'  VFM unresolved: total={total_unresolved}, nan={nan_count}, empty={empty_count}')

print()
print('=== Fix 6: fleet_system_health now covers ALL active faults ===')
rows = c.execute(f"""
    SELECT system, sum(vehicles_affected), sum(active_faults), sum(critical_faults)
    FROM {fsh}
    GROUP BY system
    ORDER BY sum(active_faults) DESC
    LIMIT 10
""")
total_active_in_fsh = 0
for r in rows:
    print(f'  {r[0] or "(empty)"}: veh={r[1]}, active={r[2]}, crit={r[3]}')
    total_active_in_fsh += r[2]

total_fsh = c.execute(f"SELECT sum(active_faults) FROM {fsh}")[0][0]
print(f'  -> Total active faults in FSH: {total_fsh} (should be ~{total_unresolved})')

print()
print('=== Fix 7: active_fault_trend is no longer always "stable" ===')
rows = c.execute(f"SELECT active_fault_trend, count() FROM {fhs} GROUP BY active_fault_trend ORDER BY count() DESC")
for r in rows:
    print(f'  {r[0]}: {r[1]} clients')
