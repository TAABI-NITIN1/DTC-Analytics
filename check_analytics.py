import sys; sys.path.insert(0, '.')
from src.config import load_env; load_env('.env')
from src.clickhouse_utils import get_clickhouse_client
from src.clickhouse_utils_v2 import V2_TABLES

c = get_clickhouse_client()
vfm = V2_TABLES['vehicle_fault_master']
vhs = V2_TABLES['vehicle_health_summary']
fft = V2_TABLES['fleet_fault_trends']
fhs = V2_TABLES['fleet_health_summary']

print("=== 1. VFM resolved vs unresolved ===")
for row in c.execute(f'SELECT is_resolved, count() FROM {vfm} GROUP BY is_resolved ORDER BY is_resolved'):
    print(row)

print("\n=== 2. fleet_fault_trends last 30 days (summed) ===")
rows = c.execute(f'SELECT date, sum(active_faults), sum(new_faults), sum(resolved_faults), round(avg(fleet_health_score),1) FROM {fft} WHERE date >= today()-30 GROUP BY date ORDER BY date')
for row in rows: print(row)

print("\n=== 3. avg_resolution_time in VHS (min/max/avg, in seconds) ===")
for row in c.execute(f'SELECT round(min(avg_resolution_time),0), round(max(avg_resolution_time),0), round(avg(avg_resolution_time),0) FROM {vhs} WHERE avg_resolution_time > 0'):
    print(f'min={row[0]}s ({row[0]/3600:.1f}h), max={row[1]}s ({row[1]/3600:.1f}h), avg={row[2]}s ({row[2]/3600:.1f}h)')

print("\n=== 4. fleet_health_summary totals ===")
for row in c.execute(f'SELECT sum(total_vehicles), sum(vehicles_with_active_faults), sum(vehicles_with_critical_faults), round(avg(fleet_health_score),2) FROM {fhs}'):
    print(f'total_vehicles={row[0]}, active={row[1]}, critical={row[2]}, avg_health_score={row[3]}')

print("\n=== 5. fleet_health_score distribution in VHS ===")
for row in c.execute(f"SELECT multiIf(vehicle_health_score=0,'0',vehicle_health_score<50,'<50',vehicle_health_score<80,'50-80',vehicle_health_score<95,'80-95','95-100') as bucket, count() FROM {vhs} GROUP BY bucket ORDER BY bucket"):
    print(row)

print("\n=== 6. Top 5 DTC codes (fleet-wide) ===")
for row in c.execute(f'SELECT dtc_code, any(description), sum(occurrence_count) as occ, uniqExact(uniqueid) as vehicles FROM {vfm} GROUP BY dtc_code ORDER BY occ DESC LIMIT 5'):
    print(row)

print("\n=== 7. fleet_kpis check - VHS totals ===")
for row in c.execute(f'SELECT count(), countIf(active_fault_count>0), countIf(critical_fault_count>0), round(avg(vehicle_health_score),2) FROM {vhs}'):
    print(f'total={row[0]}, with_active={row[1]}, with_critical={row[2]}, avg_health={row[3]}')

print("\n=== 8. maintenance_priority - severity distribution ===")
mp_t = V2_TABLES['maintenance_priority']
for row in c.execute(f'SELECT severity_level, count() FROM {mp_t} GROUP BY severity_level ORDER BY severity_level'):
    print(row)

print("\n=== 9. avg_resolution_time - fleet_kpis resolver check (resolved episodes, last 30 days) ===")
for row in c.execute(f'SELECT round(avg(resolution_time_sec/86400.0),2) as avg_days FROM {vfm} WHERE is_resolved=1 AND event_date >= today()-30 AND resolution_time_sec > 0'):
    print(f'avg_resolution_days (resolved, last 30d) = {row[0]}')

print("\n=== 10. vehicle_fault_master event_date range ===")
for row in c.execute(f'SELECT min(event_date), max(event_date), count() FROM {vfm}'):
    print(row)
