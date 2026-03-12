import sys; sys.path.insert(0, '.')
from src.config import load_env; load_env('.env')
from src.clickhouse_utils import get_clickhouse_client
from src.clickhouse_utils_v2 import V2_TABLES

c = get_clickhouse_client()
vfm = V2_TABLES['vehicle_fault_master']
vhs = V2_TABLES['vehicle_health_summary']
fhs = V2_TABLES['fleet_health_summary']
fft = V2_TABLES['fleet_fault_trends']
fsh = V2_TABLES['fleet_system_health']
fdd = V2_TABLES['fleet_dtc_distribution']
dfi = V2_TABLES['dtc_fleet_impact']
mp  = V2_TABLES['maintenance_priority']
coo = V2_TABLES['dtc_cooccurrence']
vm  = V2_TABLES['vehicle_master']
dm  = V2_TABLES['dtc_master']

# ── 1. Count consistency ───────────────────────────────────────────────────
print('=== 1. fleet_health_summary vs vehicle_health_summary ===')
r = c.execute(f'SELECT sum(total_vehicles), sum(vehicles_with_active_faults), sum(vehicles_with_critical_faults) FROM {fhs}')[0]
print(f'  FHS: total={r[0]}, active_veh={r[1]}, critical_veh={r[2]}')
r2 = c.execute(f'SELECT count(), countIf(active_fault_count>0), countIf(critical_fault_count>0) FROM {vhs}')[0]
print(f'  VHS: total={r2[0]}, active_veh={r2[1]}, critical_veh={r2[2]}  (should match FHS)')
r3 = c.execute(f'SELECT uniqExact(uniqueid), uniqExactIf(uniqueid, is_resolved=0), uniqExactIf(uniqueid, is_resolved=0 AND severity_level>=3) FROM {vfm}')[0]
print(f'  VFM: total={r3[0]}, active_veh={r3[1]}, critical_veh={r3[2]}  (ground truth)')

# ── 2. Health score distribution ──────────────────────────────────────────
print()
print('=== 2. Health score distribution in VHS ===')
rows = c.execute(
    "SELECT multiIf(vehicle_health_score=0,'0',vehicle_health_score<50,'<50',"
    f"vehicle_health_score<80,'50-80',vehicle_health_score<95,'80-95','95-100') as b,"
    f" count() FROM {vhs} GROUP BY b ORDER BY b"
)
for r in rows:
    print(f'  [{r[0]:>8s}]: {r[1]:4d} vehicles')

print()
print('=== 2b. Health score distribution in FHS (per client) ===')
rows = c.execute(
    "SELECT multiIf(fleet_health_score=0,'zero',fleet_health_score<50,'<50',"
    f"fleet_health_score<80,'50-80',fleet_health_score<95,'80-95','95-100') as b,"
    f" count() FROM {fhs} GROUP BY b ORDER BY b"
)
for r in rows:
    print(f'  [{r[0]:>8s}]: {r[1]:4d} clients')

# ── 3. avg_resolution_time unit check ─────────────────────────────────────
print()
print('=== 3. avg_resolution_time in VHS (seconds - check plausibility) ===')
r = c.execute(f'SELECT round(min(avg_resolution_time),0), round(avg(avg_resolution_time),0), round(max(avg_resolution_time),0) FROM {vhs} WHERE avg_resolution_time > 0')[0]
print(f'  min={r[0]:.0f}s ({r[0]/3600:.1f}h), avg={r[1]:.0f}s ({r[1]/3600:.1f}h), max={r[2]:.0f}s ({r[2]/3600:.1f}h)')
# Check if any values look like hours (i.e. suspiciously small like 0.1–5)
too_small = c.execute(f'SELECT count() FROM {vhs} WHERE avg_resolution_time > 0 AND avg_resolution_time < 60')[0][0]
print(f'  Values < 60s (suspicious if unit were hours): {too_small}')

# ── 4. fleet_fault_trends: active_faults vs global unresolved ─────────────
print()
print('=== 4. fleet_fault_trends: today-7 to today ===')
rows = c.execute(
    f'SELECT date, sum(active_faults), sum(critical_faults), sum(new_faults), sum(resolved_faults)'
    f' FROM {fft} GROUP BY date ORDER BY date DESC LIMIT 7'
)
for r in rows:
    print(f'  {r[0]}: active={r[1]:5d}, crit={r[2]:3d}, new={r[3]:5d}, resolved={r[4]:5d}')

# ── 5. fleet_system_health ────────────────────────────────────────────────
print()
print('=== 5. fleet_system_health - by system ===')
rows = c.execute(
    f'SELECT system, sum(vehicles_affected), sum(active_faults), sum(critical_faults), round(avg(risk_score),1)'
    f' FROM {fsh} GROUP BY system ORDER BY sum(active_faults) DESC LIMIT 8'
)
for r in rows:
    print(f'  {str(r[0]):<20s}: veh={r[1]:4d}, active={r[2]:5d}, crit={r[3]:3d}, risk={r[4]}')

# Check: system_health active_faults sum vs VFM unresolved count
fsh_active = c.execute(f'SELECT sum(active_faults) FROM {fsh}')[0][0]
vfm_unresolved = c.execute(f'SELECT count() FROM {vfm} WHERE is_resolved=0')[0][0]
print(f'  FSH sum(active_faults)={fsh_active}  vs  VFM is_resolved=0 count={vfm_unresolved}')
print(f'  Diff (empty-system episodes not in FSH): {vfm_unresolved - fsh_active}')

# ── 6. dtc_master coverage ────────────────────────────────────────────────
print()
print('=== 6. DTC master coverage (VFM codes missing from dtc_master) ===')
missing = c.execute(
    f'SELECT count(DISTINCT dtc_code), sum(occurrence_count) FROM {vfm}'
    f' WHERE dtc_code NOT IN (SELECT dtc_code FROM {dm})'
)[0]
print(f'  Missing DTC codes: {missing[0]}, total occurrences affected: {missing[1]:,}')
severity_rows = c.execute(f'SELECT severity_level, count() FROM {dm} GROUP BY severity_level ORDER BY severity_level')
for r in severity_rows:
    print(f'  dtc_master sev={r[0]}: {r[1]} codes')

# ── 7. maintenance_priority: does it cover all unresolved vehicles? ────────
print()
print('=== 7. maintenance_priority coverage ===')
r = c.execute(f'SELECT count(DISTINCT uniqueid), count(DISTINCT dtc_code) FROM {mp}')[0]
print(f'  MP: {r[0]} unique vehicles, {r[1]} unique DTC codes')
r2 = c.execute(f'SELECT uniqExact(uniqueid) FROM {vfm} WHERE is_resolved=0')[0][0]
print(f'  VFM unresolved vehicles: {r2}  (MP should be subset)')
r3 = c.execute(f'SELECT severity_level, count() FROM {mp} GROUP BY severity_level ORDER BY severity_level')[:]
for row in r3:
    print(f'  MP sev={row[0]}: {row[1]} rows')

# ── 8. dtc_cooccurrence sanity ────────────────────────────────────────────
print()
print('=== 8. dtc_cooccurrence ===')
r = c.execute(f'SELECT count(), sum(vehicles_affected), max(vehicles_affected), max(cooccurrence_count) FROM {coo}')[0]
print(f'  Pairs: {r[0]:,}, total veh-affected: {r[1]:,}, max_veh: {r[2]}, max_cooc: {r[3]}')
print('  Top 5 co-occurring pairs:')
rows = c.execute(f'SELECT dtc_code_a, dtc_code_b, vehicles_affected FROM {coo} ORDER BY vehicles_affected DESC LIMIT 5')
for r in rows:
    print(f'  {r[0]} + {r[1]}: {r[2]} vehicles')

# ── 9. fleet_dtc_distribution vs dtc_fleet_impact ─────────────────────────
print()
print('=== 9. fleet_dtc_distribution vs dtc_fleet_impact (row counts match?) ===')
r1 = c.execute(f'SELECT count() FROM {fdd}')[0][0]
r2 = c.execute(f'SELECT count() FROM {dfi}')[0][0]
print(f'  fleet_dtc_distribution: {r1}  dtc_fleet_impact: {r2}  (should both be client x dtc combos)')
# Top DTC by occurrence
rows = c.execute(f'SELECT dtc_code, any(description), sum(total_occurrences), sum(vehicles_affected) FROM {fdd} GROUP BY dtc_code ORDER BY sum(total_occurrences) DESC LIMIT 5')
print('  Top 5 DTCs in fleet_dtc_distribution:')
for r in rows:
    print(f'  {r[0]:>6s} ({str(r[1])[:30]:<30s}): occ={r[2]:,}, veh={r[3]}')

# ── 10. GraphQL fleet_kpis resolver cross-check ───────────────────────────
print()
print('=== 10. fleet_kpis cross-check (last 30 days) ===')
r = c.execute(f'SELECT count(), countIf(active_fault_count>0), countIf(critical_fault_count>0), round(avg(vehicle_health_score),2) FROM {vhs}')[0]
print(f'  total_vehicles={r[0]}, vehicles_with_dtcs={r[1]}, critical={r[2]}, fleet_health={r[3]}')
r2 = c.execute(f'SELECT round(avg(resolution_time_sec/86400.0),3) FROM {vfm} WHERE is_resolved=1 AND event_date>=today()-30 AND resolution_time_sec>0')[0][0]
print(f'  avg_resolution_days (last 30d, resolved)={r2}')
r3 = c.execute(f'SELECT count(DISTINCT uniqueid) FROM {vfm} WHERE is_resolved=0 AND severity_level>=3')[0][0]
print(f'  maintenance_due (unresolved sev>=3 vehicles)={r3}')
r4 = c.execute(f'SELECT sum(occurrence_count) FROM {vfm} WHERE event_date>=today()-30')[0][0]
print(f'  total_dtc_alerts (all occurrences, last 30d): {r4:,}')

print()
print('=== ALL CHECKS DONE ===')
