import csv, collections
base = 'experiments/results/B_seed42_smoke'
with open(base + '/policy_node1.csv') as f:
    rows = list(csv.DictReader(f))
print('policy rows:', len(rows))
# churn window t in (45, 75): desired_n distribution
dist = collections.Counter(r['desired_n'] for r in rows if 45 < float(r['time_s']) < 75)
print('desired_n distribution during churn window (45-75s):', dict(dist))
for r in rows:
    if r['desired_n'] == '4' and float(r['time_s']) < 80:
        print('first N4 row:', {k: r[k] for k in
              ('time_s','placement_gen','policy_epoch','region','risk_score',
               'desired_n','assigned_n','confirmed_live_copies')})
        break
with open(base + '/peer_health_node1.csv') as f:
    hrows = list(csv.DictReader(f))
print('health rows for peer 4:')
for r in hrows:
    if r['node_id'] == '4':
        print(' ', {k: r[k] for k in ('time_s','state','observed_seconds','availability_score')})
