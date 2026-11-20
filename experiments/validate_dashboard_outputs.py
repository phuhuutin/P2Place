#!/usr/bin/env python3
"""Validate dashboard-compatible output against the A/B runner's exact CSV schema.

Usage: python3 experiments/validate_dashboard_outputs.py experiments/results/bc_900_aggressive/seed712367/B
Exit nonzero on missing files, incomplete timeline, wrong headers, or impossible counts.
"""
import argparse, csv, json, pathlib, sys

SCHEMAS={
 'runner_operations.csv':['attempt','time_s','phase','workload_class','via_node','region','x','y','color','success','latency_ms','error','error_category'],
 'region_replication_events.csv':['run_id','time_s','event_type','region','old_n','new_n','placement_gen','placement_fp','policy_epoch','authority_node','reason'],
 'node_replication_timeline.csv':['run_id','time_s','node_id','process_running','membership_state','health','placement_gen','placement_fp','policy_epoch','assigned_regions','n2_regions','n3_regions','n4_regions','avg_target_n','desired_region_copies','connected_assigned_regions'],
 'replication_summary.csv':['run_id','time_s','placement_gen','placement_fp','policy_epoch','n2_regions','n3_regions','n4_regions','avg_target_n','total_desired_copies','total_assigned_copies','underassigned_regions','processes_running','members_committed'],
 'node_process_events.csv':['time_s','node_id','boot','action','pid'],
}

def check(folder):
    folder=pathlib.Path(folder)
    failures=[];totals={};data={}
    for name,fields in SCHEMAS.items():
        f=folder/name
        if not f.is_file():failures.append('MISSING '+name);continue
        with f.open(newline='') as handle:
            reader=csv.DictReader(handle)
            if reader.fieldnames!=fields:failures.append('COLUMN MISMATCH '+name+' '+str(reader.fieldnames))
            rows=list(reader)
        totals[name]=len(rows);data[name]=rows
    for name in ('placement_snapshots.jsonl','summary.json','fault_plan.json'):
        if not (folder/name).is_file():failures.append('MISSING '+name)
    if failures:return failures,totals
    summary=json.loads((folder/'summary.json').read_text())
    if not summary.get('replication_timeline',{}).get('complete'):
        failures.append('INCOMPLETE replication_timeline (missing authoritative metrics?)')
    ns=int(summary.get('nodes',10));duration=int(float(summary.get('duration_s',900)))
    if len(data['replication_summary.csv'])!=duration+1:failures.append('unexpected 1Hz cluster sample count')
    if len(data['node_replication_timeline.csv'])!=(duration+1)*ns:failures.append('unexpected 1Hz node sample count')
    node_samples={}
    for row in data['node_replication_timeline.csv']:
        key=row['time_s'];node_samples.setdefault(key,[]).append(row)
    for row in data['replication_summary.csv']:
        if sum(int(row[f'n{n}_regions']) for n in (2,3,4))!=256:
            failures.append('N totals not 256 at '+row['time_s']);break
        assigned=sum(int(v['assigned_regions']) for v in node_samples.get(row['time_s'],[]))
        if assigned!=int(row['total_assigned_copies']):
            failures.append('assigned copy count mismatch at '+row['time_s']);break
    if totals['runner_operations.csv']!=summary.get('dashboard_operations'):
        failures.append('operation total mismatches summary')
    if not data['node_process_events.csv']:
        failures.append('NO process events: fault/boot reconstruction unverified')
    else:
        node_set={int(x['node_id']) for x in data['node_process_events.csv'] if x['action']=='launch' and x['boot']=='1'}
        if len(node_set)!=ns:failures.append('incomplete startup launch ground truth')
        if not any(x['action']=='kill' for x in data['node_process_events.csv']):
            failures.append('no SIGKILL events')
    for n in range(1,ns+1):
        if not (folder/f'node{n}_boot1.log').exists():failures.append('missing boot 1 log node '+str(n))
        if not (folder/'metrics'/f'node{n}_boot1').is_dir():failures.append('missing boot1 metrics node '+str(n))
    return failures,totals

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('result_dir',type=pathlib.Path);a=p.parse_args()
    errors,totals=check(a.result_dir)
    print(json.dumps({'valid':not bool(errors),'rows':totals,'errors':errors},indent=2))
    sys.exit(bool(errors))
