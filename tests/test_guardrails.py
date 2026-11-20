#!/usr/bin/env python3
"""Offline tests proving the independent harness refuses misleading evidence."""
import csv, tempfile, pathlib, unittest
from test_strategy_c_independent import placement, pref, pixel, ready, color

class HarnessGuardrails(unittest.TestCase):
    def write_csv(self,path,header,rows):
        path.parent.mkdir(parents=True,exist_ok=True)
        with path.open('w',newline='') as f:
            w=csv.DictWriter(f,fieldnames=header);w.writeheader();w.writerows(rows)
    def case(self,old=False,kind='complete'):
        root=pathlib.Path(tempfile.mkdtemp());events=root/'metrics1'/'region_replication_events.csv'
        header=['event_type','region','new_n','placement_gen','policy_epoch']
        self.write_csv(events,header,[{'event_type':'initial','region':192,'new_n':2,'placement_gen':6,'policy_epoch':1},
          {'event_type':'n_change','region':192,'new_n':3,'placement_gen':6,'policy_epoch':2},
          {'event_type':'n_change','region':192,'new_n':4,'placement_gen':6,'policy_epoch':3}])
        header=['region','placement_gen','policy_epoch','event','classification']
        for node,epoch in [(1,2),(2,3)]:
            self.write_csv(root/f'metrics{node}'/'replica_repair_events.csv',header,[{'region':192,'placement_gen':6,'policy_epoch':1 if old else epoch,'event':'backfill_completed','classification':kind}])
        return root
    def test_correct_activation(self):
        r=self.case();good,_=ready(r,192,(5,6),(1,2));self.assertTrue(good)
    def test_stale_epoch_rejected(self):
        r=self.case(old=True);good,_=ready(r,192,(5,6),(1,2));self.assertFalse(good)
    def test_partial_rejected(self):
        r=self.case(kind='complete_partial');good,_=ready(r,192,(5,6),(1,2));self.assertFalse(good)
    def test_missing_replica_rejected(self):
        r=self.case();(r/'metrics2'/'replica_repair_events.csv').unlink();good,_=ready(r,192,(5,6),(1,2));self.assertFalse(good)
    def test_bad_coordinates(self):
        self.assertEqual(pixel(192,0),(0,192));self.assertEqual(pixel(192,255),(15,207))
    def test_unique_write_colors(self):
        self.assertEqual(len({color(1234,i) for i in range(180)}),180)

if __name__=='__main__':unittest.main(verbosity=2)
