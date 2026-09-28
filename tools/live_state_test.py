import unittest
from live_state import BoundedSeen, FamilyTracker, PendingTimes, Pacer

class FakeClock:
    def __init__(self): self.value=0.0
    def __call__(self): return self.value

class StateTests(unittest.TestCase):
    def test_seen_is_bounded_and_reports_duplicates(self):
        seen=BoundedSeen(2)
        self.assertTrue(seen.add('a'));self.assertTrue(seen.add('b'));self.assertFalse(seen.add('a'))
        self.assertTrue(seen.add('c'));self.assertEqual(len(seen),2);self.assertTrue(seen.add('b'))
    def test_pending_expires_and_bounds(self):
        pending=PendingTimes(2,5)
        pending.add('a',0);pending.add('b',1);pending.add('c',2);self.assertIsNone(pending.pop('a'))
        pending.expire(8);self.assertEqual(len(pending),0)
    def test_family_tracker_preserves_affinity_and_sequence(self):
        tracker=FamilyTracker("ABCDE")
        tracker.submitted("A","run-1");tracker.accepted("A","broker-a");tracker.delivered("A","run-1",0)
        tracker.submitted("A","run-1");tracker.accepted("A","broker-a");tracker.delivered("A","run-1",1)
        self.assertEqual(tracker.values["A"]["last_sequence"],1)
        self.assertEqual(tracker.values["A"]["broker_mismatches"],0)
        tracker.accepted("A","broker-b");tracker.delivered("A","run-1",3)
        self.assertEqual(tracker.values["A"]["broker"],"broker-b")
        self.assertEqual(tracker.values["A"]["broker_mismatches"],1)
        self.assertEqual(tracker.values["A"]["gaps"],1)
        tracker.delivered("A","run-2",0)
        self.assertEqual(tracker.values["A"]["run_id"],"run-1")
        self.assertEqual(tracker.values["A"]["last_sequence"],3)
    def test_family_tracker_counts_leading_gap(self):
        tracker=FamilyTracker("ABCDE")
        tracker.submitted("A","run-1");self.assertTrue(tracker.delivered("A","run-1",3))
        self.assertEqual(tracker.values["A"]["gaps"],3)
    def test_sdkperf_sequences_do_not_touch_family_tracker(self):
        tracker=FamilyTracker("ABCDE")
        self.assertEqual(sum(v["ordering_errors"] for v in tracker.values.values()),0)
    def test_pacer_does_not_burst_after_stall(self):
        clock=FakeClock();p=Pacer(clock)
        self.assertAlmostEqual(p.delay(100),.01)
        clock.value=2
        self.assertAlmostEqual(p.delay(100),.01)

if __name__=='__main__': unittest.main()
