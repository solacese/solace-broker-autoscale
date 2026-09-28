import unittest
from live_state import BoundedSeen, PendingTimes, Pacer

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
    def test_pacer_does_not_burst_after_stall(self):
        clock=FakeClock();p=Pacer(clock)
        self.assertAlmostEqual(p.delay(100),.01)
        clock.value=2
        self.assertAlmostEqual(p.delay(100),.01)

if __name__=='__main__': unittest.main()
