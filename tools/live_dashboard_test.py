import ast,unittest
from pathlib import Path

class DashboardSDKPerfControlTest(unittest.TestCase):
 def test_control_is_authenticated_and_bounded(self):
  source=Path(__file__).with_name("live_dashboard.py").read_text();ast.parse(source)
  self.assertIn('if self.path=="/admin/sdkperf"',source)
  self.assertIn('if not cookie_ok(self):self.send(401',source)
  self.assertIn('if not 1<=messages<=SDKPERF_MAX_MESSAGES or not 1<=rate<=SDKPERF_MAX_RATE',source)
  self.assertNotIn('shell=True',source)
  self.assertNotIn('SWLB_SDKPERF_USERNAME=&lt;user&gt;',source)
  self.assertIn('Direct broker smoke test — bypasses the shims',source)
  self.assertIn('"$SDKPERF_CMD" "-cip=$SMF_URL"',source)
  self.assertIn("copyText('directConsumer')",source)
 def test_persisted_sdkperf_evidence_is_restored(self):
  source=Path(__file__).with_name("live_dashboard.py").read_text()
  self.assertIn("def latest_sdkperf_status()",source)
  self.assertIn('"consumer_received":int(data["sdkperf_consumer_unique"])',source)
 def test_shutdown_has_bounded_publisher_drain(self):
  source=Path(__file__).with_name("live_dashboard.py").read_text()
  self.assertIn("deadline=time.monotonic()+15",source)
  self.assertIn("if pending==0:break",source)
 def test_harness_uses_existing_supervised_shims(self):
  source=Path(__file__).with_name("live_dashboard.py").read_text()
  self.assertIn('processes[group_key+"-publisher"].write(event["publish"])',source)
  self.assertNotIn('SWLB_SDKPERF_PUBLISHER',source)

if __name__=="__main__":unittest.main()
