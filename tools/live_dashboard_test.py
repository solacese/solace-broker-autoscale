import ast,unittest
from pathlib import Path

class DashboardSDKPerfControlTest(unittest.TestCase):
 def test_control_is_authenticated_and_bounded(self):
  source=Path(__file__).with_name("live_dashboard.py").read_text();ast.parse(source)
  self.assertIn('if self.path=="/admin/sdkperf"',source)
  self.assertIn('if not cookie_ok(self):self.send(401',source)
  self.assertIn('if not request_ok(self):self.send(403',source)
  self.assertIn('if data.get("action")=="start":sdkperf_start(managed_spec(data))',source)
  self.assertIn('if sdkperf_execution_lock.locked():self.send(409',source)
  control=Path(__file__).with_name("sdkperf_control.py").read_text()
  self.assertIn("MAX_MESSAGES=1000",control);self.assertIn("MAX_RATE=100",control)
  self.assertNotIn('shell=True',source)
  self.assertNotIn('SWLB_SDKPERF_USERNAME=&lt;user&gt;',source)
  html=Path(__file__).with_name("dashboard.html").read_text()
  self.assertIn('<h1>Solace Workload Balancer</h1>',html)
  self.assertNotIn('Ordered families. Balanced brokers.',html)
  self.assertNotIn('message-stream',html)
  self.assertIn('Direct broker smoke test — bypasses the shims',html)
  self.assertIn('"$SDKPERF_CMD" "-cip=$SMF_URL"',html)
  self.assertIn('data-copy="directConsumer"',html)
 def test_persisted_sdkperf_evidence_is_restored(self):
  source=Path(__file__).with_name("live_dashboard.py").read_text()
  self.assertIn("def latest_sdkperf_status()",source)
  self.assertIn('"consumer_received":int(data["sdkperf_consumer_unique"])',source)
  self.assertIn('"producer_transmitted":int(data.get("sdkperf_producer_transmitted",0))',source)
  self.assertIn('data.get("duration",data.get("duration_limit"))',source)
 def test_shutdown_has_bounded_publisher_drain(self):
  source=Path(__file__).with_name("live_dashboard.py").read_text()
  self.assertIn("deadline=time.monotonic()+15",source)
  self.assertIn("if pending==0:break",source)
 def test_family_demo_uses_real_receipt_and_explicit_sequence(self):
  source=Path(__file__).with_name("live_dashboard.py").read_text()
  self.assertIn('FAMILY_EXPECTED={"A":"broker-b","B":"broker-c","C":"broker-b","D":"broker-a","E":"broker-a"}',source)
  self.assertIn('family_tracker.accepted(family,broker)',source)
  self.assertIn('headers.get("family_sequence"',source)
  self.assertNotIn('idx%64',source)
 def test_harness_uses_existing_supervised_shims(self):
  source=Path(__file__).with_name("live_dashboard.py").read_text()
  self.assertIn('processes[group_key+"-publisher"].write(event["publish"])',source)
  self.assertNotIn('SWLB_SDKPERF_PUBLISHER',source)

if __name__=="__main__":unittest.main()
