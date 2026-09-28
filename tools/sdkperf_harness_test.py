import io,json,os,signal,subprocess,tempfile,time,unittest
from pathlib import Path
from unittest import mock
import sdkperf_harness as h

class FakeProcess:
 def __init__(self,returncode=None):self.returncode=returncode;self.pid=os.getpid();self.signals=[]
 def poll(self):return self.returncode
 def wait(self,timeout=None):
  if self.returncode is None:raise subprocess.TimeoutExpired("fake",timeout)
  return self.returncode

class HarnessTest(unittest.TestCase):
 def test_command_text_redacts_password(self):
  text=h.command_text(["sdkperf","-cu=user@vpn","-cp=secret","-mn=10"])
  self.assertNotIn("secret",text);self.assertIn("<password>",text)
 def test_bounds_reject_oversized_run_before_environment(self):
  with mock.patch("sys.argv",["sdkperf_harness.py","--messages","1001"]),self.assertRaises(RuntimeError):h.main()
 def test_dash_prefixed_producer_args_parse_and_validate(self):
  self.assertTrue(h.allowed_producer_arg("-l"));self.assertTrue(h.allowed_producer_arg("-lb=64"));self.assertFalse(h.allowed_producer_arg("-cip=evil"))
  with mock.patch("sys.argv",["sdkperf_harness.py","--producer-arg=-cip=evil"]),self.assertRaises(RuntimeError):h.main()
 def test_missing_consumer_fails_readiness(self):
  with tempfile.TemporaryDirectory() as d:
   p=FakeProcess(returncode=1)
   with self.assertRaisesRegex(RuntimeError,"consumer exited"):h.wait_consumer_ready(Path(d)/"missing.log",p,.1)
 def test_stop_interrupts_consumer_readiness(self):
  with tempfile.TemporaryDirectory() as d:
   h.stopping.set()
   try:
    with self.assertRaisesRegex(RuntimeError,"stopped"):h.wait_consumer_ready(Path(d)/"missing.log",FakeProcess(),1)
   finally:h.stopping.clear()
 def test_stop_interrupts_process_wait(self):
  h.stopping.set()
  try:
   with self.assertRaisesRegex(RuntimeError,"stopped"):h.wait_process(FakeProcess(),1)
  finally:h.stopping.clear()
 def test_impossible_duration_rejected_before_environment(self):
  with mock.patch("sys.argv",["sdkperf_harness.py","--messages","1000","--rate","100","--seconds","10"]),self.assertRaisesRegex(RuntimeError,"at least 30"):h.main()
 def test_consumer_evidence_streams_unique_run_ids_beyond_log_cap(self):
  evidence=h.ConsumerEvidence("run",1000)
  for i in range(1000):
   line=f"sdkperf-run-{i:06d} "+("x"*8192)+"\n";evidence.observe(line);evidence.observe(line)
  evidence.observe("sdkperf-other-000001\n")
  self.assertEqual(evidence.count(),1000)
 def test_stop_interrupts_final_consumer_evidence_wait(self):
  evidence=h.ConsumerEvidence("run",1);evidence.observe("sdkperf-run-000001\n");h.stopping.set()
  try:
   with self.assertRaisesRegex(RuntimeError,"stopped"):h.wait_consumer_evidence(evidence,FakeProcess(),1,1)
  finally:h.stopping.clear()
 def test_final_consumer_evidence_wait_returns_exact_unique_count(self):
  evidence=h.ConsumerEvidence("run",2);evidence.observe("sdkperf-run-000001 sdkperf-run-000001\n");evidence.observe("sdkperf-run-000002\n")
  self.assertEqual(h.wait_consumer_evidence(evidence,FakeProcess(),2,1),2)
 def test_stop_interrupts_version_probe(self):
  fake=FakeProcess();fake.stdout=io.StringIO("")
  h.stopping.set()
  try:
   with mock.patch.object(h,"spawn",return_value=fake),mock.patch.dict(os.environ,{"SWLB_SDKPERF_JAVA_HOME":"/java"}),self.assertRaisesRegex(RuntimeError,"stopped"):h.version_of("sdkperf")
  finally:h.stopping.clear()
 def test_producer_evidence_requires_sdkperf_total(self):
  with tempfile.TemporaryDirectory() as d:
   path=Path(d)/"producer.log";path.write_text("Total Messages transmitted = 100\n")
   self.assertEqual(h.producer_transmitted(path),100)
   path.write_text("publisher exited without statistics\n");self.assertIsNone(h.producer_transmitted(path))
 def test_stop_children_targets_process_groups_even_after_wrapper_exit(self):
  fake=FakeProcess(returncode=0);h.children[:]=[fake]
  with mock.patch("os.killpg") as killpg:
   h.stop_children();self.assertEqual(killpg.call_args_list[0].args,(fake.pid,signal.SIGTERM));self.assertEqual(killpg.call_args_list[-1].args,(fake.pid,signal.SIGKILL))
  h.children.clear()

if __name__=="__main__":unittest.main()
