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
 def test_missing_consumer_fails_readiness(self):
  with tempfile.TemporaryDirectory() as d:
   p=FakeProcess(returncode=1)
   with self.assertRaisesRegex(RuntimeError,"consumer exited"):h.wait_consumer_ready(Path(d)/"missing.log",p,.1)
 def test_consumer_evidence_requires_unique_run_ids(self):
  with tempfile.TemporaryDirectory() as d:
   path=Path(d)/"consumer.log";path.write_text("sdkperf-run-000001\nsdkperf-run-000001\nsdkperf-run-000002\nother-000003\n")
   count,_=h.count_consumer_results(path,"run");self.assertEqual(count,2)
 def test_producer_evidence_requires_sdkperf_total(self):
  with tempfile.TemporaryDirectory() as d:
   path=Path(d)/"producer.log";path.write_text("Total Messages transmitted = 100\n")
   self.assertEqual(h.producer_transmitted(path),100)
   path.write_text("publisher exited without statistics\n");self.assertIsNone(h.producer_transmitted(path))
 def test_stop_children_targets_process_groups(self):
  fake=FakeProcess();h.children[:]=[fake]
  with mock.patch("os.killpg") as killpg:
   h.stop_children();self.assertEqual(killpg.call_args_list[0].args,(fake.pid,signal.SIGTERM));self.assertEqual(killpg.call_args_list[-1].args,(fake.pid,signal.SIGKILL))
  h.children.clear()

if __name__=="__main__":unittest.main()
