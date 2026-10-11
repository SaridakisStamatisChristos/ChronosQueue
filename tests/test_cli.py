from __future__ import annotations
import json, os, subprocess, sys, tempfile, threading, unittest
from http.server import ThreadingHTTPServer
from chronosqueue.api import Handler
from chronosqueue.clock import FakeClock
from chronosqueue.store import Store

class CliCase(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.db=os.path.join(self.tmp.name,"q.db")
        self.store=Store(self.db,clock=FakeClock(1_700_000_000_000_000)); self.store.migrate()
        self.server=ThreadingHTTPServer(("127.0.0.1",0),Handler); self.server.store=self.store
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True); self.thread.start()
        self.base=f"http://127.0.0.1:{self.server.server_port}"
        self.env=os.environ.copy(); self.env["PYTHONPATH"]=os.path.abspath("src")
    def tearDown(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join(timeout=2); self.tmp.cleanup()
    def cli(self,*args):
        p=subprocess.run([sys.executable,"-m","chronosqueue.cli","--url",self.base,*args],env=self.env,text=True,capture_output=True,check=True)
        return p.stdout
    def test_cli_submit_list_json_after_subcommand(self):
        out=self.cli("submit","--queue","cli","--payload",'{"x":1}',"--json")
        jid=json.loads(out)["job"]["id"]
        listing=json.loads(self.cli("list","--queue","cli","--json"))
        self.assertEqual(jid,listing["items"][0]["id"])
    def test_cli_claim_complete(self):
        jid=json.loads(self.cli("submit","--queue","cli","--payload","{}","--json"))["job"]["id"]
        claim=json.loads(self.cli("claim","--worker","w","--queue","cli","--lease-ms","1000","--json"))
        done=json.loads(self.cli("complete","--worker","w","--job",jid,"--lease-token",claim["lease_token"],"--data",'{"ok":true}',"--json"))
        self.assertEqual("succeeded",done["job"]["state"])

if __name__=='__main__': unittest.main()
