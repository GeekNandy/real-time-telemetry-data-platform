#!/usr/bin/env python3
"""HTTP 401 and healthy connections must never pass as network isolation."""
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import unittest
ROOT=Path(__file__).resolve().parents[1]
class Handler(BaseHTTPRequestHandler):
    def log_message(self,*args):pass
    def do_GET(self):
        self.send_response(401 if self.path=='/private' else 200);self.send_header('Content-Length','0');self.end_headers()
class ProbeContracts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary=tempfile.TemporaryDirectory(prefix='network-probe-contract-');cls.output=Path(cls.temporary.name)
        subprocess.run(['javac','--release','21','-Xlint:all','-Werror','-d',str(cls.output),str(ROOT/'tools/network-probe/src/NetworkProbe.java')],check=True,timeout=30)
        cls.peer=ThreadingHTTPServer(('127.0.0.1',0),Handler);cls.thread=threading.Thread(target=cls.peer.serve_forever,daemon=True);cls.thread.start()
    @classmethod
    def tearDownClass(cls):
        cls.peer.shutdown();cls.peer.server_close();cls.thread.join(timeout=5);cls.temporary.cleanup()
    def probe(self,mode,target,expected):
        return subprocess.run(['java','-Xmx32m','-cp',str(self.output),'NetworkProbe',mode,target,expected],capture_output=True,text=True,timeout=10).returncode
    def test_allowed_http_and_tcp_connect(self):
        self.assertEqual(0,self.probe('http',f'http://127.0.0.1:{self.peer.server_port}/health','allow'))
        self.assertEqual(0,self.probe('tcp',f'127.0.0.1:{self.peer.server_port}','allow'))
    def test_http_401_and_healthy_connection_cannot_pass_as_denial(self):
        for path in ('/health','/private'):self.assertNotEqual(0,self.probe('http',f'http://127.0.0.1:{self.peer.server_port}'+path,'deny'))
    def test_refused_connection_is_denied_but_cannot_be_allowed(self):
        with socket.socket() as reserved:
            reserved.bind(('127.0.0.1',0));port=reserved.getsockname()[1]
            self.assertEqual(0,self.probe('tcp',f'127.0.0.1:{port}','deny'))
            self.assertNotEqual(0,self.probe('tcp',f'127.0.0.1:{port}','allow'))
if __name__=='__main__':unittest.main()
