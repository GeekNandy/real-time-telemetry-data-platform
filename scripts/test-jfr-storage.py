#!/usr/bin/env python3
"""Use an actual JVM to verify that JFR working chunks use the recording volume."""
import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('capacity_run',ROOT/'scripts/capacity-run.py')
CAPACITY=importlib.util.module_from_spec(spec);spec.loader.exec_module(CAPACITY)

PROBE='''
import java.nio.file.Files;
import java.nio.file.Path;
class RepositoryProbe {
  public static void main(String[] args) throws Exception {
    try (var chunks = Files.walk(Path.of(args[0]))) {
      if (chunks.noneMatch(p -> Files.isRegularFile(p) && p.toString().endsWith(".jfr")))
        throw new AssertionError("No JFR chunk in the configured recording volume");
    }
    try (var temporary = Files.walk(Path.of(System.getProperty("java.io.tmpdir")))) {
      if (temporary.anyMatch(p -> p.toString().endsWith(".jfr")))
        throw new AssertionError("JFR used the application's temporary filesystem");
    }
    System.out.println("REPOSITORY_OK");
  }
}
'''

class JfrStorage(unittest.TestCase):
    def test_live_jvm_writes_working_chunks_outside_application_tmp(self):
        java=str(Path(os.environ['JAVA_HOME'])/'bin/java') if 'JAVA_HOME' in os.environ else shutil.which('java')
        self.assertTrue(java,'A JDK is required; this test must not silently skip')
        with tempfile.TemporaryDirectory(prefix='jfr-storage-') as folder:
            root=Path(folder);temporary=root/'application-tmp';temporary.mkdir()
            profiles=root/'profiles';profiles.mkdir()
            source=root/'RepositoryProbe.java';source.write_text(PROBE)
            recording=profiles/'worker.jfr'
            process=subprocess.run([java,f'-Djava.io.tmpdir={temporary}',
                CAPACITY.jfr_repository_option('worker',profiles),
                f'-XX:StartFlightRecording=name=probe,settings=default,maxsize=16m,dumponexit=true,filename={recording}',
                str(source),str(profiles/'worker-repository')],capture_output=True,text=True,timeout=45)
            self.assertEqual(0,process.returncode,(process.stdout+process.stderr)[-4000:])
            self.assertIn('REPOSITORY_OK',process.stdout)
            self.assertTrue(recording.is_file());self.assertGreater(recording.stat().st_size,0)

    def test_roles_have_separate_repositories_and_unknown_roles_are_rejected(self):
        self.assertNotEqual(CAPACITY.jfr_repository_option('collector'),CAPACITY.jfr_repository_option('worker'))
        with self.assertRaises(ValueError):CAPACITY.jfr_repository_option('unknown')

if __name__=='__main__':unittest.main()
