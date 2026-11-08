"""Git failure diagnostics and deadlines, including pipe-inheriting helpers."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch


SCRIPT = Path(__file__).resolve().parents[1] / 'foundation/task_runtime/git_support.py'


class GitProcessTest(unittest.TestCase):
    def assert_process_exited(self, pid):
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            stat = Path(f'/proc/{pid}/stat')
            try:
                if stat.exists() and stat.read_text().split()[2] in ('Z', 'X'):
                    return
                os.kill(pid, 0)
            except (ProcessLookupError, FileNotFoundError):
                return
            time.sleep(0.01)
        self.fail('Owned process did not exit after cancellation')

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        self.env = dict(os.environ, PATH=str(self.bin) + os.pathsep + os.environ['PATH'])

    def fake_git(self, body):
        path = self.bin / 'git'
        path.write_text('#!' + sys.executable + '\n' + body)
        path.chmod(0o755)

    def detached_defaults(self):
        # Preserve all inherited Git config/credential selectors while making
        # the repository/environment request the unsafe default for this test.
        env = dict(self.env)
        count = int(env.get('GIT_CONFIG_COUNT', '0'))
        for key in ('maintenance.autoDetach', 'gc.autoDetach'):
            env['GIT_CONFIG_KEY_' + str(count)] = key
            env['GIT_CONFIG_VALUE_' + str(count)] = 'true'
            count += 1
        env['GIT_CONFIG_COUNT'] = str(count)
        return env

    def test_auto_maintenance_is_foreground_without_editing_repository_config(self):
        env = self.detached_defaults()
        subprocess.run(['git', 'init', str(self.root)], env=env, capture_output=True, check=True)
        config = self.root / '.git/config'
        original = config.read_bytes() + b'\n[maintenance]\n auto = true\n autoDetach = true\n[gc]\n autoDetach = true\n'
        config.write_bytes(original)
        body = ('print(json.dumps([git(repo,"config","--get",key) for key in '
                '("maintenance.autoDetach","gc.autoDetach","maintenance.auto")]))\n')
        with patch.object(self, 'env', env):
            result = self.call(body)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), ['false', 'false', 'true'])
        self.assertEqual(config.read_bytes(), original)

    @unittest.skipUnless(sys.platform == 'linux', 'Needs Linux descendant supervision')
    def test_repeated_real_fetch_does_not_orphan_automatic_maintenance(self):
        trace = self.root / 'trace.json'
        env = dict(self.detached_defaults(), GIT_TRACE2_EVENT=str(trace))
        probe = Path(__file__).with_name('fixtures') / 'git_maintenance_probe.py'
        result = subprocess.run([sys.executable, str(probe), str(self.root), str(SCRIPT)],
                                env=env, capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        value = json.loads(result.stdout)
        if value.get('unsupported'):
            self.skipTest(value['unsupported'])
        self.assertEqual(value['reaped_orphans'], 0)
        events = [json.loads(line) for line in trace.read_text().splitlines()]
        self.assertTrue(any(e.get('event') == 'cmd_name' and e.get('name') == 'maintenance' for e in events))

    def call(self, body, seconds='2', ready=None):
        # Only the fixture startup is gated. Production deadlines are unchanged.
        handshake = ''
        if ready is not None:
            handshake = ('import pathlib,time\nowner=runtime["ProcessOwner"]\noriginal_start=owner.start\n'
                         'def start(self,*args,**kwargs):\n global fixture_ready_at\n original_start(self,*args,**kwargs)\n'
                         ' deadline=time.monotonic()+10\n'
                         f' marker=pathlib.Path({str(ready)!r})\n'
                         ' while True:\n'
                         '  try:\n   if int(marker.read_text())>0: break\n'
                         '  except (FileNotFoundError,ValueError): pass\n'
                         '  if time.monotonic()>=deadline: raise AssertionError("fixture readiness timeout")\n'
                         '  time.sleep(0.01)\n'
                         ' fixture_ready_at=time.monotonic()\n'
                         'owner.start=start\n')
        code = ('import json,runpy,sys\nruntime=runpy.run_path(sys.argv[1])\ngit=runtime["git"]\n'
                'repo=sys.argv[2]\n' + handshake + body)
        if ready is not None:
            code += '\nprint(json.dumps({"execution_seconds":time.monotonic()-fixture_ready_at}))\n'
        return subprocess.run([sys.executable, '-c', code, str(SCRIPT), str(self.root)],
                              env=dict(self.env, TASKS_GIT_TIMEOUT_SECONDS=seconds),
                              capture_output=True, text=True, timeout=15 if ready else 5)

    def test_diagnostics_classify_without_echoing_argv_or_remote_output(self):
        output = 'Authentication failed for https://private/VERY_SECRET_TOKEN\n'
        self.fake_git('import sys\nsys.stderr.write(' + repr(output) + ')\nsys.exit(128)\n')
        result = self.call("try: git(repo,'fetch','https://private/ARGV_SECRET')\nexcept RuntimeError: pass\n")
        self.assertEqual(result.returncode, 0, result.stderr)
        record = json.loads(result.stderr)
        self.assertEqual(record['operation'], 'fetch')
        self.assertEqual(record['category'], 'authentication')
        self.assertEqual(record['exit_code'], 128)
        self.assertEqual(record['stderr_bytes'], len(output.encode()))
        self.assertEqual(record['output'], 'omitted')
        self.assertEqual(result.stdout, '')
        for secret in ('VERY_SECRET_TOKEN', 'ARGV_SECRET', 'https://private'):
            self.assertNotIn(secret, result.stderr + result.stdout)

    def test_false_predicate_is_quiet_and_git_uses_process_cwd(self):
        self.fake_git('import sys,os\nassert "-C" not in sys.argv\nsys.exit(1)\n')
        result = self.call("print(json.dumps(git(repo,'merge-base','--is-ancestor','a','b',predicate=True)))\n")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, 'false\n')
        self.assertEqual(result.stderr, '')

    def test_only_porcelain_push_rejection_is_definite(self):
        row = '!\tPRIVATE_SOURCE:refs/heads/main\t[rejected] (stale info)\n'
        cas_row = row.replace('[rejected] (stale info)', '[remote rejected] (incorrect old value provided)')
        cases = [(row, '', True, True), (cas_row, '', True, True),
                 ('', row, True, False), (row, '', False, False)]
        for stdout, stderr, porcelain, definite in cases:
            with self.subTest(porcelain=porcelain, stdout=bool(stdout)):
                self.fake_git('import sys\nsys.stdout.write(' + repr(stdout) + ')\n'
                              'sys.stderr.write(' + repr(stderr) + ')\nsys.exit(1)\n')
                option = "'--porcelain'," if porcelain else ''
                result = self.call("try: git(repo,'push'," + option + "'origin','PRIVATE_REFSPEC')\n"
                                   "except RuntimeError as error: print(json.dumps(error.definite_rejection))\n")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(json.loads(result.stdout), definite)
                self.assertEqual(json.loads(result.stderr)['definite_rejection'], definite)
                self.assertNotIn('PRIVATE_', result.stderr)

    def test_first_failure_remains_before_cleanup_failure(self):
        self.fake_git('import sys\nsys.stderr.write("sensitive unknown output")\nsys.exit(2)\n')
        result = self.call("for operation in ('fetch','update-ref'):\n try: git(repo,operation,'sensitive-argument')\n except RuntimeError: pass\n")
        records = [json.loads(line) for line in result.stderr.splitlines()]
        self.assertEqual([r['operation'] for r in records], ['fetch', 'update-ref'])
        self.assertLess(records[0]['event'], records[1]['event'])
        self.assertTrue(all(r['category'] == 'unknown' for r in records))
        self.assertNotIn('sensitive', result.stderr)

    def test_timeout_kills_helper_after_direct_git_exits(self):
        pidfile = self.root / 'helper.pid'
        child = ('import os,signal,time,pathlib; time.sleep(0.4); '
                 'signal.signal(signal.SIGTERM,signal.SIG_IGN); '
                 f'pathlib.Path({str(pidfile)!r}).write_text(str(os.getpid())); '
                 'time.sleep(30)')
        self.fake_git('import os,subprocess,sys\n'
                      f'subprocess.Popen([sys.executable,"-c",{child!r}])\n'
                      'os._exit(0)\n')
        result = self.call("try: git(repo,'push','origin','private-ref')\nexcept RuntimeError: pass\n", seconds='0.3', ready=pidfile)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertLess(json.loads(result.stdout)['execution_seconds'], 2)
        record = json.loads(result.stderr)
        self.assertTrue(record['timeout'])
        self.assertEqual(record['operation'], 'push')
        self.assertEqual(record['category'], 'timeout')
        pid = int(pidfile.read_text())
        try:
            self.assert_process_exited(pid)
        finally:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

    def test_timeout_values_must_be_finite_positive(self):
        for value in ('0', '-1', 'nan', 'inf', 'PRIVATE_CONFIG_VALUE'):
            with self.subTest(value=value):
                result = self.call("try: git(repo,'fetch')\nexcept ValueError: print('rejected')\n", seconds=value)
                self.assertEqual(result.stdout, 'rejected\n')

    def test_timeout_keeps_output_counts_without_echoing_output(self):
        output = 'SYNTHETIC_PRIVATE_TIMEOUT_OUTPUT'
        ready = self.root / 'output-ready.pid'
        self.fake_git('import sys,time,os,pathlib\ntime.sleep(0.4)\nsys.stderr.write(' + repr(output) + ')\n'
                      'sys.stderr.flush()\n' + f'pathlib.Path({str(ready)!r}).write_text(str(os.getpid()))\n'
                      'time.sleep(30)\n')
        result = self.call("try: git(repo,'fetch')\nexcept RuntimeError: pass\n", seconds='0.2', ready=ready)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertLess(json.loads(result.stdout)['execution_seconds'], 2)
        record = json.loads(result.stderr)
        self.assertTrue(record['timeout'])
        self.assertEqual(record['stderr_bytes'], len(output))
        self.assertNotIn(output, result.stderr + result.stdout)

    def test_interrupt_stops_detached_git_before_caller_exits(self):
        self.assert_interrupt_stops_git(signal.SIGINT)

    def test_shell_termination_stops_detached_git_before_caller_exits(self):
        for signum in (signal.SIGTERM, signal.SIGHUP):
            with self.subTest(signal=signum):
                self.assert_interrupt_stops_git(signum)

    def assert_interrupt_stops_git(self, signum):
        pidfile = self.root / 'git.pid'
        pidfile.unlink(missing_ok=True)
        self.fake_git('import os,signal,time,pathlib\ntime.sleep(0.4)\n'
                      'signal.signal(signal.SIGTERM,signal.SIG_IGN)\n'
                      f'pathlib.Path({str(pidfile)!r}).write_text(str(os.getpid()))\n'
                      'time.sleep(30)\n')
        code = ('import runpy,sys,signal,json\ngit=runpy.run_path(sys.argv[1])["git"]\n'
                'signals=(signal.SIGTERM,signal.SIGHUP)\n'
                'before=[signal.getsignal(s) for s in signals]\n'
                'try: git(sys.argv[2],"push","origin","private-ref")\n'
                'finally: print(json.dumps(before==[signal.getsignal(s) for s in signals]))\n')
        caller = subprocess.Popen([sys.executable, '-c', code, str(SCRIPT), str(self.root)],
                                  env=dict(self.env, TASKS_GIT_TIMEOUT_SECONDS='15'),
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                  start_new_session=True)
        pid = None
        try:
            deadline = time.monotonic() + 10
            while caller.poll() is None and time.monotonic() < deadline:
                try:
                    pid = int(pidfile.read_text())
                    if pid > 0:
                        break
                except (FileNotFoundError, ValueError):
                    pass
                time.sleep(0.01)
            self.assertIsNotNone(pid, 'fixture readiness timeout')
            os.killpg(caller.pid, signum)
            stdout, _stderr = caller.communicate(timeout=3)
            self.assertNotEqual(caller.returncode, 0)
            self.assertEqual(stdout.strip(), b'true')
            self.assert_process_exited(pid)
        finally:
            if caller.poll() is None:
                os.killpg(caller.pid, signal.SIGKILL)
                caller.communicate(timeout=3)
            for stream in (caller.stdout, caller.stderr):
                stream.close()
            if pid is None:
                try:
                    pid = int(pidfile.read_text())
                except (FileNotFoundError, ValueError):
                    pass
            if pid is not None:
                try:
                    os.killpg(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

    def test_interrupted_process_construction_does_not_orphan_git(self):
        pidfile = self.root / 'construction.pid'
        self.fake_git('import time\ntime.sleep(30)\n')
        body = ('import subprocess,pathlib\noriginal=subprocess.Popen.__init__\n'
                'def interrupt(self,*args,**kwargs):\n'
                ' original(self,*args,**kwargs)\n'
                f' pathlib.Path({str(pidfile)!r}).write_text(str(self.pid))\n'
                ' raise KeyboardInterrupt\n'
                'subprocess.Popen.__init__=interrupt\n'
                'try: git(repo,"push","origin","private-ref")\n'
                'except KeyboardInterrupt: print("interrupted")\n')
        result = self.call(body)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, 'interrupted\n')
        pid = int(pidfile.read_text())
        try:
            self.assert_process_exited(pid)
        finally:
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

    def test_interrupt_at_constructor_return_and_monitor_handoff_is_owned(self):
        self.fake_git('import time\ntime.sleep(30)\n')
        for boundary in ('constructor-return', 'monitor-handoff'):
            with self.subTest(boundary=boundary):
                pidfile = self.root / (boundary + '.pid')
                body = ('import os,pathlib,signal\n'
                        'cls=runtime["OwnedProcess"]\noriginal=cls.__init__\n'
                        'def create(self,*args,**kwargs):\n'
                        ' original(self,*args,**kwargs)\n'
                        f' pathlib.Path({str(pidfile)!r}).write_text(str(self.pid))\n')
                if boundary == 'constructor-return':
                    body += ' os.kill(os.getpid(),signal.SIGINT)\n'
                body += 'cls.__init__=create\n'
                if boundary == 'monitor-handoff':
                    body += ('owner_cls=runtime["ProcessOwner"]\n'
                             'original_start=owner_cls.start\n'
                             'def start(self,*args,**kwargs):\n'
                             ' original_start(self,*args,**kwargs)\n'
                             ' os.kill(os.getpid(),signal.SIGINT)\n'
                             'owner_cls.start=start\n')
                body += ('try: git(repo,"push","origin","private-ref")\n'
                         'except KeyboardInterrupt: print("interrupted")\n')
                result = self.call(body)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, 'interrupted\n')
                self.assert_stopped(int(pidfile.read_text()))

    def test_constructor_exception_closes_all_owned_pipe_streams(self):
        self.fake_git('import time\ntime.sleep(30)\n')
        body = ('import subprocess\ncreated=[]\noriginal=subprocess.Popen.__init__\n'
                'def interrupt(self,*args,**kwargs):\n'
                ' original(self,*args,**kwargs)\n created.append(self)\n raise KeyboardInterrupt\n'
                'subprocess.Popen.__init__=interrupt\n'
                'try: git(repo,"fetch")\nexcept KeyboardInterrupt: pass\n'
                'p=created[0]\n'
                'print(json.dumps({"closed":all(s.closed for s in (p.stdin,p.stdout,p.stderr)),"reaped":p.returncode is not None}))\n')
        result = self.call(body)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), {'closed': True, 'reaped': True})

    def assert_stopped(self, pid):
        try:
            self.assert_process_exited(pid)
        finally:
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

    def test_second_cancellation_during_term_grace_still_reaps(self):
        pidfile = self.root / 'second-interrupt.pid'
        self.fake_git('import signal,time\nsignal.signal(signal.SIGTERM,signal.SIG_IGN)\ntime.sleep(30)\n')
        body = ('import os,pathlib,signal,subprocess\n'
                'cls=runtime["OwnedProcess"]\noriginal=cls.communicate\n'
                'def interrupt(self,*args,**kwargs):\n'
                f' pathlib.Path({str(pidfile)!r}).write_text(str(self.pid))\n'
                ' os.kill(os.getpid(),signal.SIGINT)\n'
                ' return original(self,*args,**kwargs)\n'
                'cls.communicate=interrupt\n'
                'try: git(repo,"push","origin","private-ref")\n'
                'except KeyboardInterrupt: print("interrupted")\n')
        result = self.call(body)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, 'interrupted\n')
        self.assert_stopped(int(pidfile.read_text()))

    def test_first_cancellation_at_cleanup_entry_still_reaps(self):
        pidfile = self.root / 'cleanup-entry.pid'
        self.fake_git('import time\ntime.sleep(30)\n')
        body = ('import os,pathlib,signal\ncls=runtime["ProcessOwner"]\n'
                'original_start=cls.start\noriginal_close=cls.close\n'
                'def fail(self,*args,**kwargs):\n'
                ' original_start(self,*args,**kwargs)\n'
                f' pathlib.Path({str(pidfile)!r}).write_text(str(self.process.pid))\n'
                ' raise RuntimeError("monitor failed")\n'
                'def interrupt(self):\n'
                ' if self.process is not None and not self.closed: os.kill(os.getpid(),signal.SIGINT)\n'
                ' return original_close(self)\n'
                'cls.start=fail\ncls.close=interrupt\n'
                'try: git(repo,"push","origin","private-ref")\n'
                'except KeyboardInterrupt: print("interrupted")\n')
        result = self.call(body)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, 'interrupted\n')
        self.assert_stopped(int(pidfile.read_text()))

    def test_launch_errors_do_not_echo_dynamic_paths(self):
        marker = 'SYNTHETIC_PRIVATE_CWD'
        result = self.call('from pathlib import Path\n'
                           'try: git(Path(repo)/"SYNTHETIC_PRIVATE_CWD", "ls-remote")\n'
                           'except RuntimeError: raise SystemExit(1)\n')
        self.assertEqual(result.returncode, 1)
        self.assertNotIn(marker, result.stderr + result.stdout)
        record = json.loads(result.stderr.splitlines()[0])
        self.assertEqual(record['category'], 'launch_failed')
        self.assertEqual(record['operation'], 'ls-remote')
        self.assertIsNone(record['exit_code'])
        body = ('import os\nos.environ["PATH"]="SYNTHETIC_PRIVATE_EXECUTABLE"\n'
                'try: git(repo,"fetch")\nexcept RuntimeError: pass\n')
        result = self.call(body)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn('SYNTHETIC_', result.stderr)
        self.assertEqual(json.loads(result.stderr)['category'], 'launch_failed')



class GitCleanupFailureTest(unittest.TestCase):
    def setUp(self):
        foundation = str(SCRIPT.parent.parent)
        sys.path.insert(0, foundation)
        self.addCleanup(sys.path.remove, foundation)
        from task_runtime import git_support
        self.support = git_support

    def test_unresponsive_pipe_cleanup_falls_back_to_direct_kill_and_retains_output(self):
        process = Mock(pid=123, returncode=None)
        for name in ('stdin', 'stdout', 'stderr'):
            getattr(process, name).closed = False
        process.communicate.side_effect = subprocess.TimeoutExpired('fixture', 0.2, output=b'out', stderr=b'err')
        with patch.object(self.support.os, 'killpg', side_effect=ProcessLookupError) as groups, \
                patch.object(self.support.os, 'kill', side_effect=ProcessLookupError) as direct:
            self.assertEqual(self.support.stop_group(process), (b'out', b'err'))
        self.assertEqual(groups.call_count, 2)
        direct.assert_called_once_with(123, signal.SIGKILL)
        process.stdout.close.assert_called_once()
        process.stderr.close.assert_called_once()
        process.wait.assert_called_once_with(timeout=0.2)

    def test_owner_closes_every_pipe_even_after_close_errors_and_preserves_cancellation(self):
        owner = self.support.ProcessOwner()
        owner.process = Mock(pid=None)
        owner.process.stdout.close.side_effect = OSError('fixture close failure')
        self.assertEqual(owner.close(), (b'', b''))
        self.assertEqual(owner.close(), (b'', b''))
        for name in ('stdin', 'stdout', 'stderr'):
            getattr(owner.process, name).close.assert_called_once()
        self.assertIsNone(owner.raise_cancelled())
        owner.cancelled = signal.SIGTERM
        with self.assertRaises(SystemExit) as raised:
            owner.raise_cancelled()
        self.assertEqual(raised.exception.code, 128 + signal.SIGTERM)
        owner.cancelled = signal.SIGINT
        with self.assertRaises(KeyboardInterrupt):
            owner.raise_cancelled()


if __name__ == '__main__':
    unittest.main()
