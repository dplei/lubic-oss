#!/usr/bin/env python3
"""UPOS 协议实测工具；不接入产品录制链路、不投稿。配置/凭证仅放私有运行目录。"""
import argparse
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import signal
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


def read(path):
    return json.loads(path.read_text())


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def save(path, value):
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w") as f:
        json.dump(value, f, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    sync_dir(path.parent)


def log(event, **fields):
    print(json.dumps({"at": time.time(), "event": event, **fields}), flush=True)


def locked(root, role):
    f = (root / (role + ".lock")).open("a")
    fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    return f


def object_url(ticket):
    endpoint = ticket["endpoint"]
    if endpoint.startswith("//"):
        endpoint = "https:" + endpoint
    return endpoint.rstrip("/") + "/" + ticket["upos_uri"].removeprefix("upos://")


def request(ticket, method, query=None, body=None):
    url = object_url(ticket)
    if query:
        url += "?" + urllib.parse.urlencode(query)
    headers = {"X-Upos-Auth": ticket["auth"]}
    if isinstance(body, dict):
        body = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, method=method, data=body, headers=headers)
    return urllib.request.urlopen(req, timeout=45)


def fetch(ticket, method, query=None, body=None):
    with request(ticket, method, query, body) as res:
        return res.read()


def retry(label, call, **fields):
    for attempt in range(4):
        try:
            return call()
        except (OSError, urllib.error.URLError) as e:
            log(label + "_failed", attempt=attempt, error_type=type(e).__name__, **fields)
            if attempt == 3:
                raise
            time.sleep(2 ** attempt)


def block_path(root, n):
    return root / "spool" / f"{n:08d}.chunk"


def allocated(root):
    total = 0
    for p in (root / "spool").iterdir():
        try:
            total += p.stat().st_blocks * 512
        except FileNotFoundError:
            pass  # 上传进程可能刚刚确认并回收该块。
    return total


def record(root):
    config = read(root / "job.json")
    ticket = read(root / "ticket.json")
    stream = read(root / "stream.json")
    chunk = ticket["chunk_size"]
    budget = config["buffer_bytes"]
    assert 2 * chunk <= budget, "budget must hold at least two upload blocks"
    state_path = root / "record.json"
    if state_path.exists():
        raise RuntimeError("producer already ran; never overwrite an existing stream")
    (root / "spool").mkdir()
    state = {"started_at": time.time(), "closed": False, "blocks": [], "bytes": 0}
    save(state_path, state)
    headers = "".join(f"{k}: {v}\r\n" for k, v in stream["stream_headers"].items())
    args = ["ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "warning",
            "-rw_timeout", "30000000", "-headers", headers,
            "-i", stream["raw_stream_url"], "-t", str(config["seconds"]),
            "-map", "0:v:0?", "-map", "0:a:0?", "-c", "copy",
            "-progress", str(root / "ffmpeg.progress"), "-stats_period", "5",
            "-f", "flv", "-flvflags", "no_duration_filesize", "pipe:1"]
    digest = hashlib.sha256()
    peak = 0
    error = None
    with (root / "ffmpeg.stderr.log").open("ab") as stderr:
        proc = subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                stderr=stderr, bufsize=0)
        try:
            while True:
                wait = config.get("full_wait_seconds", 30)
                deadline = time.monotonic() + wait
                while allocated(root) + chunk > budget:
                    if time.monotonic() >= deadline:
                        raise RuntimeError(f"buffer full for {wait}s; stopping instead of growing it")
                    time.sleep(.1)
                # 边读边写进 .writing（计入预算），内存只有 64 KiB 读缓冲。
                n = len(state["blocks"])
                path = block_path(root, n)
                tmp = path.with_suffix(".writing")
                size = 0
                block = hashlib.sha256()
                pending = digest.copy()
                with tmp.open("xb") as f:
                    while size < chunk:
                        part = proc.stdout.read(min(65536, chunk - size))
                        if not part:
                            break
                        if state["bytes"] + size + len(part) > config["declared_bytes"]:
                            raise RuntimeError("stream exceeded declared UPOS size")
                        f.write(part)
                        block.update(part)
                        pending.update(part)
                        size += len(part)
                    f.flush()
                    os.fsync(f.fileno())
                if not size:
                    tmp.unlink()
                    break
                # 先让块文件就位，再登记；登记过的块号一定能在 spool 里找到。
                os.replace(tmp, path)
                sync_dir(path.parent)
                digest = pending
                state["blocks"].append({"size": size, "sha256": block.hexdigest()})
                state["bytes"] += size
                save(state_path, state)
                peak = max(peak, allocated(root))
                log("recorded", block=n, bytes=state["bytes"], allocated=allocated(root))
                if size < chunk:
                    break
            code = proc.wait(timeout=10)
            if code:
                error = f"ffmpeg exited {code}"
        except BaseException as e:
            error = f"{type(e).__name__}: {e}"
            proc.kill()
            proc.wait(timeout=10)
        finally:
            proc.stdout.close()
    state.update(closed=True, sha256=digest.hexdigest(), ended_at=time.time(),
                 peak_allocated=peak, error=error)
    save(state_path, state)
    log("record_closed", bytes=state["bytes"], error=error, peak_allocated=peak)
    if error:
        raise RuntimeError(error)


CRASH_AFTER_PUT_EXIT = 75
CRASH_AFTER_PUT_ENV = "UPOS_PROBE_CRASH_AFTER_PUT"
crash = os._exit  # 跳过所有清理，等同进程被杀；selftest 会替换它。


def upload(root, stop_after=None, crash_after_put=None):
    config = read(root / "job.json")
    ticket = read(root / "ticket.json")
    path = root / "upload.json"
    state = read(path) if path.exists() else {"done": [], "bytes": 0, "complete": False}
    if state["complete"]:
        return
    if "upload_id" not in state:
        with request(ticket, "POST", {"uploads": "", "output": "json"}, b"") as res:
            state["upload_id"] = json.load(res)["upload_id"]
        state["started_at"] = time.time()
        save(path, state)
    log("uploader_started", resumed_blocks=len(state["done"]), pid=os.getpid())
    while True:
        if not (root / "record.json").exists():
            time.sleep(.1)
            continue
        recording = read(root / "record.json")
        for n in state["done"]:
            p = block_path(root, n)
            if p.exists():
                p.unlink()
                sync_dir(p.parent)
        n = len(state["done"])
        assert state["done"] == list(range(n)), "checkpoint has gaps"
        if n < len(recording["blocks"]):
            p = block_path(root, n)
            if not p.exists():
                if recording["closed"]:
                    raise RuntimeError(f"unacknowledged block {n} missing; cannot recover")
                time.sleep(.1)
                continue
            meta = recording["blocks"][n]
            data = p.read_bytes()
            assert len(data) == meta["size"] and hashlib.sha256(data).hexdigest() == meta["sha256"]
            start = n * ticket["chunk_size"]
            query = {"uploadId": state["upload_id"], "chunks": math.ceil(config["declared_bytes"] / ticket["chunk_size"]),
                     "total": config["declared_bytes"], "chunk": n, "size": len(data),
                     "partNumber": n + 1, "start": start, "end": start + len(data)}
            retry("put", lambda: fetch(ticket, "PUT", query, data), block=n)
            if crash_after_put == n:
                # 远端已确认、断点未写：重启后必须原块号重传，本地块不能已删。
                log("simulated_crash_after_put", block=n)
                crash(CRASH_AFTER_PUT_EXIT)
            state["done"].append(n)
            state["bytes"] += len(data)
            state["last_put_at"] = time.time()
            state.setdefault("first_put_at", state["last_put_at"])
            save(path, state)
            # 远端确认 + 断点原子替换/同步成功之后才删除；崩溃最多重传同一个块号。
            p.unlink()
            sync_dir(p.parent)
            log("uploaded_and_released", block=n, bytes=state["bytes"], allocated=allocated(root))
            if stop_after is not None and len(state["done"]) >= stop_after:
                return
        elif recording["closed"]:
            assert recording["bytes"] > 0, "empty source"
            if state.get("phase") == "completing":
                # 上次 complete 发出后结果未知；重复 complete 是否幂等尚未在真实 UPOS 验证。
                log("complete_resent_after_unknown", blocks=len(state["done"]))
            state["phase"] = "completing"
            save(path, state)
            query = {"name": config["name"], "uploadId": state["upload_id"],
                     "biz_id": ticket["biz_id"], "output": "json", "profile": "ugcupos/bup"}
            body = {"parts": [{"partNumber": x + 1, "eTag": "etag"} for x in state["done"]]}
            result = json.loads(retry("complete", lambda: fetch(ticket, "POST", query, body)))
            if result.get("OK") != 1:
                raise RuntimeError("UPOS complete rejected")
            state.update(complete=True, phase="complete", completed_at=time.time())
            save(path, state)
            log("complete", bytes=state["bytes"], blocks=len(state["done"]),
                session_seconds=state["completed_at"] - state["started_at"])
            return
        else:
            time.sleep(.1)


def verify(root):
    ticket = read(root / "ticket.json")
    source = read(root / "record.json")
    uploaded = read(root / "upload.json")
    assert uploaded["complete"] and source["closed"]
    digest = hashlib.sha256()
    count = 0
    # stderr 写文件：写 stdin 期间没人读管道，错误多时会互相卡死。
    warnings = (root / "ffprobe.stderr.log").open("w+b")
    probe = subprocess.Popen(["ffprobe", "-v", "error", "-count_packets", "-show_entries",
                              "stream=codec_type,codec_name,width,height,r_frame_rate,nb_read_packets:format=duration",
                              "-of", "json", "-i", "pipe:0"], stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=warnings)
    try:
        with request(ticket, "GET") as res:
            for i, meta in enumerate(source["blocks"]):
                data = res.read(meta["size"])
                assert len(data) == meta["size"] and hashlib.sha256(data).hexdigest() == meta["sha256"], f"remote block {i} differs"
                digest.update(data)
                count += len(data)
                probe.stdin.write(data)
            assert res.read(1) == b"", "remote object has extra data"
        probe.stdin.close()
        probe.stdin = None
        stdout, _ = probe.communicate(timeout=60)
        assert probe.returncode == 0, "ffprobe rejected the merged object"
        assert count == source["bytes"] and digest.hexdigest() == source["sha256"]
        media = json.loads(stdout)
        assert any(s["codec_type"] == "video" for s in media["streams"])
        result = {"verified_at": time.time(), "bytes": count, "sha256": digest.hexdigest(),
                  "blocks": len(source["blocks"]), "ffprobe": media,
                  "ffprobe_warning": (root / "ffprobe.stderr.log").read_text(errors="replace")[:1000],
                  "source_error": source.get("error")}
        save(root / "verified.json", result)
        log("verified", bytes=count, blocks=result["blocks"], sha256=result["sha256"])
    finally:
        if probe.poll() is None:
            probe.kill()
            probe.wait()
        warnings.close()


def run(root):
    """守护一次实测：录制不中断，块 3 远端确认后、断点落盘前让上传器崩溃再重启，结束后自动校验。"""
    config = read(root / "job.json")
    assert not (root / "record.json").exists(), "use a fresh job directory"
    logs = [(root / (role + ".log")).open("ab") for role in ("record", "upload")]
    errors = [(root / (role + ".stderr.log")).open("ab") for role in ("record", "upload")]
    def start(role, i, env=None):
        return subprocess.Popen([sys.executable, str(Path(__file__).resolve()), role, str(root)],
                                stdout=logs[i], stderr=errors[i], start_new_session=True,
                                env=env and {**os.environ, **env})
    producer = start("record", 0)
    worker = start("upload", 1, {CRASH_AFTER_PUT_ENV: "3"})
    begin = time.time()
    peak = 0
    restart = None
    rss = 0
    rss_at = 0
    try:
        with (root / "metrics.jsonl").open("a") as metrics:
            while True:
                produced = read(root / "record.json") if (root / "record.json").exists() else {}
                uploaded = read(root / "upload.json") if (root / "upload.json").exists() else {}
                disk = allocated(root) if (root / "spool").exists() else 0
                peak = max(peak, disk)
                assert disk <= config["buffer_bytes"], "disk buffer exceeded budget"
                if time.time() - rss_at >= 10:
                    table = subprocess.check_output(["ps", "-axo", "pid=,ppid=,rss="], text=True)
                    rows = [list(map(int, line.split())) for line in table.splitlines() if line.strip()]
                    children = {producer.pid, worker.pid}
                    while True:
                        found = {pid for pid, parent, _ in rows if parent in children} - children
                        if not found:
                            break
                        children.update(found)
                    rss = sum(kb * 1024 for pid, _, kb in rows if pid in children)
                    rss_at = time.time()
                status = {"at": time.time(), "phase": "recording", "record_pid": producer.pid,
                          "upload_pid": worker.pid, "recorded_bytes": produced.get("bytes", 0),
                          "uploaded_bytes": uploaded.get("bytes", 0), "allocated_bytes": disk,
                          "peak_sampled_bytes": peak, "done_blocks": len(uploaded.get("done", [])),
                          "restarted": restart is not None, "workers_rss_bytes": rss}
                metrics.write(json.dumps(status) + "\n"); metrics.flush()
                save(root / "status.json", status)
                if restart is None and worker.poll() == CRASH_AFTER_PUT_EXIT:
                    # 第一个上传器在块 3 远端确认后、断点落盘前崩溃；新进程必须原块号重传。
                    uploaded = read(root / "upload.json")
                    assert uploaded["done"] == [0, 1, 2] and block_path(root, 3).exists()
                    assert producer.poll() is None
                    restart = {"at": time.time(), "old_pid": worker.pid, "producer_pid": producer.pid,
                               "crash": "after_put_before_checkpoint", "reput_block": 3,
                               "done_before": len(uploaded["done"]), "released_bytes": uploaded["bytes"],
                               "session_hash": hashlib.sha256(uploaded["upload_id"].encode()).hexdigest()}
                    worker = start("upload", 1)
                    restart["new_pid"] = worker.pid
                    save(root / "restart.json", restart)
                    log("uploader_restarted_after_crash", block=3, producer_pid=producer.pid)
                if worker.poll() is not None and worker.returncode != 0:
                    raise RuntimeError(f"upload worker failed ({worker.returncode}); inspect upload.stderr.log")
                if producer.poll() is not None and producer.returncode != 0:
                    raise RuntimeError(f"producer failed ({producer.returncode}); inspect record.stderr.log")
                if uploaded.get("complete"):
                    assert produced.get("closed") and not produced.get("error")
                    if restart:
                        assert hashlib.sha256(uploaded["upload_id"].encode()).hexdigest() == restart["session_hash"]
                        assert producer.pid == restart["producer_pid"]
                    break
                if time.time() - begin > config["seconds"] + 240:
                    raise TimeoutError("live experiment exceeded its time budget")
                time.sleep(1)
        status["phase"] = "verifying"; save(root / "status.json", status)
        verify(root)
        uploaded = read(root / "upload.json")
        duration = uploaded["completed_at"] - uploaded["first_put_at"]
        passed = restart is not None and duration >= config["min_session_seconds"]
        status.update(phase="passed" if passed else "incomplete", completed_at=time.time(),
                      first_put_to_complete_seconds=duration, checkpoint_resume_verified=restart is not None,
                      byte_hash_verified=True, peak_sampled_bytes=peak,
                      declared_limit=read(root / "job.json")["declared_bytes"])
        save(root / "status.json", status)
        log(status["phase"], first_put_to_complete_seconds=duration, peak_sampled_bytes=peak)
    except BaseException as e:
        save(root / "status.json", {"phase": "failed", "at": time.time(),
                                    "error": f"{type(e).__name__}: {e}", "peak_sampled_bytes": peak})
        raise
    finally:
        # ponytail: 守护进程自己被 SIGKILL 时子进程（独立会话）会成孤儿，正式版交给 launchd/agent 管。
        for proc in (producer, worker):
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGTERM)
        for proc in (producer, worker):
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
        for f in logs + errors:
            f.close()


def selftest():
    import tempfile
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    parts = {}
    puts = []
    merged = []
    fake = {"put_delay": 0, "drop_complete": False}
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            q = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query, keep_blank_values=True)
            if "uploads" in q:
                result = {"upload_id": "probe-session"}
            else:
                names = [p["partNumber"] for p in json.loads(body)["parts"]]
                assert names == list(range(1, len(names) + 1))
                merged.append(b"".join(parts[n] for n in names))
                if fake["drop_complete"]:
                    # 服务端已合并，但响应丢了：客户端只能看到断连。
                    fake["drop_complete"] = False
                    self.close_connection = True
                    return
                result = {"OK": 1}
            data = json.dumps(result).encode()
            self.send_response(200); self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data)

        def do_PUT(self):
            q = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
            n = int(q["partNumber"][0])
            assert q["uploadId"] == ["probe-session"]
            time.sleep(fake["put_delay"])
            parts[n] = self.rfile.read(int(self.headers["Content-Length"]))
            puts.append(n)
            self.send_response(200); self.send_header("Content-Length", "0"); self.end_headers()

    class Crashed(Exception):
        pass

    def simulated_crash(code):
        raise Crashed(code)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    ticket = {"endpoint": f"http://127.0.0.1:{server.server_port}", "upos_uri": "upos://bucket/probe.flv",
              "auth": "test", "biz_id": 1}
    try:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); (root / "spool").mkdir()
            save(root / "job.json", {"name": "probe.flv", "declared_bytes": 40})
            save(root / "ticket.json", {**ticket, "chunk_size": 10})
            chunks = [b"a" * 10, b"b" * 10, b"c" * 7]
            for i, data in enumerate(chunks): block_path(root, i).write_bytes(data)
            save(root / "record.json", {"closed": True, "bytes": 27, "blocks": [{"size": len(d), "sha256": hashlib.sha256(d).hexdigest()} for d in chunks]})
            # 断点落盘失败：远端已有块，但本地必须仍在。
            original = globals()["save"]
            def fail_checkpoint(path, value):
                if path.name == "upload.json" and value["done"]:
                    raise OSError("simulated fsync failure")
                original(path, value)
            globals()["save"] = fail_checkpoint
            try:
                upload(root)
                raise AssertionError("checkpoint failure was ignored")
            except OSError:
                assert block_path(root, 0).exists()
            finally:
                globals()["save"] = original
            # 远端确认后、断点落盘前崩溃：块还在，断点没记，重启后原块号重传。
            globals()["crash"] = simulated_crash
            try:
                upload(root, crash_after_put=0)
                raise AssertionError("simulated crash did not happen")
            except Crashed:
                assert block_path(root, 0).exists() and read(root / "upload.json")["done"] == []
            finally:
                globals()["crash"] = os._exit
            upload(root, stop_after=1)
            assert puts == [1, 1, 1], "block 0 must be re-put under the same part number"
            assert not block_path(root, 0).exists() and not merged
            first = read(root / "upload.json")
            assert first["done"] == [0]
            # complete 已在服务端生效但响应丢失：重发，结果不虚报。
            fake["drop_complete"] = True
            upload(root)
            final = read(root / "upload.json")
            assert final["upload_id"] == first["upload_id"] and final["complete"]
            assert merged == [b"".join(chunks)] * 2 and not list((root / "spool").iterdir())

        # 录制端预算：用假 ffmpeg 一次吐出约 5 块，上传慢于录制，缓存必须顶到预算后等待。
        with tempfile.TemporaryDirectory() as tmp:
            bin_dir = Path(tmp) / "bin"; bin_dir.mkdir()
            (bin_dir / "ffmpeg").write_text("#!/bin/sh\nhead -c 40000 /dev/urandom\n")
            (bin_dir / "ffmpeg").chmod(0o700)
            path_before = os.environ["PATH"]
            os.environ["PATH"] = f"{bin_dir}:{path_before}"
            try:
                chunk, budget = 8192, 16384
                def job(name, **extra):
                    root = Path(tmp) / name; root.mkdir()
                    save(root / "job.json", {"name": "probe.flv", "declared_bytes": 65536, "seconds": 1,
                                             "buffer_bytes": budget, **extra})
                    save(root / "ticket.json", {**ticket, "chunk_size": chunk})
                    save(root / "stream.json", {"raw_stream_url": "fake://", "stream_headers": {}})
                    return root
                # 没有上传器：写满两块后等待超时就停，绝不写第三块。
                root = job("stalled", full_wait_seconds=0.5)
                try:
                    record(root)
                    raise AssertionError("full buffer did not stop the producer")
                except RuntimeError as e:
                    assert "buffer full" in str(e)
                state = read(root / "record.json")
                assert state["closed"] and len(state["blocks"]) == 2 and allocated(root) <= budget
                # 上传器每块慢 0.3 秒：录制端顶到预算等待，最终整流一致。
                root = job("throttled")
                fake["put_delay"] = .3
                failures = []
                def produce():
                    try:
                        record(root)
                    except BaseException as e:
                        failures.append(e)
                producer = threading.Thread(target=produce); producer.start()
                upload(root)
                producer.join()
                assert not failures, failures
                state = read(root / "record.json")
                assert state["bytes"] == 40000 and len(state["blocks"]) == 5 and not state["error"]
                assert state["peak_allocated"] == budget, "backpressure path was not reached"
                assert hashlib.sha256(merged[-1]).hexdigest() == state["sha256"]
            finally:
                os.environ["PATH"] = path_before
        print("PASS: checkpoint failure and crash-after-put keep bytes and re-put; one session; "
              "complete resent after lost response; producer waits at budget and stops when stalled")
    finally:
        server.shutdown(); server.server_close()

if __name__ == "__main__":
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("role", choices=["record", "upload", "verify", "run", "selftest"])
    parser.add_argument("root", type=Path, nargs="?")
    args = parser.parse_args()
    if args.role == "selftest":
        selftest()
    else:
        if args.root is None:
            parser.error("root is required")
        lock = locked(args.root, args.role)
        # 守护进程 SIGTERM 时让 record 走 except，照常写 closed/error。
        signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
        if args.role == "upload":
            crash_at = os.environ.get(CRASH_AFTER_PUT_ENV)
            upload(args.root, crash_after_put=int(crash_at) if crash_at else None)
        else:
            {"record": record, "verify": verify, "run": run}[args.role](args.root)
