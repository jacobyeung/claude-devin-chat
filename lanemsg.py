#!/usr/bin/env python3
"""Durable two-way messages between an orchestrator and its Devin or Codex lanes.

Each lane owns a mailbox, `<lane dir>/mail`, with two Maildir-style queues: `inbox/` (orchestrator to lane)
and `outbox/` (lane to orchestrator). A queue has `tmp/` (being written), `new/` (not yet delivered), `cur/`
(delivered and still open) and `done/` (acknowledged or answered). Messages change state only by rename(2),
which is atomic within one filesystem (Lustre included), so a crash never leaves a partial or duplicated
message and exactly one reader claims each message.

Delivery is pushed both ways. Devin lanes receive messages through hooks (`lanemsg.py hook <Event>`, wired by
`.devin/hooks.v1.json` or the user's Devin config): SessionStart, UserPromptSubmit and PostToolUse put new
messages into the lane's context, and Stop keeps the lane working while a message is undelivered or a question
from the orchestrator is unanswered. Messages for the orchestrator go to its session on the host where that
session runs: a Claude Code session through its inbox socket, a Codex session through the host's Codex
app-server daemon, which steers the message into the running turn or queues it as the next turn. A lane on
another host relays the push over ssh. A push that fails is retried with the lane's next message, by
`lanemsg adopt`, or by the lane's launcher. Codex lanes have no hooks and run `lanemsg.py inbox` between work
steps.

`lanemsg run` (or any launcher) puts the lane identity (LANEMSG_DIR, LANEMSG_ATTEMPT) in each attempt's
environment. The hooks bind an attempt to the first Devin session that reports in, so nested Devin processes
that inherit the environment cannot consume the lane's messages.
"""
from __future__ import annotations

import argparse
import base64
import fcntl
import functools
import hashlib
import itertools
import json
import os
import re
import shlex
import shutil
import socket
import subprocess
import sys
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

SELF = Path(__file__).resolve()
KINDS = ("note", "ask", "reply")
MAX_STOP_REMINDERS = 2
BODY_LIMIT = 20000
SUBAGENT_TOOL = "run_subagent"
SUBAGENT_TOOLS = (SUBAGENT_TOOL, "read_subagent")
BACKGROUND_SUBAGENT_HOLD_S = 600
FOREGROUND_SUBAGENT_HOLD_S = 3 * 3600
WEBSOCKET_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class LaneError(Exception):
    pass


def runs_root() -> Path:
    return Path(os.environ.get("LANEMSG_RUNS_ROOT") or Path.home())


def registry() -> Path:
    state = os.environ.get("XDG_STATE_HOME") or Path.home() / ".local" / "state"
    return Path(os.environ.get("LANEMSG_REGISTRY") or Path(state) / "lanemsg" / "lanes")


def claude_sessions() -> Path:
    return Path(os.environ.get("LANEMSG_CLAUDE_SESSIONS", str(Path.home() / ".claude" / "sessions")))


def codex_home() -> Path:
    return Path(os.environ.get("CODEX_HOME") or Path.home() / ".codex")


def devin_user_config() -> Path:
    return Path.home() / ".config" / "devin" / "config.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return default


def write_atomic(path: Path, text: str) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(text)
    if path.exists():
        shutil.copymode(path, temporary)
    os.replace(temporary, path)


def lane_key(lane_dir: Path) -> str:
    lane_dir = Path(lane_dir).resolve()
    try:
        return str(lane_dir.relative_to(runs_root().resolve()))
    except ValueError:
        return str(lane_dir)


def clip(text: str, limit: int = BODY_LIMIT) -> str:
    return text if len(text) <= limit else text[:limit] + f"\n[... {len(text) - limit} more characters]"


@functools.lru_cache(maxsize=1)
def command() -> str:
    """How agents invoke this script: `lanemsg` when that name on PATH runs this file (a symlink to it, or a small
    wrapper script that names it), otherwise this interpreter and this file."""
    on_path = shutil.which("lanemsg")
    if on_path:
        path = Path(on_path)
        if path.resolve() == SELF or (path.stat().st_size < 4096 and str(SELF) in path.read_text(errors="ignore")):
            return "lanemsg"
    return f"{shlex.quote(sys.executable)} {shlex.quote(str(SELF))}"


def proc_stat(pid) -> list | None:
    try:
        return Path(f"/proc/{int(pid)}/stat").read_text().rsplit(")", 1)[1].split()
    except (OSError, TypeError, ValueError, IndexError):
        return None


@functools.lru_cache(maxsize=1)
def pid_domain() -> str:
    """This host's process-id domain, in the form Claude Code records in its session files."""
    try:
        return f"linux:{Path('/etc/machine-id').read_text().strip()}:{os.readlink('/proc/self/ns/pid')}"
    except OSError:
        return ""


def live_local_session(data: dict) -> bool:
    """True if a Claude Code session file describes a live process on this host. ~/.claude/sessions is shared
    by every login node, and a pid only means something on the host that recorded it."""
    fields = proc_stat(data.get("pid"))
    return (bool(fields) and fields[0] not in ("Z", "X")
            and (not data.get("pidDomain") or not pid_domain() or data["pidDomain"] == pid_domain())
            and (not data.get("procStart") or str(data["procStart"]) == fields[19]))


def codex_process(pid) -> bool:
    try:
        return Path(f"/proc/{int(pid)}/comm").read_text().strip() == "codex"
    except (OSError, ValueError):
        return False


def orchestrator_ancestor() -> dict | None:
    """The Claude Code or Codex session this process runs under: the nearest one up the process tree. Codex gives
    every shell command it runs the id of its root session in CODEX_SESSION_ID, also when a subagent thread runs
    the command."""
    pid, seen = os.getpid(), set()
    codex_session = os.environ.get("CODEX_SESSION_ID") or os.environ.get("CODEX_THREAD_ID")
    while pid > 1 and pid not in seen:
        seen.add(pid)
        data = read_json(claude_sessions() / f"{pid}.json")
        if isinstance(data, dict) and data.get("sessionId") and data.get("pid") == pid and live_local_session(data):
            return {"agent": "claude", "session_id": data["sessionId"], "name": data.get("name"),
                    "host": socket.gethostname(), "pid": pid}
        if codex_session and codex_process(pid):
            return {"agent": "codex", "session_id": codex_session, "thread_id": os.environ.get("CODEX_THREAD_ID"),
                    "host": socket.gethostname(), "pid": pid}
        fields = proc_stat(pid)
        pid = int(fields[1]) if fields else 0
    return None


def claude_socket(target: dict) -> str | None:
    """Inbox socket of the live Claude Code session on this host with the target's session id, else its name."""
    by_name = []
    for path in claude_sessions().glob("*.json"):
        data = read_json(path)
        if not isinstance(data, dict):
            continue
        sock = data.get("messagingSocketPath")
        if not sock or not os.path.exists(sock) or not live_local_session(data):
            continue
        if target.get("session_id") and data.get("sessionId") == target["session_id"]:
            return sock
        if target.get("name") and data.get("name") == target["name"]:
            by_name.append((data.get("updatedAt") or 0, sock))
    return max(by_name)[1] if by_name else None


def post_to_claude(sock: str, text: str) -> str:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(5)
        connection.connect(sock)
        message = {"type": "user", "message": {"role": "user", "content": text}}
        connection.sendall((json.dumps(message) + "\n").encode())
        connection.shutdown(socket.SHUT_WR)
        try:
            return connection.recv(4096).decode(errors="replace").strip()
        except socket.timeout:
            return ""


def read_exactly(stream, size: int) -> bytes:
    data = stream.read(size)
    if len(data) < size:
        raise OSError("the Codex app-server closed the connection")
    return data


def apply_mask(data: bytes, key: bytes) -> bytes:
    return bytes(byte ^ key[index % 4] for index, byte in enumerate(data))


def websocket_send(connection: socket.socket, text: str, masked: bool = True) -> None:
    """Send one WebSocket text frame (clients mask their frames, servers do not)."""
    data = text.encode()
    size = len(data)
    length = bytes([size]) if size < 126 else bytes([126]) + size.to_bytes(2, "big") if size < 65536 \
        else bytes([127]) + size.to_bytes(8, "big")
    key = os.urandom(4) if masked else b""
    connection.sendall(bytes([0x81, length[0] | (0x80 if masked else 0)]) + length[1:] + key
                       + (apply_mask(data, key) if masked else data))


def websocket_receive(stream) -> str:
    """Read one WebSocket text message, skipping control frames."""
    parts = []
    while True:
        first, second = read_exactly(stream, 2)
        size = second & 0x7F
        if size >= 126:
            size = int.from_bytes(read_exactly(stream, 2 if size == 126 else 8), "big")
        key = read_exactly(stream, 4) if second & 0x80 else b""
        data = read_exactly(stream, size)
        if first & 0x0F == 8:
            raise OSError("the Codex app-server closed the connection")
        if first & 0x0F < 8:
            parts.append(apply_mask(data, key) if key else data)
            if first & 0x80:
                return b"".join(parts).decode()


@contextmanager
def codex_app_server(timeout: float = 10.0):
    """JSON-RPC to this host's Codex app-server daemon, which speaks WebSocket on its Unix control socket. Yields
    request(method, params), which returns the result or raises OSError."""
    path = os.path.realpath(codex_home() / "app-server-control" / "app-server-control.sock")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(timeout)
        try:
            connection.connect(path)
        except OSError as exc:
            raise OSError(f"no Codex app-server daemon is running on {socket.gethostname()} ({exc})") from exc
        key = base64.b64encode(os.urandom(16)).decode()
        connection.sendall(f"GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                           f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n".encode())
        with connection.makefile("rb") as stream:
            head = [stream.readline()]
            while head[-1] not in (b"\r\n", b""):
                head.append(stream.readline())
            accept = base64.b64encode(hashlib.sha1((key + WEBSOCKET_GUID).encode()).digest())
            if b" 101 " not in head[0] or accept not in b"".join(head):
                raise OSError(f"the Codex app-server refused the WebSocket upgrade: {head[0]!r}")
            ids = itertools.count(1)

            def request(method: str, params: dict) -> dict:
                request_id = next(ids)
                websocket_send(connection, json.dumps({"id": request_id, "method": method, "params": params}))
                while True:
                    try:
                        reply = json.loads(websocket_receive(stream))
                    except ValueError as exc:
                        raise OSError(f"unreadable reply from the Codex app-server: {exc}") from exc
                    if reply.get("id") != request_id or "method" in reply:
                        continue
                    error = reply.get("error")
                    if error:
                        detail = error.get("message", error) if isinstance(error, dict) else error
                        raise OSError(f"{method} failed: {detail}")
                    return reply.get("result") or {}

            request("initialize", {"clientInfo": {"name": "lanemsg", "version": "1"},
                                   "capabilities": {"experimentalApi": True}})
            websocket_send(connection, json.dumps({"method": "initialized"}))
            yield request


def post_to_codex(target: dict, text: str, message_id: str) -> str:
    """Steer the message into the Codex session's running turn. When no turn is running, queue it as the session's
    next turn, which the app-server starts at once."""
    thread, entry = target["session_id"], [{"type": "text", "text": text}]
    with codex_app_server() as request:
        status = ((request("thread/read", {"threadId": thread}).get("thread") or {}).get("status") or {}).get("type")
        if status == "active":
            turns = request("thread/turns/list", {"threadId": thread, "limit": 1}).get("data") or [{}]
            if turns[0].get("status") == "inProgress":
                try:
                    request("turn/steer", {"threadId": thread, "expectedTurnId": turns[0].get("id"), "input": entry,
                                           "clientUserMessageId": message_id})
                    return f"steered into the running turn of Codex session {thread}"
                except OSError:
                    pass
        elif status != "idle":
            raise OSError(f"Codex session {thread} is {status or 'unknown'} on {socket.gethostname()}")
        request("thread/queue/add", {"threadId": thread, "input": entry, "clientUserMessageId": message_id})
        return f"queued as the next turn of Codex session {thread}"


def post_to_orchestrator(target: dict, text: str, message_id: str) -> str:
    """Deliver one message to the orchestrator's session, which must run on this host."""
    if target.get("agent") == "codex":
        return post_to_codex(target, text, message_id)
    sock = claude_socket(target)
    if not sock:
        raise OSError(f"no live Claude session on {socket.gethostname()} is registered as this lane's orchestrator")
    return post_to_claude(sock, text)


def relay_push(mailbox: Mailbox, host: str, min_age_s: float) -> list:
    """Run the push on the orchestrator's host, the only host from which its session can be reached."""
    remote = shlex.join([sys.executable, "-I", str(SELF), "push", "--min-age", str(min_age_s),
                         str(mailbox.path.resolve())])
    try:
        result = subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", host, remote],
                                stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=60)
        if result.returncode:
            raise OSError(result.stderr.strip()[-300:] or f"ssh exited with status {result.returncode}")
        return json.loads(result.stdout.strip().splitlines()[-1])
    except (OSError, subprocess.TimeoutExpired, ValueError, IndexError) as exc:
        raise OSError(f"relay to {host} failed: {exc}") from exc


def parse_target(value: str) -> dict:
    """An orchestrator named by hand: `[claude:|codex:]<session id or Claude session name>[@<host>]`."""
    agent, _, rest = value.partition(":") if value.startswith(("claude:", "codex:")) else ("claude", "", value)
    reference, _, host = rest.partition("@")
    key = "session_id" if agent == "codex" or (len(reference) == 36 and reference.count("-") == 4) else "name"
    return {"agent": agent, key: reference, **({"host": host} if host else {})}


def describe(sender: dict) -> str:
    if sender.get("agent") in ("claude", "codex"):
        return f"orchestrator {sender.get('name') or sender['agent'] + ' session ' + str(sender.get('session_id'))}"
    if sender.get("agent") == "lane":
        return f"lane {sender.get('lane')} (attempt {sender.get('attempt')})"
    if sender.get("agent") == "file":
        return f"orchestrator (file {sender.get('file')})"
    return "orchestrator"


class Mailbox:
    def __init__(self, path):
        self.path = Path(path)
        self._depth = 0

    def dir(self, queue: str, state: str) -> Path:
        return self.path / queue / state

    @property
    def lane(self) -> dict:
        return read_json(self.path / "lane.json", {})

    def create(self, name: str, run_root: Path) -> Mailbox:
        for queue in ("inbox", "outbox"):
            for state in ("tmp", "new", "cur", "done"):
                self.dir(queue, state).mkdir(parents=True, exist_ok=True)
        (self.path / "attempts").mkdir(exist_ok=True)
        lane_dir = self.path.parent.resolve()
        key = lane_key(lane_dir)
        info = {"key": key, "hash": hashlib.sha1(key.encode()).hexdigest()[:8], "name": name,
                "lane_dir": str(lane_dir), "run_root": str(Path(run_root).resolve()), "mail": str(self.path.resolve())}
        if self.lane != info:
            write_atomic(self.path / "lane.json", json.dumps(info, indent=2) + "\n")
        return self

    def log(self, event: str, **fields) -> None:
        line = json.dumps({"t": utc_now(), "event": event, **fields}) + "\n"
        descriptor = os.open(self.path / "events.jsonl", os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
        try:
            os.write(descriptor, line.encode())
        finally:
            os.close(descriptor)

    @contextmanager
    def locked(self):
        if self._depth:
            self._depth += 1
            try:
                yield
            finally:
                self._depth -= 1
            return
        with open(self.path / ".lock", "a") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            self._depth = 1
            try:
                yield
            finally:
                self._depth = 0

    def state(self) -> dict:
        return read_json(self.path / "state.json", {})

    def update_state(self, change):
        with self.locked():
            state = self.state()
            result = change(state)
            write_atomic(self.path / "state.json", json.dumps(state, indent=1) + "\n")
            return result

    def write(self, queue: str, kind: str, body: str, sender: dict, reply_to: str | None = None,
              quote: str | None = None) -> dict:
        lane = self.lane
        stamp = datetime.now(timezone.utc)
        message = {"id": f"{lane['hash']}-{stamp:%Y%m%dT%H%M%S}-{os.urandom(3).hex()}", "kind": kind,
                   "queue": queue, "lane": lane["key"], "sender": sender, "body": body, "reply_to": reply_to,
                   "quote": quote, "created": stamp.strftime("%Y-%m-%d %H:%M:%SZ"), "created_ts": stamp.timestamp()}
        temporary = self.dir(queue, "tmp") / f"{message['id']}.json"
        with open(temporary, "w") as stream:
            stream.write(json.dumps(message, indent=1) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.rename(temporary, self.dir(queue, "new") / temporary.name)
        self.log("sent", id=message["id"], queue=queue, kind=kind, reply_to=reply_to, sender=sender)
        return message

    def has(self, queue: str, state: str) -> bool:
        try:
            with os.scandir(self.dir(queue, state)) as entries:
                return any(entry.name.endswith(".json") for entry in entries)
        except FileNotFoundError:
            return False

    def list(self, queue: str, states=("new", "cur")) -> list:
        items = []
        for state in states:
            try:
                entries = list(os.scandir(self.dir(queue, state)))
            except FileNotFoundError:
                continue
            for entry in entries:
                message = read_json(entry.path) if entry.name.endswith(".json") else None
                if message:
                    items.append((state, message))
        return sorted(items, key=lambda item: item[1].get("created_ts", 0))

    def find(self, message_id: str):
        for queue in ("inbox", "outbox"):
            for state in ("new", "cur", "done"):
                path = self.dir(queue, state) / f"{message_id}.json"
                if path.exists():
                    message = read_json(path)
                    if message:
                        return queue, state, message
        return None

    def move(self, message_id: str, queue: str, to_state: str, from_states=("new", "cur")) -> bool:
        for state in from_states:
            if state == to_state:
                continue
            try:
                os.rename(self.dir(queue, state) / f"{message_id}.json", self.dir(queue, to_state) / f"{message_id}.json")
                return True
            except FileNotFoundError:
                continue
        return False

    def session_for(self, attempt) -> str | None:
        return (read_json(self.path / "attempts" / f"{attempt}.json") or {}).get("session_id")

    def reset_binding(self, attempt) -> None:
        """Set aside a binding left by an earlier run that reused this attempt number."""
        path = self.path / "attempts" / f"{attempt}.json"
        if path.exists():
            os.replace(path, path.with_name(f"{attempt}.stale-{int(time.time())}.json.old"))

    def reserve_attempt(self) -> int:
        """Claim the next unused attempt number; O_EXCL gives concurrent launches different numbers."""
        names = (re.fullmatch(r"(\d+)\.(json|launch)", path.name) for path in (self.path / "attempts").iterdir())
        attempt = max((int(match.group(1)) for match in names if match), default=0) + 1
        while True:
            try:
                os.close(os.open(self.path / "attempts" / f"{attempt}.launch", os.O_CREAT | os.O_EXCL | os.O_WRONLY))
                return attempt
            except FileExistsError:
                attempt += 1

    def bind(self, attempt, session_id: str) -> bool:
        """Bind the attempt to the first session that reports in; True if this session owns the attempt."""
        if not session_id:
            return False
        path = self.path / "attempts" / f"{attempt}.json"
        if not path.exists():
            temporary = path.with_name(f".{attempt}.{os.getpid()}.tmp")
            temporary.write_text(json.dumps({"session_id": session_id, "bound": utc_now(),
                                             "devin_pid": os.getppid()}) + "\n")
            try:
                os.link(temporary, path)
            except FileExistsError:
                pass
            else:
                self.log("session_bound", attempt=str(attempt), session=session_id)
            finally:
                temporary.unlink()
        return self.session_for(attempt) == session_id

    def orchestrator(self) -> dict | None:
        return read_json(self.path / "orchestrator.json")

    def set_orchestrator(self, target: dict, how: str) -> None:
        target = {key: target[key] for key in ("agent", "session_id", "name", "host") if target.get(key)}
        current = self.orchestrator() or {}
        if all(current.get(key) == value for key, value in target.items()):
            return
        write_atomic(self.path / "orchestrator.json", json.dumps({**target, "set_by": how, "set_at": utc_now()}) + "\n")
        self.log("orchestrator_set", how=how, **target)

    def deliver(self, attempt, session_id: str | None, how: str) -> list:
        """Claim the lane's undelivered messages for this session, oldest first."""
        delivered = [message for _, message in self.list("inbox", ("new",))
                     if self.move(message["id"], "inbox", "done" if message["kind"] == "reply" else "cur", ("new",))]
        if delivered:
            def record(state):
                for message in delivered:
                    state.setdefault("delivered", {})[message["id"]] = {
                        "session": session_id, "attempt": str(attempt), "at": utc_now(), "how": how}
            self.update_state(record)
            for message in delivered:
                self.log("delivered", id=message["id"], kind=message["kind"], session=session_id,
                         attempt=str(attempt), how=how)
        return delivered

    def open_for_lane(self) -> list:
        return [message for _, message in self.list("inbox", ("cur",))]

    def open_questions(self) -> list:
        return [message for _, message in self.list("outbox") if message["kind"] == "ask"]


def push_pending(mailbox: Mailbox, min_age_s: float = 0.0, relay: bool = True) -> list:
    """Post the lane's undelivered messages to its orchestrator's session; returns the ids it posted. When the
    orchestrator runs on another host, the push runs there over ssh. The first failed push of a message also
    leaves a TO_ORCHESTRATOR_*.md copy in the lane directory, where file-based lane watchers look."""
    if not mailbox.has("outbox", "new"):
        return []
    target = mailbox.orchestrator()
    host, relay_error = (target or {}).get("host"), None
    if relay and host and host != socket.gethostname():
        try:
            return relay_push(mailbox, host, min_age_s)
        except OSError as exc:
            relay_error = exc
    pushed = []
    with mailbox.locked():
        state = mailbox.state()
        failures = state.setdefault("push_failures", {})
        before = dict(failures)
        for _, message in mailbox.list("outbox", ("new",)):
            if time.time() - message.get("created_ts", 0) < min_age_s:
                continue
            try:
                if relay_error or not target:
                    raise relay_error or OSError("no orchestrator session is registered for this lane")
                response = post_to_orchestrator(target, format_for_orchestrator(message, mailbox), message["id"])
            except OSError as exc:
                if message["id"] not in failures:
                    copy = mailbox.path.parent / f"TO_ORCHESTRATOR_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}_" \
                                                 f"{message['id']}.md"
                    write_atomic(copy, format_for_orchestrator(message, mailbox) + "\n")
                if failures.get(message["id"]) != str(exc):
                    failures[message["id"]] = str(exc)
                    mailbox.log("push_failed", id=message["id"], error=str(exc), target=target)
                continue
            mailbox.move(message["id"], "outbox", "cur" if message["kind"] == "ask" else "done", ("new",))
            failures.pop(message["id"], None)
            mailbox.log("pushed", id=message["id"], target=target, response=response[:300])
            pushed.append(message["id"])
        if failures != before:
            write_atomic(mailbox.path / "state.json", json.dumps(state, indent=1) + "\n")
    return pushed


def format_for_lane(messages: list, header: str) -> str:
    lines = [header]
    for message in messages:
        lines.append(f"--- {message['id']} | {message['kind'].upper()} from {describe(message['sender'])} "
                     f"| {message['created']}")
        if message.get("reply_to"):
            lines.append(f"(answer to your message {message['reply_to']}: \"{message.get('quote') or ''}\")")
        lines.append(clip(message["body"]))
        if message["kind"] == "ask":
            lines.append(f">>> Answer it: {command()} reply {message['id']} \"<your answer>\"")
        elif message["kind"] == "note":
            lines.append(f">>> When handled: {command()} ack {message['id']}")
    return "\n".join(lines)


def format_for_orchestrator(message: dict, mailbox: Mailbox) -> str:
    lane, sender = mailbox.lane, message["sender"]
    title = {"ask": "QUESTION", "note": "NOTE", "reply": "ANSWER"}[message["kind"]]
    where = f"lane {lane['key']} (attempt {sender.get('attempt')}, {sender.get('engine', 'devin')} session " \
            f"{sender.get('session_id') or 'unknown'})"
    lines = [f"[lanemsg] {title} from {where} | msg {message['id']}"]
    if message.get("reply_to"):
        lines.append(f"(answer to your message {message['reply_to']}: \"{message.get('quote') or ''}\")")
    lines.append(clip(message["body"]))
    if message["kind"] == "ask":
        lines.append(f">>> The lane is waiting for your answer: {command()} reply {message['id']} \"<answer>\"")
    else:
        lines.append(f">>> No answer needed. To write to the lane: {command()} send {lane['key']} \"<text>\"")
    return "\n".join(lines)


def identity_block(mailbox: Mailbox, attempt) -> str:
    cli = command()
    lines = [f"[lanemsg] You are lane `{mailbox.lane['key']}` (attempt {attempt}). The orchestrator can message you "
             "while you work; its messages appear in your context as [lanemsg] blocks. Commands:",
             f"  {cli} reply <id> \"<text>\"   answer a message (questions need an answer)",
             f"  {cli} ack <id>   mark a note handled",
             f"  {cli} send \"<text>\"   tell the orchestrator something it must know now (a final result, a "
             "blocker, a decision it has to make); keep routine progress out of messages",
             f"  {cli} ask \"<text>\"   ask the orchestrator; the answer arrives here later"]
    if os.environ.get("LANEMSG_ENGINE", "devin") == "devin":
        lines.append("If you cannot continue without an answer, record your progress and end your turn: whoever "
                     "launched the lane resumes this same session when the answer arrives. Do not poll for answers.")
    else:
        lines.append(f"Check for messages between work steps with `{cli} inbox`. Do not poll in a loop.")
    return "\n".join(lines)


def open_items_block(mailbox: Mailbox, session_id: str | None) -> str:
    """Open messages this session has not seen yet (all of them when session_id is None), plus the lane's
    unanswered questions. Shown messages count as delivered to the session."""
    delivered = mailbox.state().get("delivered", {})
    earlier = [message for message in mailbox.open_for_lane()
               if session_id is None or delivered.get(message["id"], {}).get("session") != session_id]
    parts = []
    if earlier:
        parts.append(format_for_lane(earlier, "[lanemsg] Still open from earlier sessions (answer or ack each one "
                                              "you have already handled):"))
        if session_id:
            def record(state):
                for message in earlier:
                    state.setdefault("delivered", {}).setdefault(message["id"], {})["session"] = session_id
            mailbox.update_state(record)
    questions = mailbox.open_questions()
    if questions:
        parts.append("[lanemsg] Your questions still waiting for the orchestrator: " + "; ".join(
            f"{question['id']}: {clip(question['body'], 300)}" for question in questions))
    return "\n".join(parts)


def subagent_hold(call: dict) -> float:
    return BACKGROUND_SUBAGENT_HOLD_S if call["background"] else FOREGROUND_SUBAGENT_HOLD_S


def subagent_running(state: dict, session_id: str) -> bool:
    """Devin subagents report hook events under the parent's session and prompt ids, so the hooks hold
    delivery while a run_subagent call is open (for a bounded time, in case its end is never reported)."""
    calls = state.get("subagents", {}).get(session_id, {})
    return any(time.time() - call["since"] < subagent_hold(call) for call in calls.values())


def track_subagent(mailbox: Mailbox, event: str, session_id: str, payload: dict) -> None:
    tool, tool_use_id = payload.get("tool_name"), payload.get("tool_use_id") or "unknown"
    tool_input = payload.get("tool_input") or {}
    output = str((payload.get("tool_response") or {}).get("output") or "")

    def change(state):
        calls, now = state.setdefault("subagents", {}).setdefault(session_id, {}), time.time()
        if event == "PreToolUse" and tool == SUBAGENT_TOOL:
            calls[tool_use_id] = {"background": False, "since": now}
        elif event == "PostToolUse" and tool == SUBAGENT_TOOL:
            calls.pop(tool_use_id, None)
            started = re.search(r"agent_id=(\w+)", output)
            if tool_input.get("is_background") and started and "completed" not in output:
                calls[f"agent:{started.group(1)}"] = {"background": True, "since": now}
        elif event == "PostToolUse" and tool_input.get("agent_id"):
            agent = str(tool_input["agent_id"])
            if re.search(rf"Subagent {re.escape(agent)}\b.*\b(completed|failed|cancel)", output):
                calls.pop(f"agent:{agent}", None)
        for key in [key for key, call in calls.items() if now - call["since"] >= subagent_hold(call)]:
            calls.pop(key)
    mailbox.update_state(change)


def run_hook(event: str, payload: dict) -> dict | None:
    mail_dir, attempt = os.environ.get("LANEMSG_DIR"), os.environ.get("LANEMSG_ATTEMPT")
    if not mail_dir or not attempt or not (Path(mail_dir) / "lane.json").exists():
        return None
    mailbox = Mailbox(Path(mail_dir))
    session_id = payload.get("session_id") or ""
    subagent_call = payload.get("tool_name") in SUBAGENT_TOOLS
    import_file_notes(mailbox)
    if event == "PostToolUse" and not subagent_call and not mailbox.has("inbox", "new") \
            and not mailbox.state().get("reinject", {}).get(session_id):
        return None
    if not mailbox.bind(attempt, session_id):
        return None
    if event in ("PreToolUse", "PostToolUse") and subagent_call:
        track_subagent(mailbox, event, session_id, payload)
        if event == "PreToolUse":
            return None
    elif event in ("PostToolUse", "Stop") and subagent_running(mailbox.state(), session_id):
        return None
    if event == "PostCompaction":
        mailbox.update_state(lambda state: state.setdefault("reinject", {}).__setitem__(session_id, True))
        return None
    if event == "Stop":
        return stop_decision(mailbox, attempt, session_id)
    if event == "SessionStart":
        mailbox.update_state(lambda state: state.get("subagents", {}).pop(session_id, None))
        parts = [identity_block(mailbox, attempt), open_items_block(mailbox, session_id)]
    elif event in ("UserPromptSubmit", "PostToolUse"):
        reinject = mailbox.update_state(lambda state: state.get("reinject", {}).pop(session_id, None))
        parts = [identity_block(mailbox, attempt), open_items_block(mailbox, session_id)] if reinject else []
    else:
        return None
    new = mailbox.deliver(attempt, session_id, event)
    if new:
        parts.append(format_for_lane(new, "[lanemsg] New message(s) from the orchestrator:"))
    text = "\n".join(part for part in parts if part)
    return {"hookSpecificOutput": {"hookEventName": event, "additionalContext": text}} if text else None


def stop_decision(mailbox: Mailbox, attempt, session_id: str) -> dict | None:
    new = mailbox.deliver(attempt, session_id, "Stop")
    if new:
        return {"decision": "block", "reason": format_for_lane(new, "[lanemsg] New message(s) from the orchestrator "
                                                                     "arrived; handle them before you stop:")}
    unanswered = [message for message in mailbox.open_for_lane() if message["kind"] == "ask"]
    if not unanswered:
        return None

    def remind(state):
        counts, due = state.setdefault("stop_reminders", {}), []
        for message in unanswered:
            if counts.get(message["id"], 0) < MAX_STOP_REMINDERS:
                counts[message["id"]] = counts.get(message["id"], 0) + 1
                due.append(message)
        return due
    due = mailbox.update_state(remind)
    if not due:
        return None
    return {"decision": "block", "reason": format_for_lane(due, "[lanemsg] The orchestrator is waiting for your "
                                                                "answer; reply before you stop:")}


def hook_main(event: str) -> int:
    try:
        raw = sys.stdin.read()
        output = run_hook(event, json.loads(raw) if raw.strip() else {})
        if output:
            print(json.dumps(output))
    except Exception:
        try:
            with open(Path(os.environ["LANEMSG_DIR"]) / "hook_errors.log", "a") as stream:
                stream.write(f"{utc_now()} {event}\n{traceback.format_exc()}\n")
        except Exception:
            pass
    return 0


def import_file_notes(mailbox: Mailbox) -> None:
    """Turn ORCHESTRATOR_NOTE_*.md files (the file channel) into inbox notes. On the first scan of a mailbox,
    notes already acknowledged in HEARTBEAT (the file channel's receipt) are recorded but not sent again."""
    lane_dir = mailbox.path.parent
    try:
        mtime = lane_dir.stat().st_mtime
    except OSError:
        return
    state = mailbox.state()
    if state.get("notes_scanned_mtime") == mtime and state.get("notes_scanned_at", 0) > mtime + 2:
        return
    with mailbox.locked():
        state = mailbox.state()
        acked = ""
        if "imported_notes" not in state:
            acked = "".join(path.read_text(errors="ignore") for path in (lane_dir / "HEARTBEAT.md", lane_dir / "HEARTBEAT.log")
                            if path.exists())
        seen = state.setdefault("imported_notes", {})
        for path in sorted(lane_dir.glob("ORCHESTRATOR_NOTE_*.md")):
            if path.name in seen:
                continue
            if f"ACK {path.name}" in acked or f"ACK {path.stem}" in acked:
                seen[path.name] = "acknowledged before the mailbox existed"
            else:
                seen[path.name] = mailbox.write("inbox", "note", path.read_text().strip() or path.name,
                                                {"agent": "file", "file": path.name})["id"]
        state.update(notes_scanned_mtime=mtime, notes_scanned_at=time.time())
        write_atomic(mailbox.path / "state.json", json.dumps(state, indent=1) + "\n")


def file_channel_ack(mailbox: Mailbox, message: dict) -> None:
    """Acknowledge an imported note the way the file channel expects: an ACK line in HEARTBEAT.md."""
    if message.get("sender", {}).get("agent") == "file":
        with open(mailbox.path.parent / "HEARTBEAT.md", "a") as stream:
            stream.write(f"{utc_now()} | ACK {message['sender']['file']} {utc_now()}\n")


def register_lane(lane_dir: Path, name: str, run_root: Path, orchestrator: str = "") -> Mailbox:
    mailbox = Mailbox(Path(lane_dir) / "mail").create(name, run_root)
    import_file_notes(mailbox)
    lane = mailbox.lane
    entry = registry() / f"{lane['hash']}.json"
    if read_json(entry) != lane:
        entry.parent.mkdir(parents=True, exist_ok=True)
        write_atomic(entry, json.dumps(lane, indent=2) + "\n")
    if orchestrator and not mailbox.orchestrator():
        mailbox.set_orchestrator(parse_target(orchestrator), "lane registration")
    return mailbox


def registered_lanes() -> list:
    lanes = (read_json(path) for path in registry().glob("*.json"))
    return sorted((lane for lane in lanes if isinstance(lane, dict) and lane.get("key")), key=lambda lane: lane["key"])


def lane_status(lane: dict) -> str:
    lane_dir = Path(lane["lane_dir"])
    if (lane_dir / "DONE.json").exists():
        return "done"
    if (lane_dir / "FAILURE_FLAG.json").exists():
        return "failed"
    state = read_json(Path(lane["run_root"]) / "_controller" / "STATE.json") or {}
    return state.get("lanes", {}).get(lane["name"], {}).get("status", "unknown")


def resolve_lane(reference: str) -> Mailbox:
    path = Path(reference)
    if path.is_absolute() and (path / "mail" / "lane.json").exists():
        return Mailbox(path / "mail")
    lanes = registered_lanes()
    matches = [lane for lane in lanes if reference in (lane["key"], lane["hash"])] or [
        lane for lane in lanes if lane["key"].endswith("/" + reference.strip("/")) or lane["name"] == reference]
    if len(matches) > 1:
        live = [lane for lane in matches if lane_status(lane) not in ("done", "failed")]
        matches = live if len(live) == 1 else matches
    if not matches:
        raise LaneError(f"no registered lane matches {reference!r}; list them with `{command()} lanes --all`")
    if len(matches) > 1:
        raise LaneError(f"{reference!r} matches several lanes: " + ", ".join(lane["key"] for lane in matches))
    return Mailbox(Path(matches[0]["mail"]))


def mailbox_for_message(message_id: str) -> Mailbox:
    lane = read_json(registry() / f"{message_id.split('-', 1)[0]}.json")
    if not lane:
        raise LaneError(f"no registered lane owns message {message_id}")
    return Mailbox(Path(lane["mail"]))


def lane_context():
    mail_dir = os.environ.get("LANEMSG_DIR")
    return (Mailbox(Path(mail_dir)), os.environ.get("LANEMSG_ATTEMPT", "?")) if mail_dir else None


def lane_sender(mailbox: Mailbox, attempt) -> dict:
    return {"agent": "lane", "lane": mailbox.lane["key"], "attempt": attempt,
            "engine": os.environ.get("LANEMSG_ENGINE", "devin"), "session_id": mailbox.session_for(attempt)}


def orchestrator_sender(mailbox: Mailbox) -> dict:
    session = orchestrator_ancestor()
    if session:
        mailbox.set_orchestrator(session, "orchestrator wrote to the lane")
        return session
    return {"agent": "other", "host": socket.gethostname(), "pid": os.getppid()}


def body_from(words: list) -> str:
    text = sys.stdin.read() if words == ["-"] else " ".join(words)
    if not text.strip():
        raise LaneError("the message text is empty")
    return text.strip()


def lane_delivery_hint(mailbox: Mailbox) -> str:
    status = lane_status(mailbox.lane)
    return {"running": "it reaches the lane at its next tool call",
            "parked": "the lane reads it when it is resumed",
            "waiting": "the lane reads it when it starts",
            "done": "WARNING: the lane is finished and will not read it",
            "failed": "WARNING: the lane has failed and will not read it"}.get(
        status, "it reaches the lane at its next tool call, or when the lane next starts or resumes")


def push_report(mailbox: Mailbox, message: dict) -> str:
    """Whether the message reached the orchestrator. The push's own result comes first: after a push relayed to
    another host, this host's NFS cache can still list the moved message as new."""
    if message["id"] in push_pending(mailbox) or (mailbox.find(message["id"]) or ("", "?", None))[1] != "new":
        return "delivered to the orchestrator's session"
    return "queued: no live orchestrator session took it yet; it is retried with your next message and " \
           "delivered when the orchestrator adopts this lane"


def cmd_send(args, kind: str) -> str:
    context = lane_context() if not args.orchestrator else None
    if context:
        mailbox, attempt = context
        message = mailbox.write("outbox", kind, body_from(args.words), lane_sender(mailbox, attempt))
        return f"sent {kind} {message['id']}; {push_report(mailbox, message)}"
    if len(args.words) < 2:
        raise LaneError(f"usage: {command()} {kind} <lane> <text>")
    mailbox = resolve_lane(args.words[0])
    message = mailbox.write("inbox", kind, body_from(args.words[1:]), orchestrator_sender(mailbox))
    return f"sent {kind} {message['id']} to lane {mailbox.lane['key']}; {lane_delivery_hint(mailbox)}"


def cmd_reply(args) -> str:
    context = lane_context() if not args.orchestrator else None
    mailbox, attempt = context if context else (mailbox_for_message(args.id), None)
    queue = "inbox" if context else "outbox"
    found = mailbox.find(args.id)
    if not found or found[0] != queue:
        raise LaneError(f"message {args.id} is not addressed to you")
    if found[1] == "done" and found[2]["kind"] == "ask":
        raise LaneError(f"{args.id} was already answered; use `send` for a follow-up")
    original = found[2]
    if context:
        sender = lane_sender(mailbox, attempt)
    else:
        sender = orchestrator_sender(mailbox)
    reply_queue = "outbox" if context else "inbox"
    message = mailbox.write(reply_queue, "reply", body_from(args.words), sender, reply_to=args.id,
                            quote=clip(original["body"], 300))
    mailbox.move(args.id, queue, "done")
    mailbox.log("answered", id=args.id, reply=message["id"])
    if context:
        file_channel_ack(mailbox, original)
    if context:
        return f"replied {message['id']} to {args.id}; {push_report(mailbox, message)}"
    return f"replied {message['id']} to {args.id}; {lane_delivery_hint(mailbox)}"


def cmd_ack(args) -> str:
    context = lane_context() if not args.orchestrator else None
    mailbox = context[0] if context else mailbox_for_message(args.id)
    queue = "inbox" if context else "outbox"
    found = mailbox.find(args.id)
    if not found or found[0] != queue:
        raise LaneError(f"message {args.id} is not addressed to you")
    if context and found[2]["kind"] == "ask":
        raise LaneError(f"{args.id} is a question; answer it with `{command()} reply {args.id} \"<text>\"`")
    if not mailbox.move(args.id, queue, "done"):
        return f"{args.id} was already closed"
    mailbox.log("acked", id=args.id, note=" ".join(args.words) or None)
    if context:
        file_channel_ack(mailbox, found[2])
    return f"closed {args.id}"


def cmd_inbox(args) -> str:
    context = lane_context() if not args.orchestrator else None
    if context:
        mailbox, attempt = context
        import_file_notes(mailbox)
        still_open = open_items_block(mailbox, None)
        new = [] if args.peek else mailbox.deliver(attempt, mailbox.session_for(attempt), "inbox command")
        parts = [format_for_lane(new, "[lanemsg] New message(s) from the orchestrator:") if new else "", still_open]
        return "\n".join(part for part in parts if part) or "no open messages"
    mailboxes = [resolve_lane(args.lane)] if args.lane else [Mailbox(Path(lane["mail"])) for lane in registered_lanes()]
    out = []
    for mailbox in mailboxes:
        if not mailbox.lane:
            continue
        states = ("new", "cur", "done") if args.all else ("new", "cur")
        for state, message in mailbox.list("outbox", states):
            out.append(f"[{state}] " + format_for_orchestrator(message, mailbox))
            if state == "new" and not args.peek:
                mailbox.move(message["id"], "outbox", "cur" if message["kind"] == "ask" else "done", ("new",))
                mailbox.log("read", id=message["id"], how="inbox command")
    return "\n\n".join(out) or "no open messages from lanes"


def cmd_show(args) -> str:
    context = lane_context() if not args.orchestrator else None
    mailbox = context[0] if context else mailbox_for_message(args.id)
    found = mailbox.find(args.id)
    if not found:
        raise LaneError(f"message {args.id} not found")
    return json.dumps({"queue": found[0], "state": found[1], **found[2]}, indent=1)


def summarize(mailbox: Mailbox) -> dict:
    lane = mailbox.lane
    counts = {f"{queue}_{state}": len(mailbox.list(queue, (state,))) for queue in ("inbox", "outbox")
              for state in ("new", "cur")}
    attempts = sorted((path.stem, (read_json(path) or {}).get("session_id"))
                      for path in (mailbox.path / "attempts").glob("*.json"))
    return {"lane": lane["key"], "status": lane_status(lane), "orchestrator": mailbox.orchestrator(),
            **counts, "sessions": dict(attempts),
            "open_questions": [question["id"] for question in mailbox.open_questions()]}


def cmd_status(args) -> str:
    context = lane_context() if not args.orchestrator else None
    if context:
        return json.dumps(summarize(context[0]), indent=1)
    if args.lane:
        mailbox = resolve_lane(args.lane)
        events = (mailbox.path / "events.jsonl")
        tail = events.read_text().splitlines()[-8:] if events.exists() else []
        return json.dumps(summarize(mailbox), indent=1) + "\nrecent events:\n" + "\n".join(tail)
    return cmd_lanes(argparse.Namespace(all=False))


def cmd_lanes(args) -> str:
    rows = []
    for lane in registered_lanes():
        status = lane_status(lane)
        if not args.all and status in ("done", "failed"):
            continue
        mailbox = Mailbox(Path(lane["mail"]))
        rows.append(f"{status:8} {lane['key']}  to-lane new/open {len(mailbox.list('inbox', ('new',)))}/"
                    f"{len(mailbox.list('inbox', ('cur',)))}  from-lane new/open {len(mailbox.list('outbox', ('new',)))}/"
                    f"{len(mailbox.list('outbox', ('cur',)))}")
    return "\n".join(rows) or ("no registered lanes" if args.all else "no live lanes (use --all to include finished ones)")


def hooks_config() -> dict:
    """Generic Devin hooks: they run only in processes whose environment names a lane (set by its launcher)."""
    def entry(event: str, matcher: str = "") -> list:
        script = f'test -n "$LANEMSG_HOOK" || exit 0; exec "${{LANEMSG_PYTHON:-python3}}" -I "$LANEMSG_HOOK" hook {event}'
        return [{"matcher": matcher, "hooks": [{"type": "command", "command": f"sh -c {shlex.quote(script)}",
                                                "timeout": 30}]}]
    config = {event: entry(event) for event in ("SessionStart", "UserPromptSubmit", "PostToolUse", "Stop",
                                                "PostCompaction")}
    config["PreToolUse"] = entry("PreToolUse", f"^{SUBAGENT_TOOL}$")
    return config


def cmd_install_hooks(args) -> str:
    """Merge the hooks into <project>/.devin/hooks.v1.json, or into the user's Devin config (all projects)."""
    path = Path(args.project).resolve() / ".devin" / "hooks.v1.json" if args.project else devin_user_config()
    config = read_json(path) if path.exists() else {}
    hooks = config if args.project else config.get("hooks", {}) if isinstance(config, dict) else None
    if not isinstance(config, dict) or not isinstance(hooks, dict):
        raise LaneError(f"{path} does not hold a JSON object of hooks; fix it by hand first")
    merged = dict(hooks)
    for event, entries in hooks_config().items():
        merged[event] = [item for item in merged.get(event, []) if "LANEMSG_HOOK" not in json.dumps(item)] + entries
    path.parent.mkdir(parents=True, exist_ok=True)
    write_atomic(path, json.dumps(merged if args.project else {**config, "hooks": merged}, indent=2) + "\n")
    return f"installed lanemsg hooks in {path}"


def cmd_adopt(args) -> str:
    session = orchestrator_ancestor()
    if not session:
        raise LaneError("adopt must run inside the Claude Code or Codex session that will receive the lanes' messages")
    mailboxes = [resolve_lane(reference) for reference in args.lanes]
    for mailbox in mailboxes:
        mailbox.set_orchestrator(session, "adopt")
        push_pending(mailbox)
    return f"{describe(session)} now receives messages from: " + ", ".join(mailbox.lane["key"] for mailbox in mailboxes)


def cmd_register(args) -> str:
    lane_dir = Path(args.lane_dir).resolve()
    lane_dir.mkdir(parents=True, exist_ok=True)
    mailbox = register_lane(lane_dir, args.name or lane_dir.name, Path(args.run_root or lane_dir.parent))
    session = parse_target(args.session) if args.session else orchestrator_ancestor()
    if session:
        mailbox.set_orchestrator(session, "register")
    key = shlex.quote(mailbox.lane["key"])
    receiver = describe(session) if session else f"nobody yet; run `{command()} adopt {key}` inside the orchestrator"
    return f"registered lane {key}; its messages go to {receiver}\n" \
           f"launch an attempt with: {command()} run {key} -- devin -p --prompt-file <brief>"


def cmd_run(args) -> str:
    """Replace this process with an attempt of the lane, its identity in the environment (never returns)."""
    argv = args.argv[1:] if args.argv[:1] == ["--"] else args.argv
    if not argv:
        raise LaneError(f"usage: {command()} run <lane> -- <command> [arguments]")
    mailbox, executable = resolve_lane(args.lane), shutil.which(argv[0])
    if not executable:
        raise LaneError(f"cannot run {argv[0]}: no such executable")
    attempt, engine = mailbox.reserve_attempt(), "codex" if Path(argv[0]).name == "codex" else "devin"
    mailbox.log("launched", attempt=str(attempt), engine=engine, host=socket.gethostname(), argv=argv)
    env = {**os.environ, "LANEMSG_DIR": str(mailbox.path.resolve()), "LANEMSG_ATTEMPT": str(attempt),
           "LANEMSG_HOOK": str(SELF), "LANEMSG_PYTHON": sys.executable, "LANEMSG_ENGINE": engine}
    try:
        os.execve(executable, argv, env)
    except OSError as exc:
        raise LaneError(f"cannot run {argv[0]}: {exc}") from exc


def cmd_push(args) -> str:
    mail = Path(args.mail)
    if not (mail / "lane.json").exists():
        raise LaneError(f"{mail} is not a lane mailbox")
    return json.dumps(push_pending(Mailbox(mail), args.min_age, relay=False))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="lanemsg", description=__doc__.split("\n\n")[0])
    parser.add_argument("--orchestrator", action="store_true",
                        help="act as the orchestrator even if LANEMSG_DIR is set")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("send", "ask"):
        sub = commands.add_parser(name, help=f"{name} a message (orchestrator: {name} <lane> <text>)")
        sub.add_argument("words", nargs="+")
    sub = commands.add_parser("reply", help="answer a message")
    sub.add_argument("id")
    sub.add_argument("words", nargs="+")
    sub = commands.add_parser("ack", help="close a message without answering it")
    sub.add_argument("id")
    sub.add_argument("words", nargs="*")
    sub = commands.add_parser("inbox", help="show open messages and mark new ones read")
    sub.add_argument("lane", nargs="?")
    sub.add_argument("--all", action="store_true")
    sub.add_argument("--peek", action="store_true", help="do not mark anything read")
    sub = commands.add_parser("show", help="print one message")
    sub.add_argument("id")
    sub = commands.add_parser("status", help="mailbox status for one lane, or all live lanes")
    sub.add_argument("lane", nargs="?")
    sub = commands.add_parser("lanes", help="list registered lanes")
    sub.add_argument("--all", action="store_true")
    sub = commands.add_parser("register", help="create a lane's mailbox; the calling session becomes its orchestrator")
    sub.add_argument("lane_dir")
    sub.add_argument("--name", help="short lane name (default: the directory's name)")
    sub.add_argument("--run-root", help="directory that holds the run's lanes (default: the lane directory's parent)")
    sub.add_argument("--session", help="orchestrator session, when register does not run inside it: "
                                       "[claude:|codex:]<session id or Claude session name>[@<host>]")
    sub = commands.add_parser("run", help="launch an attempt of a lane: run <lane> -- devin -p --prompt-file <brief>")
    sub.add_argument("lane")
    sub.add_argument("argv", nargs=argparse.REMAINDER)
    sub = commands.add_parser("adopt", help="make this Claude Code or Codex session the orchestrator of these lanes")
    sub.add_argument("lanes", nargs="+")
    sub = commands.add_parser("install-hooks", help="merge the generic Devin hooks into <project>/.devin/hooks.v1.json"
                                                    ", or into ~/.config/devin/config.json without a project")
    sub.add_argument("project", nargs="?")
    sub = commands.add_parser("push", help="post a lane's undelivered messages from this host (run by the ssh relay)")
    sub.add_argument("mail")
    sub.add_argument("--min-age", type=float, default=0.0)
    sub = commands.add_parser("hook", help="Devin hook entry point (reads the event payload on stdin)")
    sub.add_argument("event")
    args = parser.parse_args(argv)
    if args.command == "hook":
        return hook_main(args.event)
    handlers = {"send": lambda: cmd_send(args, "note"), "ask": lambda: cmd_send(args, "ask"),
                "reply": lambda: cmd_reply(args), "ack": lambda: cmd_ack(args), "inbox": lambda: cmd_inbox(args),
                "show": lambda: cmd_show(args), "status": lambda: cmd_status(args), "lanes": lambda: cmd_lanes(args),
                "register": lambda: cmd_register(args), "run": lambda: cmd_run(args),
                "adopt": lambda: cmd_adopt(args), "install-hooks": lambda: cmd_install_hooks(args),
                "push": lambda: cmd_push(args)}
    try:
        print(handlers[args.command]())
    except LaneError as exc:
        print(f"lanemsg: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
