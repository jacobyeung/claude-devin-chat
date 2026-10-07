from __future__ import annotations

import base64
import contextlib
import hashlib
import io
import json
import os
import shlex
import socket
import subprocess
import sys
import tempfile
import threading
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
import lanemsg  # noqa: E402

ORCHESTRATOR_ID = "11111111-2222-3333-4444-555555555555"
CODEX_ID = "01a1155a-bea1-7b21-ae0e-3fb92b820cf5"


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    for key in ("LANEMSG_DIR", "LANEMSG_ATTEMPT", "LANEMSG_ENGINE", "LANEMSG_HOOK", "LANEMSG_PYTHON",
                "CODEX_SESSION_ID", "CODEX_THREAD_ID"):
        monkeypatch.delenv(key, raising=False)
    (tmp_path / "no_claude").mkdir()
    monkeypatch.setenv("LANEMSG_RUNS_ROOT", str(tmp_path / "runs"))
    monkeypatch.setenv("LANEMSG_REGISTRY", str(tmp_path / "registry"))
    monkeypatch.setenv("LANEMSG_CLAUDE_SESSIONS", str(tmp_path / "no_claude"))
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "codex"))


@pytest.fixture
def lane(tmp_path):
    def create(name="alpha", run="r1"):
        lane_dir = tmp_path / "runs" / run / "ctrl" / name
        lane_dir.mkdir(parents=True)
        return lanemsg.register_lane(lane_dir, name, lane_dir.parent)
    return create


@pytest.fixture
def fake_claude(tmp_path, monkeypatch):
    """A Claude Code session stand-in: a session file for this test process and an inbox socket server."""
    sessions = tmp_path / "claude_sessions"
    sessions.mkdir()
    sock_path = os.path.join(tempfile.mkdtemp(prefix="lm", dir="/tmp"), "cc.sock")
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(sock_path)
    server.listen()
    received = []

    def serve():
        while True:
            try:
                connection, _ = server.accept()
            except OSError:
                return
            with connection:
                data = b""
                while True:
                    chunk = connection.recv(65536)
                    if not chunk:
                        break
                    data += chunk
                received.append(json.loads(data.decode()))
                connection.sendall(b'{"ok":true}\n')
    threading.Thread(target=serve, daemon=True).start()
    start = Path(f"/proc/{os.getpid()}/stat").read_text().rsplit(")", 1)[1].split()[19]
    (sessions / f"{os.getpid()}.json").write_text(json.dumps({
        "pid": os.getpid(), "procStart": start, "sessionId": ORCHESTRATOR_ID, "name": "fake-orch",
        "messagingSocketPath": sock_path}))
    monkeypatch.setenv("LANEMSG_CLAUDE_SESSIONS", str(sessions))
    yield received
    server.close()


@pytest.fixture
def fake_codex():
    """A Codex app-server daemon stand-in: JSON-RPC over WebSocket on the control socket in CODEX_HOME, which is
    a symlink to a short /tmp path as in a real installation."""
    control = Path(os.environ["CODEX_HOME"]) / "app-server-control"
    control.mkdir(parents=True)
    sock_path = os.path.join(tempfile.mkdtemp(prefix="lm", dir="/tmp"), "codex.sock")
    os.symlink(sock_path, control / "app-server-control.sock")
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(sock_path)
    server.listen()
    daemon = {"status": "idle", "turn": {"id": "turn-1", "status": "inProgress"}, "steer_error": None, "calls": []}

    def handle(connection):
        with connection, connection.makefile("rb") as stream:
            head = [stream.readline()]
            while head[-1] not in (b"\r\n", b""):
                head.append(stream.readline())
            key = next(line.split(b":", 1)[1].strip() for line in head if line.lower().startswith(b"sec-websocket-key"))
            accept = base64.b64encode(hashlib.sha1(key + lanemsg.WEBSOCKET_GUID.encode()).digest())
            connection.sendall(b"HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\nconnection: Upgrade\r\n"
                               b"sec-websocket-accept: " + accept + b"\r\n\r\n")
            while True:
                try:
                    request = json.loads(lanemsg.websocket_receive(stream))
                except OSError:
                    return
                if "id" not in request:
                    continue
                daemon["calls"].append((request["method"], request["params"]))
                lanemsg.websocket_send(connection, json.dumps({"method": "configWarning", "params": {}}), masked=False)
                result = {"thread/read": {"thread": {"status": {"type": daemon["status"]}}},
                          "thread/turns/list": {"data": [daemon["turn"]]}}.get(request["method"], {})
                reply = {"id": request["id"], "result": result}
                if request["method"] == "turn/steer" and daemon["steer_error"]:
                    reply = {"id": request["id"], "error": {"code": -32600, "message": daemon["steer_error"]}}
                lanemsg.websocket_send(connection, json.dumps(reply), masked=False)

    def serve():
        while True:
            try:
                connection, _ = server.accept()
            except OSError:
                return
            handle(connection)
    threading.Thread(target=serve, daemon=True).start()
    yield daemon
    server.close()


def as_lane(monkeypatch, mailbox, attempt=1, engine="devin"):
    monkeypatch.setenv("LANEMSG_DIR", str(mailbox.path))
    monkeypatch.setenv("LANEMSG_ATTEMPT", str(attempt))
    monkeypatch.setenv("LANEMSG_ENGINE", engine)


def as_orchestrator(monkeypatch):
    for key in ("LANEMSG_DIR", "LANEMSG_ATTEMPT", "LANEMSG_ENGINE"):
        monkeypatch.delenv(key, raising=False)


def cli(capsys, *argv):
    code = lanemsg.main(list(argv))
    captured = capsys.readouterr()
    return code, captured.out.strip(), captured.err.strip()


def hook(event, session="sess-A", **payload):
    return lanemsg.run_hook(event, {"session_id": session, "hook_event_name": event, **payload})


def context(output):
    return output["hookSpecificOutput"]["additionalContext"]


def names(mailbox, queue, state):
    return sorted(path.stem for path in mailbox.dir(queue, state).glob("*.json"))


def test_messages_reach_a_new_session_with_identity_and_commands(lane, monkeypatch, capsys):
    mailbox = lane()
    assert cli(capsys, "send", "alpha", "use", "split", "B")[0] == 0
    assert cli(capsys, "ask", "r1/ctrl/alpha", "what is 2+2?")[0] == 0
    as_lane(monkeypatch, mailbox)
    text = context(hook("SessionStart", source="startup"))
    assert "You are lane `r1/ctrl/alpha` (attempt 1)" in text
    assert "use split B" in text and "what is 2+2?" in text
    assert " reply " in text and " ack " in text and "resumes this same session" in text
    assert names(mailbox, "inbox", "new") == []
    assert len(names(mailbox, "inbox", "cur")) == 2
    assert mailbox.session_for(1) == "sess-A"


def test_a_nested_session_cannot_take_the_lanes_messages(lane, monkeypatch, capsys):
    mailbox = lane()
    as_lane(monkeypatch, mailbox)
    assert hook("SessionStart", source="startup") is not None
    as_orchestrator(monkeypatch)
    cli(capsys, "send", "alpha", "for the lane only")
    as_lane(monkeypatch, mailbox)
    assert hook("SessionStart", session="sess-NESTED", source="startup") is None
    assert hook("PostToolUse", session="sess-NESTED", tool_name="exec") is None
    assert hook("Stop", session="sess-NESTED", stop_hook_active=False) is None
    assert len(names(mailbox, "inbox", "new")) == 1
    assert "for the lane only" in context(hook("PostToolUse", tool_name="exec"))


def test_post_tool_use_is_silent_without_messages_and_delivers_each_message_once(lane, monkeypatch, capsys):
    mailbox = lane()
    as_lane(monkeypatch, mailbox)
    hook("SessionStart", source="startup")
    assert hook("PostToolUse", tool_name="exec") is None
    as_orchestrator(monkeypatch)
    cli(capsys, "send", "alpha", "mid-run note")
    as_lane(monkeypatch, mailbox)
    assert "mid-run note" in context(hook("PostToolUse", tool_name="exec"))
    assert hook("PostToolUse", tool_name="exec") is None


def test_stop_delivers_new_messages_then_reminds_about_questions_at_most_twice(lane, monkeypatch, capsys):
    mailbox = lane()
    as_lane(monkeypatch, mailbox)
    hook("SessionStart", source="startup")
    as_orchestrator(monkeypatch)
    cli(capsys, "ask", "alpha", "ready to merge?")
    as_lane(monkeypatch, mailbox)
    first = hook("Stop", stop_hook_active=False)
    assert first["decision"] == "block" and "ready to merge?" in first["reason"]
    reminders = [hook("Stop", stop_hook_active=True) for _ in range(3)]
    assert [bool(item) for item in reminders] == [True, True, False]
    assert "waiting for your answer" in reminders[0]["reason"]
    assert hook("Stop", stop_hook_active=False) is None


def test_open_messages_resurface_in_the_next_attempts_session(lane, monkeypatch, capsys):
    mailbox = lane()
    cli(capsys, "ask", "alpha", "which split?")
    as_lane(monkeypatch, mailbox, attempt=1)
    hook("SessionStart", source="startup")
    as_lane(monkeypatch, mailbox, attempt=2)
    text = context(hook("SessionStart", session="sess-B", source="startup"))
    assert "Still open from earlier sessions" in text and "which split?" in text
    as_lane(monkeypatch, mailbox, attempt=3)
    resumed = context(hook("SessionStart", session="sess-B", source="resume"))
    assert "which split?" not in resumed


def test_subagent_events_do_not_receive_or_answer_the_lanes_messages(lane, monkeypatch, capsys):
    mailbox = lane()
    as_lane(monkeypatch, mailbox)
    hook("SessionStart", source="startup")
    assert hook("PreToolUse", tool_name="run_subagent", tool_use_id="t1",
                tool_input={"task": "x", "is_background": False}) is None
    as_orchestrator(monkeypatch)
    cli(capsys, "ask", "alpha", "status?")
    as_lane(monkeypatch, mailbox)
    assert hook("PostToolUse", tool_name="exec", tool_use_id="t2") is None
    assert hook("Stop", stop_hook_active=False) is None
    assert len(names(mailbox, "inbox", "new")) == 1
    output = hook("PostToolUse", tool_name="run_subagent", tool_use_id="t1", tool_input={"is_background": False},
                  tool_response={"success": True, "output": "Subagent agent_id=ab12 completed successfully:\n\ndone"})
    assert "status?" in context(output)


def test_background_subagent_hold_ends_when_its_result_is_read(lane, monkeypatch, capsys):
    mailbox = lane()
    as_lane(monkeypatch, mailbox)
    hook("SessionStart", source="startup")
    hook("PreToolUse", tool_name="run_subagent", tool_use_id="t1", tool_input={"is_background": True})
    hook("PostToolUse", tool_name="run_subagent", tool_use_id="t1", tool_input={"is_background": True},
         tool_response={"success": True, "output": "Subagent agent_id=ab12 started in the background"})
    as_orchestrator(monkeypatch)
    cli(capsys, "send", "alpha", "note during background work")
    as_lane(monkeypatch, mailbox)
    assert hook("PostToolUse", tool_name="exec", tool_use_id="t2") is None
    output = hook("PostToolUse", tool_name="read_subagent", tool_use_id="t3", tool_input={"agent_id": "ab12"},
                  tool_response={"success": True, "output": "Subagent ab12 completed successfully:\n\nok"})
    assert "note during background work" in context(output)
    assert not lanemsg.subagent_running(mailbox.state(), "sess-A")


def test_compaction_reinjects_identity_at_the_next_tool_call(lane, monkeypatch):
    mailbox = lane()
    as_lane(monkeypatch, mailbox)
    hook("SessionStart", source="startup")
    assert hook("PostCompaction", summary="...") is None
    assert "You are lane" in context(hook("PostToolUse", tool_name="exec"))
    assert hook("PostToolUse", tool_name="exec") is None


def test_question_round_trip_through_the_orchestrators_claude_session(lane, monkeypatch, capsys, fake_claude):
    mailbox = lane()
    as_lane(monkeypatch, mailbox)
    hook("SessionStart", source="startup")
    mailbox.set_orchestrator({"agent": "claude", "session_id": ORCHESTRATOR_ID}, "test")
    code, out, _ = cli(capsys, "ask", "may I use 8 GPUs?")
    assert code == 0 and "delivered to the orchestrator" in out
    pushed = fake_claude[-1]["message"]["content"]
    question_id = names(mailbox, "outbox", "cur")[0]
    assert fake_claude[-1]["type"] == "user" and "may I use 8 GPUs?" in pushed
    assert f"reply {question_id}" in pushed and "QUESTION from lane r1/ctrl/alpha" in pushed
    as_orchestrator(monkeypatch)
    code, out, _ = cli(capsys, "reply", question_id, "yes,", "until", "06:30")
    assert code == 0
    assert names(mailbox, "outbox", "cur") == [] and names(mailbox, "outbox", "done") == [question_id]
    as_lane(monkeypatch, mailbox)
    text = context(hook("PostToolUse", tool_name="exec"))
    assert "yes, until 06:30" in text and f"answer to your message {question_id}" in text
    assert names(mailbox, "inbox", "cur") == []
    assert mailbox.orchestrator()["session_id"] == ORCHESTRATOR_ID


def test_unpushed_messages_wait_and_are_posted_once_an_orchestrator_exists(lane, monkeypatch, capsys, fake_claude):
    mailbox = lane()
    as_lane(monkeypatch, mailbox)
    code, out, _ = cli(capsys, "send", "finished step 1")
    assert code == 0 and "queued" in out
    assert lanemsg.push_pending(mailbox) == []
    events = [json.loads(line)["event"] for line in (mailbox.path / "events.jsonl").read_text().splitlines()]
    assert events.count("push_failed") == 1
    copy, = mailbox.path.parent.glob("TO_ORCHESTRATOR_*.md")
    assert "finished step 1" in copy.read_text() and "NOTE from lane r1/ctrl/alpha" in copy.read_text()
    as_orchestrator(monkeypatch)
    assert "fake-orch now receives messages" in cli(capsys, "adopt", "alpha")[1]
    assert names(mailbox, "outbox", "new") == []
    assert "finished step 1" in fake_claude[-1]["message"]["content"]


def test_orchestrator_messages_record_their_claude_session_as_the_reply_target(lane, capsys, fake_claude):
    mailbox = lane()
    cli(capsys, "send", "alpha", "hello")
    assert {**mailbox.orchestrator(), "set_at": None} == {
        "agent": "claude", "session_id": ORCHESTRATOR_ID, "name": "fake-orch", "host": socket.gethostname(),
        "set_by": "orchestrator wrote to the lane", "set_at": None}
    sender = mailbox.list("inbox")[0][1]["sender"]
    assert sender["agent"] == "claude" and sender["name"] == "fake-orch"


def test_answered_questions_and_acks_on_questions_are_refused(lane, monkeypatch, capsys):
    mailbox = lane()
    cli(capsys, "ask", "alpha", "which seed?")
    question_id = names(mailbox, "inbox", "new")[0]
    as_lane(monkeypatch, mailbox)
    code, _, err = cli(capsys, "ack", question_id)
    assert code == 1 and "is a question" in err
    assert cli(capsys, "reply", question_id, "seed 7")[0] == 0
    code, _, err = cli(capsys, "reply", question_id, "seed 8")
    assert code == 1 and "already answered" in err
    assert len(names(mailbox, "outbox", "new")) == 1


def test_codex_lanes_read_messages_with_the_inbox_command(lane, monkeypatch, capsys):
    mailbox = lane()
    cli(capsys, "send", "alpha", "please rebase")
    as_lane(monkeypatch, mailbox, engine="codex")
    out = cli(capsys, "inbox")[1]
    assert "please rebase" in out
    note_id = names(mailbox, "inbox", "cur")[0]
    assert "Still open" in cli(capsys, "inbox")[1]
    assert cli(capsys, "ack", note_id)[0] == 0
    assert cli(capsys, "inbox")[1] == "no open messages"


def test_orchestrator_inbox_lists_lane_messages_and_marks_them_read(lane, monkeypatch, capsys):
    mailbox = lane()
    as_lane(monkeypatch, mailbox)
    cli(capsys, "ask", "need a decision")
    cli(capsys, "send", "fyi")
    as_orchestrator(monkeypatch)
    out = cli(capsys, "inbox")[1]
    assert "need a decision" in out and "fyi" in out
    assert len(names(mailbox, "outbox", "cur")) == 1 and len(names(mailbox, "outbox", "done")) == 1
    assert "need a decision" in cli(capsys, "inbox")[1]


def test_lane_references_resolve_by_suffix_and_report_ambiguity(lane, capsys):
    lane("router", run="r1")
    lane("router", run="r2")
    lane("other", run="r2")
    code, _, err = cli(capsys, "send", "router", "x")
    assert code == 1 and "matches several lanes" in err
    assert cli(capsys, "send", "r2/ctrl/router", "x")[0] == 0
    assert cli(capsys, "send", "other", "x")[0] == 0
    code, _, err = cli(capsys, "send", "missing", "x")
    assert code == 1 and "no registered lane" in err


def test_concurrent_readers_deliver_each_message_exactly_once(lane, capsys, tmp_path):
    mailbox = lane()
    for index in range(30):
        cli(capsys, "send", "alpha", f"message {index}")
    script = (f"import sys; sys.path.insert(0, {str(REPO)!r}); import lanemsg, json; "
              f"print(json.dumps([m['id'] for m in lanemsg.Mailbox({str(mailbox.path)!r}).deliver(1, 's', 'race')]))")
    procs = [subprocess.Popen([sys.executable, "-c", script], stdout=subprocess.PIPE, env=os.environ.copy())
             for _ in range(6)]
    claimed = [message for proc in procs for message in json.loads(proc.communicate()[0])]
    assert len(claimed) == 30 and len(set(claimed)) == 30
    assert names(mailbox, "inbox", "new") == []


def test_hooks_are_inert_outside_lanes_and_install_idempotently(tmp_path):
    project = tmp_path / "project"
    (project / ".devin").mkdir(parents=True)
    other = {"matcher": "exec", "hooks": [{"type": "command", "command": "true"}]}
    (project / ".devin" / "hooks.v1.json").write_text(json.dumps({"PostToolUse": [other]}))
    for _ in range(2):
        lanemsg.main(["install-hooks", str(project)])
    config = json.loads((project / ".devin" / "hooks.v1.json").read_text())
    assert config["PostToolUse"][0] == other and len(config["PostToolUse"]) == 2
    assert set(config) == {"SessionStart", "UserPromptSubmit", "PostToolUse", "Stop", "PostCompaction", "PreToolUse"}
    command = config["Stop"][0]["hooks"][0]["command"]
    env = {key: value for key, value in os.environ.items() if not key.startswith("LANEMSG_")}
    result = subprocess.run(command, shell=True, input='{"session_id": "x"}', capture_output=True, text=True, env=env)
    assert result.returncode == 0 and result.stdout == ""


def test_example_hooks_file_matches_what_install_hooks_writes():
    assert json.loads((REPO / "examples" / "hooks.v1.json").read_text()) == lanemsg.hooks_config()


def test_default_locations_are_per_user(tmp_path, monkeypatch):
    for key in ("LANEMSG_RUNS_ROOT", "LANEMSG_REGISTRY", "LANEMSG_CLAUDE_SESSIONS"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    assert lanemsg.runs_root() == tmp_path / "home"
    assert lanemsg.registry() == tmp_path / "state" / "lanemsg" / "lanes"
    assert lanemsg.claude_sessions() == tmp_path / "home" / ".claude" / "sessions"
    monkeypatch.delenv("XDG_STATE_HOME")
    assert lanemsg.registry() == tmp_path / "home" / ".local" / "state" / "lanemsg" / "lanes"


def test_hook_failures_never_break_the_lane(lane, monkeypatch, capsys):
    mailbox = lane()
    as_lane(monkeypatch, mailbox)
    monkeypatch.setattr(lanemsg, "run_hook", lambda event, payload: 1 / 0)
    monkeypatch.setattr(sys, "stdin", __import__("io").StringIO("{}"))
    assert lanemsg.hook_main("PostToolUse") == 0
    assert "ZeroDivisionError" in (mailbox.path / "hook_errors.log").read_text()


def test_stale_binding_from_a_reused_attempt_number_is_set_aside(lane, monkeypatch):
    mailbox = lane()
    as_lane(monkeypatch, mailbox)
    hook("SessionStart", session="old-run", source="startup")
    mailbox.reset_binding(1)
    assert hook("SessionStart", session="new-run", source="startup") is not None
    assert mailbox.session_for(1) == "new-run"


def test_file_channel_notes_reach_the_lane_and_acks_land_in_heartbeat(tmp_path, monkeypatch, capsys):
    lane_dir = tmp_path / "runs" / "r1" / "ctrl" / "alpha"
    lane_dir.mkdir(parents=True)
    (lane_dir / "ORCHESTRATOR_NOTE_00_old.md").write_text("old note the lane already handled")
    (lane_dir / "HEARTBEAT.md").write_text("2026-10-06T00:00:00Z | ACK ORCHESTRATOR_NOTE_00_old.md\n")
    mailbox = lanemsg.register_lane(lane_dir, "alpha", lane_dir.parent)
    as_lane(monkeypatch, mailbox)
    hook("SessionStart", source="startup")
    assert hook("PostToolUse", tool_name="exec") is None
    (lane_dir / "ORCHESTRATOR_NOTE_20_new.md").write_text("switch to split C")
    text = context(hook("PostToolUse", tool_name="exec"))
    assert "switch to split C" in text and "file ORCHESTRATOR_NOTE_20_new.md" in text
    assert "old note" not in text
    note_id = names(mailbox, "inbox", "cur")[0]
    assert cli(capsys, "ack", note_id)[0] == 0
    assert "ACK ORCHESTRATOR_NOTE_20_new.md" in (lane_dir / "HEARTBEAT.md").read_text()
    assert hook("PostToolUse", tool_name="exec") is None


def test_mailboxes_created_before_the_file_bridge_get_only_unacknowledged_notes(lane, monkeypatch):
    mailbox = lane()
    mailbox.update_state(lambda state: state.pop("imported_notes", None))
    lane_dir = mailbox.path.parent
    (lane_dir / "ORCHESTRATOR_NOTE_00_rule.md").write_text("old rule")
    (lane_dir / "ORCHESTRATOR_NOTE_10_pending.md").write_text("pending instruction")
    (lane_dir / "HEARTBEAT.md").write_text("t | ACK ORCHESTRATOR_NOTE_00_rule 2026-10-06T01:00Z\n")
    as_lane(monkeypatch, mailbox)
    text = context(hook("SessionStart", source="startup"))
    assert "pending instruction" in text and "old rule" not in text
    assert set(mailbox.state()["imported_notes"]) == {"ORCHESTRATOR_NOTE_00_rule.md", "ORCHESTRATOR_NOTE_10_pending.md"}


def test_claude_session_files_from_other_hosts_are_ignored(lane, capsys, fake_claude, tmp_path, monkeypatch):
    sessions = Path(os.environ["LANEMSG_CLAUDE_SESSIONS"])
    path = sessions / f"{os.getpid()}.json"
    data = json.loads(path.read_text())
    assert lanemsg.orchestrator_ancestor()["session_id"] == ORCHESTRATOR_ID
    path.write_text(json.dumps({**data, "pidDomain": "linux:0000000000000000:pid:[1]"}))
    assert lanemsg.orchestrator_ancestor() is None
    assert lanemsg.claude_socket({"session_id": ORCHESTRATOR_ID}) is None
    path.write_text(json.dumps({**data, "pidDomain": lanemsg.pid_domain()}))
    assert lanemsg.claude_socket({"session_id": ORCHESTRATOR_ID}) == data["messagingSocketPath"]


def codex_orchestrator(mailbox, host=None):
    mailbox.set_orchestrator({"agent": "codex", "session_id": CODEX_ID, "host": host or socket.gethostname()}, "test")


def last_event(mailbox):
    return json.loads((mailbox.path / "events.jsonl").read_text().splitlines()[-1])


def test_a_codex_orchestrator_is_recorded_from_its_shell_environment(lane, monkeypatch, capsys):
    mailbox = lane()
    monkeypatch.setenv("CODEX_SESSION_ID", CODEX_ID)
    monkeypatch.setenv("CODEX_THREAD_ID", "thread-of-a-subagent")
    monkeypatch.setattr(lanemsg, "codex_process", lambda pid: pid == os.getppid())
    assert cli(capsys, "send", "alpha", "hello")[0] == 0
    orchestrator = mailbox.orchestrator()
    assert (orchestrator["agent"], orchestrator["session_id"], orchestrator["host"]) == \
        ("codex", CODEX_ID, socket.gethostname())
    sender = mailbox.list("inbox")[0][1]["sender"]
    assert sender["thread_id"] == "thread-of-a-subagent" and sender["pid"] == os.getppid()
    as_lane(monkeypatch, mailbox)
    assert f"from orchestrator codex session {CODEX_ID}" in context(hook("SessionStart", source="startup"))


def test_the_nearest_orchestrator_up_the_process_tree_wins(fake_claude, monkeypatch):
    monkeypatch.setenv("CODEX_SESSION_ID", CODEX_ID)
    monkeypatch.setattr(lanemsg, "codex_process", lambda pid: pid == os.getppid())
    assert lanemsg.orchestrator_ancestor()["agent"] == "claude"
    sessions = Path(os.environ["LANEMSG_CLAUDE_SESSIONS"])
    data = json.loads((sessions / f"{os.getpid()}.json").read_text())
    (sessions / f"{os.getpid()}.json").rename(sessions / "moved.json.old")
    start = Path(f"/proc/{os.getppid()}/stat").read_text().rsplit(")", 1)[1].split()[19]
    (sessions / f"{os.getppid()}.json").write_text(json.dumps({**data, "pid": os.getppid(), "procStart": start}))
    monkeypatch.setattr(lanemsg, "codex_process", lambda pid: pid == os.getpid())
    assert lanemsg.orchestrator_ancestor()["agent"] == "codex"


def test_lane_messages_queue_as_the_next_turn_of_an_idle_codex_session(lane, monkeypatch, capsys, fake_codex):
    mailbox = lane()
    codex_orchestrator(mailbox)
    as_lane(monkeypatch, mailbox)
    code, out, _ = cli(capsys, "send", "finished", "step", "1")
    assert code == 0 and "delivered to the orchestrator" in out
    note_id, = names(mailbox, "outbox", "done")
    assert [method for method, _ in fake_codex["calls"]] == ["initialize", "thread/read", "thread/queue/add"]
    params = fake_codex["calls"][-1][1]
    assert params["threadId"] == CODEX_ID and params["clientUserMessageId"] == note_id
    assert "finished step 1" in params["input"][0]["text"] and "NOTE from lane r1/ctrl/alpha" in params["input"][0]["text"]


def test_lane_questions_steer_into_the_running_turn_of_a_codex_session(lane, monkeypatch, capsys, fake_codex):
    fake_codex["status"] = "active"
    mailbox = lane()
    codex_orchestrator(mailbox)
    as_lane(monkeypatch, mailbox)
    assert cli(capsys, "ask", "may I use 8 GPUs?")[0] == 0
    method, params = fake_codex["calls"][-1]
    assert method == "turn/steer" and params["expectedTurnId"] == "turn-1" and params["threadId"] == CODEX_ID
    assert "may I use 8 GPUs?" in params["input"][0]["text"] and len(names(mailbox, "outbox", "cur")) == 1
    assert "steered into the running turn" in last_event(mailbox)["response"]


def test_a_turn_that_ends_before_the_steer_lands_gets_the_message_queued(lane, monkeypatch, capsys, fake_codex):
    fake_codex.update(status="active", steer_error="expected turn turn-1 is not active")
    mailbox = lane()
    codex_orchestrator(mailbox)
    as_lane(monkeypatch, mailbox)
    assert "delivered" in cli(capsys, "send", "results are in")[1]
    assert [method for method, _ in fake_codex["calls"]][-2:] == ["turn/steer", "thread/queue/add"]


def test_messages_for_an_unreachable_codex_session_wait_in_the_outbox(lane, monkeypatch, capsys, fake_codex):
    fake_codex["status"] = "notLoaded"
    mailbox = lane()
    codex_orchestrator(mailbox)
    as_lane(monkeypatch, mailbox)
    code, out, _ = cli(capsys, "send", "done")
    assert code == 0 and "queued" in out and len(names(mailbox, "outbox", "new")) == 1
    assert last_event(mailbox)["event"] == "push_failed" and "notLoaded" in last_event(mailbox)["error"]
    assert len(list(mailbox.path.parent.glob("TO_ORCHESTRATOR_*.md"))) == 1
    (Path(os.environ["CODEX_HOME"]) / "app-server-control" / "app-server-control.sock").rename(
        Path(os.environ["CODEX_HOME"]) / "moved.sock.old")
    assert lanemsg.push_pending(mailbox) == []
    assert "no Codex app-server daemon is running" in last_event(mailbox)["error"]


def test_lanes_on_other_hosts_relay_the_push_to_the_orchestrators_host(lane, monkeypatch, capsys, fake_codex):
    mailbox = lane()
    codex_orchestrator(mailbox, host="orchestrator-host")
    commands = []

    def ssh(argv, **kwargs):
        commands.append(argv)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            lanemsg.main(shlex.split(argv[-1])[3:])
        return subprocess.CompletedProcess(argv, 0, out.getvalue(), "")
    monkeypatch.setattr(lanemsg.subprocess, "run", ssh)
    monkeypatch.setattr(lanemsg.Mailbox, "find", lambda self, message_id: ("outbox", "new", {}))
    as_lane(monkeypatch, mailbox)
    code, out, _ = cli(capsys, "send", "from another node")
    assert code == 0 and "delivered to the orchestrator" in out
    assert commands[0][0] == "ssh" and commands[0][-2] == "orchestrator-host"
    assert shlex.split(commands[0][-1])[:4] == [sys.executable, "-I", str(lanemsg.SELF), "push"]
    assert fake_codex["calls"][-1][0] == "thread/queue/add" and names(mailbox, "outbox", "new") == []
    monkeypatch.setattr(lanemsg.subprocess, "run",
                        lambda argv, **kwargs: subprocess.CompletedProcess(argv, 255, "", "Connection refused"))
    assert "queued" in cli(capsys, "send", "second note")[1]
    assert last_event(mailbox)["error"] == "relay to orchestrator-host failed: Connection refused"


def test_register_records_the_calling_session_and_run_starts_numbered_attempts(tmp_path, capsys, fake_claude):
    code, out, _ = cli(capsys, "register", str(tmp_path / "runs" / "r1" / "beta"))
    assert code == 0 and "registered lane r1/beta" in out and "fake-orch" in out and " run r1/beta -- devin" in out
    mailbox = lanemsg.resolve_lane("beta")
    assert mailbox.orchestrator()["session_id"] == ORCHESTRATOR_ID
    probe = "import json, os; print(json.dumps({k: v for k, v in os.environ.items() if k.startswith('LANEMSG_')}))"
    first, second = (json.loads(subprocess.run(
        [sys.executable, str(lanemsg.SELF), "run", "beta", "--", sys.executable, "-c", probe],
        capture_output=True, text=True, check=True).stdout) for _ in range(2))
    assert (first["LANEMSG_ATTEMPT"], second["LANEMSG_ATTEMPT"]) == ("1", "2")
    assert first["LANEMSG_DIR"] == str(mailbox.path.resolve()) and first["LANEMSG_HOOK"] == str(lanemsg.SELF)
    assert first["LANEMSG_PYTHON"] == sys.executable and first["LANEMSG_ENGINE"] == "devin"
    assert cli(capsys, "register", str(tmp_path / "runs" / "r1" / "gamma"), "--session", f"codex:{CODEX_ID}@h1")[0] == 0
    assert lanemsg.resolve_lane("gamma").orchestrator()["host"] == "h1"


def test_orchestrators_named_by_hand():
    assert lanemsg.parse_target(f"codex:{CODEX_ID}@trinity-3-13") == \
        {"agent": "codex", "session_id": CODEX_ID, "host": "trinity-3-13"}
    assert lanemsg.parse_target("fake-orch") == {"agent": "claude", "name": "fake-orch"}
    assert lanemsg.parse_target(f"claude:{ORCHESTRATOR_ID}") == {"agent": "claude", "session_id": ORCHESTRATOR_ID}


def test_install_hooks_merges_into_the_devin_user_config(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    path = tmp_path / "home" / ".config" / "devin" / "config.json"
    path.parent.mkdir(parents=True)
    guard = {"matcher": "exec", "hooks": [{"type": "command", "command": "guard"}]}
    path.write_text(json.dumps({"version": 1, "hooks": {"PreToolUse": [guard]}, "theme_mode": "dark"}))
    path.chmod(0o600)
    for _ in range(2):
        assert lanemsg.main(["install-hooks"]) == 0
    config = json.loads(path.read_text())
    assert config["version"] == 1 and config["theme_mode"] == "dark" and path.stat().st_mode & 0o777 == 0o600
    assert config["hooks"]["PreToolUse"] == [guard] + lanemsg.hooks_config()["PreToolUse"]
    assert {event: entries[-1:] for event, entries in config["hooks"].items()} == lanemsg.hooks_config()


def test_instructions_name_lanemsg_when_the_command_on_path_runs_this_file(tmp_path, monkeypatch):
    (tmp_path / "bin").mkdir()
    wrapper = tmp_path / "bin" / "lanemsg"
    monkeypatch.setenv("PATH", str(tmp_path / "bin"))
    for target, expected in ((lanemsg.SELF, "lanemsg"), ("/elsewhere/lanemsg.py",
                             f"{shlex.quote(sys.executable)} {shlex.quote(str(lanemsg.SELF))}")):
        wrapper.write_text(f'#!/bin/sh\nexec /opt/python -I {target} "$@"\n')
        wrapper.chmod(0o755)
        lanemsg.command.cache_clear()
        assert lanemsg.command() == expected
    lanemsg.command.cache_clear()


def test_websocket_frames_round_trip_at_every_length_encoding():
    left, right = socket.socketpair()
    with left, right, right.makefile("rb") as stream:
        for size in (5, 300, 70000):
            sender = threading.Thread(target=lanemsg.websocket_send, args=(left, "é" * size))
            sender.start()
            assert lanemsg.websocket_receive(stream) == "é" * size
            sender.join()
