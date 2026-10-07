from __future__ import annotations

import json
import os
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


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    for key in ("LANEMSG_DIR", "LANEMSG_ATTEMPT", "LANEMSG_ENGINE", "LANEMSG_HOOK", "LANEMSG_PYTHON"):
        monkeypatch.delenv(key, raising=False)
    (tmp_path / "no_claude").mkdir()
    monkeypatch.setenv("LANEMSG_RUNS_ROOT", str(tmp_path / "runs"))
    monkeypatch.setenv("LANEMSG_REGISTRY", str(tmp_path / "registry"))
    monkeypatch.setenv("LANEMSG_CLAUDE_SESSIONS", str(tmp_path / "no_claude"))


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
    mailbox.set_orchestrator({"claude_session_id": ORCHESTRATOR_ID}, "test")
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
    assert mailbox.orchestrator()["claude_session_id"] == ORCHESTRATOR_ID


def test_unpushed_messages_wait_and_are_posted_once_an_orchestrator_exists(lane, monkeypatch, capsys, fake_claude):
    mailbox = lane()
    as_lane(monkeypatch, mailbox)
    code, out, _ = cli(capsys, "send", "finished step 1")
    assert code == 0 and "queued" in out
    assert lanemsg.push_pending(mailbox) == 0
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
    assert mailbox.orchestrator()["claude_session_id"] == ORCHESTRATOR_ID
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
    assert lanemsg.claude_ancestor()["claude_session_id"] == ORCHESTRATOR_ID
    path.write_text(json.dumps({**data, "pidDomain": "linux:0000000000000000:pid:[1]"}))
    assert lanemsg.claude_ancestor() is None
    assert lanemsg.claude_socket({"claude_session_id": ORCHESTRATOR_ID}) is None
    path.write_text(json.dumps({**data, "pidDomain": lanemsg.pid_domain()}))
    assert lanemsg.claude_socket({"claude_session_id": ORCHESTRATOR_ID}) == data["messagingSocketPath"]
