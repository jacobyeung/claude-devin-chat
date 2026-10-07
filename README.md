# claude-devin-chat

Two-way chat between a Claude Code session (the **orchestrator**) and Devin CLI sessions doing delegated
work (**lanes**). The orchestrator sends notes and questions to a lane while it works; the lane sends notes,
questions and answers back, and they arrive in the Claude session as user messages.

Everything is one dependency-free Python script, [`lanemsg.py`](lanemsg.py): a CLI for both sides plus the
handler for Devin's lifecycle hooks. Codex lanes are supported by polling.

## How it works

- **Mailboxes.** Each lane owns `<lane dir>/mail/` with two Maildir-style queues: `inbox/` (orchestrator to
  lane) and `outbox/` (lane to orchestrator), each with `tmp/`, `new/` (not yet delivered), `cur/` (delivered,
  still open) and `done/`. Messages change state only by `rename(2)`, so a crash never leaves a partial
  message and exactly one reader claims each one. `events.jsonl` in the mailbox is an audit log.
- **Claude to Devin.** `lanemsg send|ask <lane> "<text>"` writes to the lane's inbox. Devin hooks run
  `lanemsg.py hook <Event>`: `SessionStart`, `UserPromptSubmit` and `PostToolUse` inject new messages into the
  lane's context as `[lanemsg]` blocks, so a running lane sees them at its next tool call. `Stop` keeps the
  lane from ending its turn while a new message is undelivered or a question from the orchestrator is
  unanswered (at most two reminders per question).
- **Devin to Claude.** Inside a lane, `lanemsg send|ask|reply` writes to the outbox and pushes the message into
  the orchestrator's Claude Code session through that session's local messaging socket (`messagingSocketPath`
  in `~/.claude/sessions/<pid>.json`). If no live orchestrator session is found, the message stays queued and a
  `TO_ORCHESTRATOR_*.md` copy is written to the lane directory; it is retried with the lane's next message, and
  `lanemsg adopt` or `lanemsg inbox` picks it up.
- **Safety.** The hooks do nothing in a process whose environment does not name a lane, so they are safe to
  install in a shared project. Each attempt binds to the first Devin session that reports in, so nested Devin
  processes that inherit the environment cannot take the lane's messages, and delivery is held while a
  `run_subagent` call is open. A failing hook never breaks the lane; tracebacks go to `mail/hook_errors.log`.

## Requirements

- Linux (`/proc`, `flock`, Unix sockets) and Python 3.8 or newer; no third-party packages.
- Claude Code with per-session messaging sockets (its session files in `~/.claude/sessions/` include
  `messagingSocketPath`). Developed with Claude Code 2.1.292.
- Devin CLI with `.devin/hooks.v1.json` support. Developed with Devin CLI 3000.11.3.
- Pushes reach a Claude session only from the host it runs on. Mailboxes and the registry can live on a shared
  filesystem; messages from lanes on other hosts wait in the queue until `lanemsg inbox` or `lanemsg adopt`
  runs on the orchestrator's host.

## Install

```bash
git clone https://github.com/jacobyeung/claude-devin-chat.git
ln -s "$PWD/claude-devin-chat/lanemsg.py" ~/.local/bin/lanemsg   # any directory on PATH
lanemsg install-hooks /path/to/project                           # the project your Devin lanes run in
```

`install-hooks` merges the hooks into `<project>/.devin/hooks.v1.json` and keeps any other hooks there;
running it again changes nothing. The result is [`examples/hooks.v1.json`](examples/hooks.v1.json). Inside
Devin, `/hooks` lists the loaded hooks. When `lanemsg` on PATH resolves to this file, the instructions shown to
lanes say `lanemsg ...`; otherwise they spell out `python3 /path/to/lanemsg.py ...`.

## Quick start

1. **Register a lane.** This creates its mailbox and adds it to the registry. The arguments are the lane
   directory, the lane name, the run root (the directory holding a run's lanes), and optionally the
   orchestrator's Claude session name or id.

   ```bash
   PYTHONPATH=/path/to/claude-devin-chat python3 -c \
     'import sys, lanemsg; lanemsg.register_lane(*sys.argv[1:])' "$HOME/lanes/alpha" alpha "$HOME/lanes"
   ```

2. **Start Devin as that lane** from the project with the hooks installed. The lane's first context block
   tells it who it is and which commands to use.

   ```bash
   cd /path/to/project
   LANEMSG_DIR="$HOME/lanes/alpha/mail" LANEMSG_ATTEMPT=1 LANEMSG_HOOK=/path/to/claude-devin-chat/lanemsg.py \
     devin -p --prompt-file brief.md
   ```

3. **Connect your Claude Code session.** Have Claude run `lanemsg adopt alpha` with its shell tool. lanemsg
   finds the calling Claude session by walking up the process tree, so it must run inside that session. Any
   `send`, `ask` or `reply` that Claude runs also records its session as the lane's orchestrator.

4. **Chat.** From the Claude session:

   ```bash
   lanemsg send alpha "use split B"      # a note; the lane acks it when handled
   lanemsg ask alpha "ready to merge?"   # a question; the lane must reply
   lanemsg reply <id> "yes"              # answer a question from a lane
   lanemsg inbox                         # messages from lanes, including any that could not be pushed
   lanemsg lanes --all                   # registered lanes with their queue counts
   lanemsg status alpha                  # mailbox state, bound Devin sessions, recent events
   lanemsg show <id>                     # one message as JSON
   ```

   Inside a lane (where `LANEMSG_DIR` is set) the same commands act for the lane: `lanemsg send "<text>"`,
   `lanemsg ask "<text>"`, `lanemsg reply <id> "<text>"`, `lanemsg ack <id>` and `lanemsg inbox`. Pass
   `--orchestrator` to act as the orchestrator from inside a lane's environment, and `-` as the text to read it
   from stdin.

5. **Resume a lane that is waiting for you.** A Devin lane that asked a question and cannot continue without
   the answer ends its turn. After you reply, resume the same session so the answer is delivered; `lanemsg
   status alpha` shows the session id. Give every launch a new attempt number, because an attempt binds to the
   first Devin session that reports in.

   ```bash
   LANEMSG_DIR="$HOME/lanes/alpha/mail" LANEMSG_ATTEMPT=2 LANEMSG_HOOK=/path/to/claude-devin-chat/lanemsg.py \
     devin -r <session-id> -p "Continue; the orchestrator's answer is in your context."
   ```

## Writing a launcher

To supervise lanes automatically, a launcher imports `lanemsg` and:

- calls `register_lane(lane_dir, name, run_root, orchestrator="")` once per lane;
- launches each attempt with `LANEMSG_DIR`, `LANEMSG_ATTEMPT` (a new number per launch, or call
  `Mailbox.reset_binding(n)` before reusing one), `LANEMSG_HOOK` (the path to `lanemsg.py`), `LANEMSG_PYTHON`
  (the interpreter for the hooks, default `python3`) and `LANEMSG_ENGINE` (`devin`, the default, or `codex`);
- calls `push_pending(mailbox, min_age_s=30)` about once a minute to retry pushes that failed;
- when a Devin attempt exits cleanly, resumes `mailbox.session_for(n)` at once if `mailbox.has("inbox", "new")`,
  or otherwise, while `mailbox.open_questions()` is non-empty, parks the lane until a message lands in
  `inbox/new/`.

`lanemsg lanes` reports a lane as `done` if `<lane dir>/DONE.json` exists, `failed` if `FAILURE_FLAG.json`
exists, and otherwise uses `lanes.<name>.status` from `<run root>/_controller/STATE.json` if a launcher writes
one (`running`, `parked` and `waiting` get delivery hints).

Codex lanes have no hooks. Launch them with `LANEMSG_ENGINE=codex` and tell them in their prompt to run
`lanemsg inbox` between work steps; it delivers new messages and lists open ones.

Files named `ORCHESTRATOR_NOTE_*.md` dropped into a lane directory are imported as notes; acknowledging one
appends `ACK <file name>` to the lane's `HEARTBEAT.md`. Together with the `TO_ORCHESTRATOR_*.md` copies, this
lets tools that watch files take part.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `LANEMSG_REGISTRY` | `${XDG_STATE_HOME:-~/.local/state}/lanemsg/lanes` | Lane registry, one JSON file per lane; the orchestrator and its lanes must share it |
| `LANEMSG_RUNS_ROOT` | `$HOME` | Lane keys are lane directories relative to this root, or absolute paths for lanes outside it |
| `LANEMSG_CLAUDE_SESSIONS` | `~/.claude/sessions` | Where Claude Code writes its session files |
| `LANEMSG_DIR`, `LANEMSG_ATTEMPT`, `LANEMSG_HOOK`, `LANEMSG_PYTHON`, `LANEMSG_ENGINE` | unset | Lane identity, set by whatever launches the lane |

## Tests

```bash
python3 -m pytest tests
```
