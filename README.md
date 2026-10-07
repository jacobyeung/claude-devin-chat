# claude-devin-chat

Two-way chat between an orchestrator, which is a Claude Code or Codex session, and Devin CLI sessions doing
delegated work (**lanes**). The orchestrator sends notes and questions to a lane while it works; the lane sends
notes, questions and answers back, and they arrive as user messages in the session that launched the lane.

Everything is one dependency-free Python script, [`lanemsg.py`](lanemsg.py): a CLI for both sides plus the
handler for Devin's lifecycle hooks. Codex lanes are supported by polling. The defaults fit the Trinity
cluster: the script runs `/data2/jjyeung/envs/miniforge3/bin/python`, and the lane registry and lane folders
live under `/data2/jjyeung/lanemsg`.

## How it works

- **Mailboxes.** Each lane owns `<lane dir>/mail/` with two Maildir-style queues: `inbox/` (orchestrator to
  lane) and `outbox/` (lane to orchestrator), each with `tmp/`, `new/` (not yet delivered), `cur/` (delivered,
  still open) and `done/`. Messages change state only by `rename(2)`, so a crash never leaves a partial
  message and exactly one reader claims each one. `events.jsonl` in the mailbox is an audit log.
- **Orchestrator to lane.** `lanemsg send|ask <lane> "<text>"` writes to the lane's inbox. Devin hooks run
  `lanemsg.py hook <Event>`: `SessionStart`, `UserPromptSubmit` and `PostToolUse` inject new messages into the
  lane's context as `[lanemsg]` blocks, so a running lane sees them at its next tool call. `Stop` keeps the
  lane from ending its turn while a new message is undelivered or a question from the orchestrator is
  unanswered (at most two reminders per question).
- **Lane to orchestrator.** Inside a lane, `lanemsg send|ask|reply` writes to the outbox and pushes the message
  to whoever launched the lane, on the host where that session runs:
  - a Claude Code session through its local messaging socket (`messagingSocketPath` in
    `~/.claude/sessions/<pid>.json`);
  - a Codex session through the host's Codex app-server daemon. The thread that registered or launched the
    lane, a subagent or the main session, gets each message steered into its running turn, and an answer goes
    first to the thread that asked the question. When that thread runs no turn, the main session gets the
    message instead, with a note naming the intended thread: steered into its running turn, or queued as its
    next turn, which the daemon starts at once.

  A lane on another host runs the push on the orchestrator's host over ssh. If the push fails, for example
  because the orchestrator's session has ended, the message stays queued and a `TO_ORCHESTRATOR_*.md` copy is
  written to the lane directory; it is retried with the lane's next message, and `lanemsg adopt` or
  `lanemsg inbox` picks it up.
- **Safety.** The hooks do nothing in a process whose environment does not name a lane, so they are safe to
  install for every project. Each attempt binds to the first Devin session that reports in, so nested Devin
  processes that inherit the environment cannot take the lane's messages, and delivery is held while a
  `run_subagent` call is open. A failing hook never breaks the lane; tracebacks go to `mail/hook_errors.log`.

## Requirements

- Linux (`/proc`, `flock`, Unix sockets) and Python 3.8 or newer; no third-party packages. The shebang names
  the Trinity interpreter; elsewhere, run `python3 lanemsg.py` or edit the shebang.
- Devin CLI with hooks support. Developed with Devin CLI 3000.11.3.
- For a Claude Code orchestrator: per-session messaging sockets (its session files in `~/.claude/sessions/`
  include `messagingSocketPath`). Developed with Claude Code 2.1.292.
- For a Codex orchestrator: a session hosted by the Codex app-server daemon, which is how a plain `codex`
  starts in Codex 0.160. A TUI started with `-c` overrides runs its own in-process server, which lanemsg
  cannot reach. Developed with Codex CLI 0.160.1.
- Lanes on several hosts need the mailboxes and the registry on a shared filesystem (`/data2` on Trinity), and
  passwordless ssh from each lane's host to the orchestrator's host.

## Install

```bash
git clone https://github.com/jacobyeung/claude-devin-chat.git /data2/jjyeung/claude-devin-chat
ln -s /data2/jjyeung/claude-devin-chat/lanemsg.py ~/.local/bin/lanemsg
lanemsg install-hooks                    # every project, via ~/.config/devin/config.json
ln -s /data2/jjyeung/claude-devin-chat/skills/lanemsg ~/.codex/skills/lanemsg
ln -s /data2/jjyeung/claude-devin-chat/skills/lanemsg ~/.claude/skills/lanemsg
```

`lanemsg install-hooks` merges the hooks into the user's Devin config and keeps the other settings and hooks
there; `lanemsg install-hooks /path/to/project` writes them to `<project>/.devin/hooks.v1.json` instead, and
the result is [`examples/hooks.v1.json`](examples/hooks.v1.json). Install them in one place only, because
hooks in both places run twice. Running either command again changes nothing. Inside Devin, `/hooks` lists the
loaded hooks. The [`lanemsg` skill](skills/lanemsg/SKILL.md) tells Codex and Claude Code orchestrators how to
use the tool, so new sessions find it without a briefing.

## Quick start

1. **Register a lane** from the shell of the session that will supervise it, the main orchestrator or one of
   its subagents. This creates the lane's mailbox under `/data2/jjyeung/lanemsg/lanes` (or at an absolute
   path), adds it to the registry, and records the calling Claude Code or Codex session and thread. lanemsg
   finds them by walking up the process tree.

   ```bash
   lanemsg register distill/research_plan      # --name, --run-root; --session when run outside the orchestrator
   ```

2. **Launch an attempt** from the project directory. `lanemsg run` gives the attempt a new number, puts the
   lane identity in its environment and then runs the command, so it composes with `nohup`, `timeout` and
   `ssh`. A launch inside a Codex thread also makes that thread the lane's launcher. The lane's first context
   block tells it who it is and which commands to use.

   ```bash
   lanemsg run distill/research_plan -- devin -p --prompt-file brief.md
   ssh node7 'cd /path/to/project && nohup lanemsg run distill/research_plan -- devin -p --prompt-file brief.md > lane.log 2>&1 &'
   ```

3. **Chat.** From the orchestrator's session:

   ```bash
   lanemsg send <lane> "use split B"     # a note; the lane acks it when handled
   lanemsg ask <lane> "ready to merge?"  # a question; the lane must reply
   lanemsg reply <id> "yes"              # answer a question from a lane
   lanemsg inbox                         # messages from lanes, including any that could not be pushed
   lanemsg lanes --all                   # registered lanes with their queue counts
   lanemsg status <lane>                 # mailbox state, bound Devin sessions, recent events
   lanemsg show <id>                     # one message as JSON
   lanemsg adopt <lane>                  # make this session and thread the lane's receiver, e.g. after a restart
   lanemsg orchestrators                 # the orchestrator sessions that registered lanes name
   lanemsg tell all "<text>"             # a note to every orchestrator session with lane activity in the last day
   ```

   A lane is named by its key (`distill/research_plan`), the last part of it, or its directory. Inside a lane
   (where `LANEMSG_DIR` is set) the same commands act for the lane: `lanemsg send "<text>"`,
   `lanemsg ask "<text>"`, `lanemsg reply <id> "<text>"`, `lanemsg ack <id>` and `lanemsg inbox`. Pass
   `--orchestrator` to act as the orchestrator from inside a lane's environment, and `-` as the text to read it
   from stdin. When a different session writes to a lane, it takes the lane over.

   `lanemsg tell` reaches orchestrators from anywhere, not only from lanes: any agent that stops work an
   orchestrator started tells it so, for example
   `lanemsg tell all --from "Devin" "I stopped lane X on trinity-2-23 because Y; restart it whenever you want."`
   The note goes to each session's main thread, steered into its running turn or queued as its next turn, and
   from another host it runs over ssh.

4. **Continue a lane that is waiting for you.** A Devin lane that asked a question and cannot continue without
   the answer ends its turn, and its process exits. After you reply, launch another attempt. Resuming the same
   session keeps the lane's conversation; `lanemsg status <lane>` shows the session id of each attempt. A
   fresh session with a continuation brief also works, because open messages and answers reach whichever
   session of the lane starts next.

   ```bash
   lanemsg run <lane> -- devin -r <session-id> -p "Continue; the orchestrator's answer is in your context."
   ```

## Codex orchestrators

Codex runs each shell command with the ids of its main session and of the running thread in
`CODEX_SESSION_ID` and `CODEX_THREAD_ID`. lanemsg records both, with the host, when a session registers,
launches or adopts a lane. A push asks the host's app-server daemon (over its control socket,
`$CODEX_HOME/app-server-control/app-server-control.sock`) for each thread's state:

- **The launching thread, or the asker of the question being answered, runs a turn:** the message is steered
  into that turn with `turn/steer`. It appears in the TUI as a user message, and the model reads it at its next
  step, so a subagent supervising a lane talks with it directly.
- **That thread runs no turn, but the main session does:** the main session gets the message steered into its
  turn, with a note naming the intended thread.
- **The main session is idle:** the message is queued as the session's next turn, which the daemon starts at
  once. The daemon keeps a session loaded after its TUI exits, so an idle session takes the turn even when
  nobody watches it.
- **The main session is not loaded on that host:** the push fails and the message waits as described above.

## Writing a launcher

To supervise lanes automatically, a launcher imports `lanemsg` and:

- calls `register_lane(lane_dir, name, run_root, orchestrator="")` once per lane, where `orchestrator` is
  `[claude:|codex:]<session id or Claude session name>[@<host>]`;
- launches each attempt with `LANEMSG_DIR`, `LANEMSG_ATTEMPT` (a new number per launch from
  `Mailbox.reserve_attempt()`, or call `Mailbox.reset_binding(n)` before reusing one), `LANEMSG_HOOK` (the path
  to `lanemsg.py`), `LANEMSG_PYTHON` (the interpreter for the hooks) and `LANEMSG_ENGINE` (`devin`, the
  default, or `codex`), as `lanemsg run` does;
- calls `push_pending(mailbox, min_age_s=30)` about once a minute to retry pushes that failed;
- when a Devin attempt exits cleanly, resumes `mailbox.session_for(n)` at once if `mailbox.has("inbox", "new")`,
  or otherwise, while `mailbox.open_questions()` is non-empty, parks the lane until a message lands in
  `inbox/new/`.

`lanemsg lanes` reports a lane as `done` if `<lane dir>/DONE.json` exists, `failed` if `FAILURE_FLAG.json`
exists, and otherwise uses `lanes.<name>.status` from `<run root>/_controller/STATE.json` if a launcher writes
one (`running`, `parked` and `waiting` get delivery hints).

Codex lanes have no hooks. Launch them with `lanemsg run <lane> -- codex ...` and tell them in their prompt to
run `lanemsg inbox` between work steps; it delivers new messages and lists open ones.

Files named `ORCHESTRATOR_NOTE_*.md` dropped into a lane directory are imported as notes; acknowledging one
appends `ACK <file name>` to the lane's `HEARTBEAT.md`. Together with the `TO_ORCHESTRATOR_*.md` copies, this
lets tools that watch files take part.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `LANEMSG_REGISTRY` | `/data2/jjyeung/lanemsg/registry` | Lane registry, one JSON file per lane; the orchestrator and its lanes must share it |
| `LANEMSG_RUNS_ROOT` | `/data2/jjyeung/lanemsg/lanes` | Where relative lane names go; lane keys are lane directories relative to it, or absolute paths for lanes outside it |
| `LANEMSG_CLAUDE_SESSIONS` | `~/.claude/sessions` | Where Claude Code writes its session files |
| `CODEX_HOME` | `~/.codex` | Where Codex keeps the app-server daemon's control socket |
| `LANEMSG_DIR`, `LANEMSG_ATTEMPT`, `LANEMSG_HOOK`, `LANEMSG_PYTHON`, `LANEMSG_ENGINE` | unset | Lane identity, set by `lanemsg run` or another launcher; the hooks fall back to `/data2/jjyeung/envs/miniforge3/bin/python` |

## Tests

```bash
python -m pytest tests     # any Python 3.8+ with pytest, e.g. /data2/jjyeung/envs/spatialclaw/bin/python
```
