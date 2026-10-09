---
name: lanemsg
description: Two-way chat between you (a Codex or Claude Code orchestrator, or one of its subagents) and the Devin CLI lanes you launch. Use whenever you launch, supervise, message, or answer a Devin lane, or when a "[lanemsg] QUESTION|NOTE|ANSWER from lane ..." message arrives.
---

# lanemsg: chat with the Devin lanes you launch

A lane can ask you a question or report a result while it works, and you can send it notes and questions
mid-run. `lanemsg` is on PATH on every Trinity node; the full documentation is
`/data2/jjyeung/claude-devin-chat/README.md`. It adds a chat channel only: keep your project's coordination
rules (leases, briefs, ledgers, heartbeats) as they are.

## Launch every Devin lane through lanemsg

1. Register the lane from your own shell, once. A relative name goes under `/data2/jjyeung/lanemsg/lanes`; an
   absolute path on `/data2` works too. Registration records your session and thread as the lane's receiver.

   ```bash
   lanemsg register <run>/<lane>
   ```

2. Launch each attempt with `lanemsg run`, which gives the attempt a fresh number and the lane's environment,
   then runs the command. It works inside `ssh`, `nohup` and `timeout`:

   ```bash
   ssh <node> 'cd <project> && nohup timeout 8h lanemsg run <run>/<lane> -- devin -p --permission-mode dangerous --model <model> --prompt-file <brief> > <log> 2>&1 &'
   ```

   Devin's `-p` takes the next argument as the prompt, so use `--prompt-file` or put the prompt right after
   `-p`. A lane started without `lanemsg run` cannot chat.

## Who receives a lane's messages

The thread that registered or launched the lane receives its questions and notes, steered into that thread's
running turn: a subagent that launched a lane talks with it directly. An answer goes first to the thread that
asked the question. When the intended thread runs no turn, the main session receives the message, with a note
naming that thread. A lane on another node reaches you over ssh. Lane messages arrive as user messages that
start with `[lanemsg] QUESTION|NOTE|ANSWER from lane <name> (attempt <n>)`; the lane's name, its key, or the
8-character prefix of any of its message ids works as `<lane>` below.

Each arriving message starts a turn over your whole context, so a lane's routine notes wait: they travel with
the lane's next question or answer, arrive after about 30 minutes, or arrive when the lane stops. Lanes use
`send --now` when you must act soon, and `lanemsg inbox` shows held notes at any time. Brief lanes to message
only for questions, blockers and final results; leave routine state to your watchdog.

Writing to a lane that another live session receives does not take it over: the output names the receiver,
and `lanemsg adopt <lane>` takes the lane over when you mean to. `lanemsg reply` is safe to repeat after a
timeout; it never writes a second answer.

## Commands

```bash
lanemsg reply <id> "<answer>"     # answer a lane's question; the lane is waiting for it
lanemsg send <lane> "<text>"      # a note; a running lane sees it at its next tool call
lanemsg ask <lane> "<text>"       # a question the lane must answer
lanemsg inbox                     # open lane messages, including any that could not be pushed
lanemsg lanes --all               # registered lanes and their queues
lanemsg status <lane>             # mailbox state, bound Devin sessions, recent events
lanemsg adopt <lane>              # take a lane over, e.g. after your session restarts
lanemsg orchestrators             # the orchestrator sessions that registered lanes name
lanemsg tell all "<text>"         # a note to every orchestrator session active in the last day
```

## When you stop something another agent started

The user's rule: whenever you stop work that another agent started (a lane, an orchestrator, a service or a
job), tell the orchestrators right away, including the distillation orchestrator: what you stopped, where, why,
and that they can restart it whenever they choose.

```bash
lanemsg tell all --from "<who you are>" "I stopped <what> on <host> because <why>; restart it whenever you want."
```

## A lane waiting for your answer

A lane that cannot continue without your answer asks with `lanemsg ask --wait`, which blocks in its running
session (up to 30 minutes) and keeps its heartbeat fresh. Such a question arrives marked "waits in its running
session (no relaunch needed)". Reply, and read the reply's output: "the lane is waiting in its session" means
the lane continues on its own, so do not relaunch it. Any other output means the lane ended its session; then
launch a new attempt, for example with a continuation brief:
`lanemsg run <lane> -- devin -p ... --prompt-file <continuation brief>`. The new session receives your answer
when it starts. Resuming the old session with `devin -r <session-id>` also works when Devin's session store is
healthy.
