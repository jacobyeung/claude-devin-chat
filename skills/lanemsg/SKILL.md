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
start with `[lanemsg] QUESTION|NOTE|ANSWER from lane ...`.

## Commands

```bash
lanemsg reply <id> "<answer>"     # answer a lane's question; the lane is waiting for it
lanemsg send <lane> "<text>"      # a note; a running lane sees it at its next tool call
lanemsg ask <lane> "<text>"       # a question the lane must answer
lanemsg inbox                     # open lane messages, including any that could not be pushed
lanemsg lanes --all               # registered lanes and their queues
lanemsg status <lane>             # mailbox state, bound Devin sessions, recent events
lanemsg adopt <lane>              # take a lane over, e.g. after your session restarts
```

## A lane waiting for your answer

A Devin lane that cannot continue without your answer ends its turn and its process exits. After you reply,
launch a new attempt, for example with a continuation brief:
`lanemsg run <lane> -- devin -p ... --prompt-file <continuation brief>`. The new session receives your answer
when it starts. Resuming the old session with `devin -r <session-id>` also works when Devin's session store is
healthy.
