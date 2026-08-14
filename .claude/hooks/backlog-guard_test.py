"""Negative-and-positive test for the Backlog.md PreToolUse guard.

The cases live in this file rather than in a shell command on purpose: the hook denies any
`backlog` command containing the literal bare flags, so a test harness that spelled them out in
its own command string would block itself. They are also assembled from fragments here for the
same reason.

Run: python3 .claude/hooks/backlog-guard_test.py
"""

import json
import os
import subprocess
import sys

ROOT = "/Users/rob/repos/sf2loki"
HOOK = f"{ROOT}/.claude/hooks/backlog-guard.py"
env = dict(os.environ, CLAUDE_PROJECT_DIR=ROOT)
N = "--" + "notes"
P = "--" + "plan"

cases = [
    # --- must BLOCK (exit 2) ---
    ("bare notes flag",      {"tool_name": "Bash", "tool_input": {"command": f"backlog task edit SFL-0001 {N} hi"}}, 2),
    ("bare plan flag",       {"tool_name": "Bash", "tool_input": {"command": f"backlog task edit SFL-0001 {P} hi"}}, 2),
    ("equals form",          {"tool_name": "Bash", "tool_input": {"command": f"backlog task edit SFL-0001 {N}=hi"}}, 2),
    ("flag at end of line",  {"tool_name": "Bash", "tool_input": {"command": f"backlog task edit SFL-0001 {N}"}}, 2),
    ("bare flag on create",  {"tool_name": "Bash", "tool_input": {"command": f"backlog task create x {N} hi"}}, 2),
    ("edit task md",         {"tool_name": "Edit",  "tool_input": {"file_path": f"{ROOT}/backlog/tasks/sfl-0002 - x.md"}}, 2),
    ("write doc md",         {"tool_name": "Write", "tool_input": {"file_path": f"{ROOT}/backlog/docs/doc-0002 - Wave-operating-model.md"}}, 2),
    ("edit milestone md",    {"tool_name": "Edit",  "tool_input": {"file_path": f"{ROOT}/backlog/milestones/m-0 - x.md"}}, 2),
    ("edit completed md",    {"tool_name": "Edit",  "tool_input": {"file_path": f"{ROOT}/backlog/completed/sfl-0009 - z.md"}}, 2),
    ("edit archived task",   {"tool_name": "Edit",  "tool_input": {"file_path": f"{ROOT}/backlog/archive/tasks/sfl-0001 - x.md"}}, 2),

    # --- must ALLOW (exit 0) — the half that actually proves the guard is not a blanket deny ---
    ("append-notes allowed", {"tool_name": "Bash", "tool_input": {"command": "backlog task edit SFL-0001 --append-notes hi"}}, 0),
    ("append-plan allowed",  {"tool_name": "Bash", "tool_input": {"command": "backlog task edit SFL-0001 --append-plan hi"}}, 0),
    ("finalize in one call", {"tool_name": "Bash", "tool_input": {"command": "backlog task edit SFL-0001 --check-ac 1 -s Done"}}, 0),
    ("task list allowed",    {"tool_name": "Bash", "tool_input": {"command": "backlog task list --plain -m 'Test-coverage backlog'"}}, 0),
    ("doc update allowed",   {"tool_name": "Bash", "tool_input": {"command": "backlog doc update doc-0002 --content x"}}, 0),
    ("non-backlog cmd",      {"tool_name": "Bash", "tool_input": {"command": f"mytool {N} foo"}}, 0),
    ("config.yml allowed",   {"tool_name": "Edit",  "tool_input": {"file_path": f"{ROOT}/backlog/config.yml"}}, 0),
    ("source file allowed",  {"tool_name": "Edit",  "tool_input": {"file_path": f"{ROOT}/src/sf2loki/app.py"}}, 0),
    ("test file allowed",    {"tool_name": "Write", "tool_input": {"file_path": f"{ROOT}/tests/test_pipeline.py"}}, 0),
    ("AGENTS.md allowed",    {"tool_name": "Write", "tool_input": {"file_path": f"{ROOT}/AGENTS.md"}}, 0),
    ("archive dump allowed", {"tool_name": "Write", "tool_input": {"file_path": f"{ROOT}/archive/README.md"}}, 0),
]

fails = 0
for name, payload, want in cases:
    r = subprocess.run([sys.executable, HOOK], input=json.dumps(payload),
                       capture_output=True, text=True, env=env)
    ok = r.returncode == want
    fails += not ok
    print(f"{'PASS' if ok else 'FAIL'}  exit={r.returncode} want={want}  {name}")

# garbage stdin must never block — a hook that fails closed on a parse error wedges every tool call
r = subprocess.run([sys.executable, HOOK], input="not json", capture_output=True, text=True, env=env)
ok = r.returncode == 0
fails += not ok
print(f"{'PASS' if ok else 'FAIL'}  exit={r.returncode} want=0  garbage stdin never blocks")

total = len(cases) + 1
print(f"\n{total - fails}/{total} passed")
sys.exit(1 if fails else 0)
