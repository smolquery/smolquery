---
name: github-threads
description: >-
  Follow up on the GitHub issues and PRs we take part in, here and upstream
  (DuckDB, adbc, Explorer, ...). Each thread is a task in the smolquery-pm
  tracker project `github-threads`; a SessionStart hook (`check.sh`) reports
  new comments, reviews and state changes, threads waiting on our reply, and
  open issues/PRs on smolquery/smolquery nobody tracks yet. Use when the
  session-start report lists threads, when we open or reply to an issue or PR,
  or when asked "what's waiting on us on GitHub", "track this issue",
  "follow up on", "any replies".
---

# GitHub threads

Every GitHub conversation we are part of is one task in the smolquery-pm
tracker, project **`github-threads`** (P-6). The tracker is the record; the
SessionStart hook is the reminder. Use the `smolquery-pm` skill for the query
tool (`pmq`).

## The task

- **Title:** `<owner>/<repo>#<n>: <short title>`.
- **Body:** first line `url: <issue or PR link>` (the hook reads it), then
  what we said, what we are waiting on, and the T- keys of the work behind it.
- **Status is whose turn it is:**

| status | meaning |
|---|---|
| `todo` | needs our reply |
| `blocked` | we replied; waiting on them |
| `done` | closed, merged, or nothing left to say |
| `cancelled` | we are not following it (still counts as tracked) |

- **`updated_at` is when we last looked.** The hook reports activity by anyone
  but us after it. So any update to the task, even only a note, marks the
  thread as seen.

## When the session-start report lists something

1. Tell the user, before other work: which threads have new activity, which
   wait on us, which items are untracked.
2. For each one that needs a reply, read the thread and offer a draft.
   **Post nothing without the user's OK.** Comments start with 🤖 and follow
   the ADHD-friendly style (claim first, one idea per line).
3. After posting, or after the user decides no reply is needed, update the
   task: append a dated note, set the status, bump `updated_at`.
4. An untracked item: track it (`todo` if it needs a reply) or record it as
   `cancelled` so it stops showing.

## Recipes

```sh
pmq() { elixir skills/query-db/smolsqls_query.exs --db pm "$@"; }

# Track a thread (status todo or blocked)
body=$'url: https://github.com/OWNER/REPO/issues/N\n\nWhat we said. Waiting on: X.\n\nWork: T-123.'
pmq "INSERT INTO tasks (project_id, title, body, status, priority, created_by, updated_by)
     VALUES ((SELECT id FROM projects WHERE slug = 'github-threads'), ?, ?, ?, 'med', ?, ?)
     RETURNING key" \
  --args "$(jq -nc --arg t 'OWNER/REPO#N: title' --arg b "$body" '[$t, $b, "blocked", "chasers", "chasers"]')"

# Record a reply (or "seen, nothing to say") and hand the turn back
pmq "UPDATE tasks SET status = 'blocked', body = body || ?, updated_by = 'chasers',
            updated_at = datetime('now') WHERE key = ?" \
  --args '["\n\n**2026-10-27:** replied (link). Waiting on: X.", "T-744"]'

# Open threads
pmq "SELECT t.key, t.status, t.title FROM tasks t JOIN projects p ON p.id = t.project_id
      WHERE p.slug = 'github-threads' AND t.status IN ('todo', 'blocked') ORDER BY t.status DESC, t.id"

# The same report the hook gives, as text
./skills/github-threads/check.sh --text
```

When a thread belongs to other work (an upstream report from a T- task),
also note the link on that task, and close both when the thread ends.

## The hook

`check.sh` needs `gh` logged in, `jq`, `elixir` (mise shims) and the tracker
credentials in `.claude/smolquery-pm.env`. On any failure it prints nothing
and exits 0. It runs from `.claude/settings.local.json`:

```json
{
  "hooks": {
    "SessionStart": [
      { "hooks": [ { "type": "command",
                     "command": "\"$CLAUDE_PROJECT_DIR\"/skills/github-threads/check.sh",
                     "timeout": 60 } ] }
    ]
  }
}
```

It costs one tracker query, one `gh` call per open thread (three for a PR),
and one listing of open issues on smolquery/smolquery: about 3 s.
