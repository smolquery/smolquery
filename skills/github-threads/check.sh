#!/usr/bin/env bash
# Reports activity on the GitHub threads tracked in the smolquery-pm tracker
# (project `github-threads`), and open issues and PRs on smolquery/smolquery
# that no thread tracks yet. Run as a Claude Code SessionStart hook, it prints
# the hook's JSON: a short line for the user and the details as context.
#
#   ./skills/github-threads/check.sh          # hook JSON
#   ./skills/github-threads/check.sh --text   # the report as plain text
#
# Activity means a comment, review or state change by someone other than us
# after the thread's task was last updated in the tracker. Any failure (no
# network, no gh auth, no tracker creds) prints nothing and exits 0, so a
# session never fails to start because of it.
set -uo pipefail

repo=smolquery/smolquery
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
export PATH="$HOME/.local/share/mise/shims:$PATH"
cd "$root" || exit 0

me=$(gh api user --jq .login 2>/dev/null) || exit 0

tracked=$(elixir skills/query-db/smolsqls_query.exs --db pm --json \
  "SELECT t.key, t.status, t.title, t.body, t.updated_at
     FROM tasks t JOIN projects p ON p.id = t.project_id
    WHERE p.slug = 'github-threads'" 2>/dev/null) || exit 0

threads=$(jq -c '.data.rows[] | {
    key: .[0], status: .[1], title: .[2],
    url: ((.[3] // "") | (capture("(?m)^url: (?<u>\\S+)") | .u) // null),
    since: ((.[4] // "1970-01-01 00:00:00") | sub(" "; "T") + "Z")
  } | select(.url != null)' <<<"$tracked") || exit 0

activity=""
waiting_on_us=""

while IFS= read -r t; do
  [ -n "$t" ] || continue
  key=$(jq -r .key <<<"$t")
  status=$(jq -r .status <<<"$t")
  title=$(jq -r .title <<<"$t")
  url=$(jq -r .url <<<"$t")
  since=$(jq -r .since <<<"$t")

  case "$status" in done | cancelled) continue ;; esac
  [ "$status" = todo ] && waiting_on_us+="- $key $title ($url)"$'\n'

  path=$(sed -E 's#^https://github.com/([^/]+)/([^/]+)/(issues|pull)/([0-9]+).*#\1/\2 \4#' <<<"$url")
  read -r slug number <<<"$path"
  [ -n "${number:-}" ] || continue

  issue=$(gh api "repos/$slug/issues/$number" 2>/dev/null) || continue
  state=$(jq -r '.state + (if .pull_request.merged_at then " (merged)" else "" end)' <<<"$issue")
  changed=$(jq -r --arg s "$since" '(.closed_at // "") > $s' <<<"$issue")

  new=$(gh api "repos/$slug/issues/$number/comments?since=$since&per_page=100" 2>/dev/null |
    jq -r --arg me "$me" --arg s "$since" '.[]
      | select(.user.login != $me and .updated_at > $s)
      | "    \(.user.login) \(.updated_at[0:16]): \(.body | gsub("\\s+"; " ") | .[0:240])"')

  if jq -e '.pull_request' <<<"$issue" >/dev/null; then
    new+=$(gh api "repos/$slug/pulls/$number/reviews?per_page=100" 2>/dev/null |
      jq -r --arg me "$me" --arg s "$since" '.[]
        | select(.user.login != $me and (.submitted_at // "") > $s)
        | "\n    \(.user.login) \(.submitted_at[0:16]) review \(.state): \(.body // "" | gsub("\\s+"; " ") | .[0:200])"')
    new+=$(gh api "repos/$slug/pulls/$number/comments?since=$since&per_page=100" 2>/dev/null |
      jq -r --arg me "$me" --arg s "$since" '.[]
        | select(.user.login != $me and .updated_at > $s)
        | "\n    \(.user.login) \(.updated_at[0:16]) on \(.path): \(.body | gsub("\\s+"; " ") | .[0:200])"')
  fi

  if [ -n "$new" ] || [ "$changed" = true ]; then
    activity+="- $key $title, now $state ($url)"$'\n'
    [ -n "$new" ] && activity+="$(sed '/^$/d' <<<"$new")"$'\n'
  fi
done <<<"$threads"

known=$(jq -r '.url' <<<"$threads")
untracked=$(gh api "repos/$repo/issues?state=open&per_page=100" 2>/dev/null |
  jq -r --arg me "$me" '.[]
    | select(.user.login != $me and .user.type != "Bot")
    | "\(.html_url)\t\(if .pull_request then "PR" else "issue" end) #\(.number) by \(.user.login), \(.created_at[0:10]): \(.title)"' |
  while IFS=$'\t' read -r u line; do
    grep -qxF "$u" <<<"$known" || printf -- '- %s (%s)\n' "$line" "$u"
  done)

report=""
[ -n "$activity" ] && report+=$'New activity since we last touched these threads:\n'"$activity"$'\n'
[ -n "$waiting_on_us" ] && report+=$'Threads waiting on our reply (status todo):\n'"$waiting_on_us"$'\n'
[ -n "$untracked" ] && report+="Open issues and PRs on $repo that no thread tracks yet:"$'\n'"$untracked"$'\n'

if [ "${1:-}" = --text ]; then
  printf '%s' "${report:-Nothing new on tracked GitHub threads.}"$'\n'
  exit 0
fi

[ -n "$report" ] || exit 0

n_activity=$(grep -c '^- ' <<<"$activity")
n_waiting=$(grep -c '^- ' <<<"$waiting_on_us")
n_untracked=$(grep -c '^- ' <<<"$untracked")
summary="GitHub threads: $n_activity with new activity, $n_waiting waiting on our reply, $n_untracked untracked on $repo"

context="GitHub threads check (skills/github-threads). Before other work, tell the user what needs a reply and offer to draft each one; post nothing without their OK. Track or ignore untracked items as the skill says."$'\n\n'"$report"

jq -n --arg m "$summary" --arg c "$context" \
  '{systemMessage: $m, hookSpecificOutput: {hookEventName: "SessionStart", additionalContext: $c}}'
