# GitHub issue archive

`issues-dump.json` is the complete, verified capture of this repo's GitHub issue tracker as it
stood on **2026-08-14**, taken immediately before the issues were deleted and the repo migrated to
the in-repo Backlog.md tracker under `backlog/`.

**This file is the record, not a pointer.** The issues it describes no longer exist on GitHub, so
`gh issue view <N>` will not resolve them. Read them from here.

## What is in it

140 issues — **77 open, 63 closed** — sorted by number, with every field that carried content:

`number`, `title`, `body`, `comments`, `labels`, `state`, `stateReason`, `author`, `createdAt`,
`updatedAt`, `closedAt`, `url`, `milestone`, `assignees`.

Pull requests are excluded (this repo commits straight to `main`, so there were none to keep).

## Reading it

```bash
# one issue, body and all
jq '.[] | select(.number == 125)' archive/issues-dump.json

# the closed set, one row each
jq -r '.[] | select(.state=="CLOSED") | "#\(.number)\t\(.closedAt[:10])\t\(.title)"' archive/issues-dump.json

# full text search across bodies and comments
jq -r '.[] | select((.body // "") + ([.comments[].body] | join(" ")) | test("checkpoint"; "i")) | "#\(.number) \(.title)"' \
  archive/issues-dump.json
```

The closed set also has a human-readable index in the tracker: `backlog doc list --plain`, the
"Closed GitHub issues" document. The open set became tasks — `backlog task list --plain`.

## Completeness — verified, not assumed

`gh issue list --json comments` **paginates**, so a dump can silently carry a truncated comment set.
This one was checked rather than trusted: the REST API's own per-issue `.comments` counts were
summed and required to match the dump exactly.

```bash
gh api --paginate 'repos/rknightion/sf2loki/issues?state=all&per_page=100' \
  --jq '.[]|select(.pull_request==null)|{number,comments}'
```

Result: **140 issues in both, 9 comments in both, zero count mismatches, zero numbers on one side
only.** The comment total is genuinely that low — the 2026-07-30 audit wave put its findings in
issue bodies, not in follow-up comments.

## Redaction — swept, and none was needed

`backlog/` and this archive are committed to a public repo, so the capture was swept for account
identifiers and secrets **before** it entered git. The result was clean, so the file is unredacted
and there is no placeholder mapping to consult.

The sweep ran over the **decoded string fields** — 2,349 of them, walked recursively out of the
parsed JSON — and never over the serialized blob. That distinction is load-bearing: in
`json.dumps` output an escape such as `\n` leaves a literal `n` hard against the following word,
which breaks a `\b` word boundary and lets a regex certify a file clean while it still leaks.

Patterns swept: email addresses, Salesforce org IDs (`00D…`) and instance domains, Grafana Cloud
hostnames, Grafana/GitHub/Context7 token prefixes (`glc_`, `ghp_`, `github_pat_`, `ctx7sk-`, JWT
`eyJ`), AWS access keys, PEM private-key headers, Slack tokens, GCP service accounts, UUID/GUIDs,
long base64, IPv4 addresses, tailnet (`*.ts.net`) and local (`*.local`, `*.lan`, `*.internal`)
hostnames.

Everything that matched was benign and is deliberately preserved:

- The credential-shaped strings — `123456:glc_TOKEN@logs-prod-006.grafana.net`, `tenant:sekrit@…`,
  `example.my.salesforce.com`, `svc@example.com` — are **illustrative placeholders inside the body
  of issue #125**, which is itself about scrubbing inline URL credentials from the startup banner.
  Redacting them would destroy the issue's meaning. `logs-prod-006` is a generic Grafana Cloud
  cluster hostname used as documentation, not this account's tenant.
- Every `long_b64` hit is a 40-hex git SHA, a 64-hex sha256, or a URL path segment.
- `rknightion` (the repo owner) and `m7kni.io` (the public docs site) are already public throughout
  the tracked repo.
- The only IPv4 present is `127.0.0.1`.

Authors are `rknightion` (139) and `app/rknightion-renovate` (1).
