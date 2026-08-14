---
id: SFL-0035
title: >-
  cli: doctor's pubsub probe discards TopicInfo.can_subscribe - a topic the
  integration user cannot subscribe to reports PASS
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-1
  - roadmap
milestone: m-4
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/119'
ordinal: 35000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`sf2loki doctor`'s per-topic Pub/Sub check treats "the `GetTopic` RPC did not raise" as proof the topic is usable, and throws away the one field in the response that states whether subscription is permitted.

- `PubSubClient.get_topic` (`src/sf2loki/salesforce/pubsub_client.py:252-268`) is declared `async def get_topic(self, topic: str) -> None`. It awaits `self._stub().GetTopic(pb.TopicRequest(topic_name=topic), metadata=await self._metadata())` and never assigns or returns the response. The `TopicInfo` message is decoded by grpc and immediately discarded.
- `_check_pubsub` (`src/sf2loki/doctor.py:276-282`) calls it inside `try`/`except Exception`, emitting `CheckResult(f"pubsub:{topic}", "FAIL", _format_topic_error(exc))` on any raise and `CheckResult(f"pubsub:{topic}", "PASS", "topic reachable")` otherwise. There is no other signal available to it.
- `proto/pubsub_api.proto:17-32` defines the response the RPC returns:

  ```protobuf
  message TopicInfo {
    string topic_name = 1;
    string tenant_guid = 2;
    bool can_publish = 3;
    // Is subscription allowed?
    bool can_subscribe = 4;
    string schema_id = 5;
    string rpc_id = 6;
  }
  ```

  `rg can_subscribe` across the repo hits only `proto/pubsub_api.proto:25`, the generated stub `src/sf2loki/salesforce/_generated/pubsub_api_pb2.py`, and `tests/salesforce/test_pubsub_client.py:894` — where the fake servicer *builds* a `pb.TopicInfo(..., can_subscribe=True, ...)` and the assertion (`test_get_topic_returns_none_and_sends_metadata`, `tests/salesforce/test_pubsub_client.py:888-910`) pins that the method returns `None`. No production code path reads the field.

`GetTopic` returning OK proves the channel exists and the caller is authenticated for it; `can_subscribe=false` is the API's distinct answer for "exists, but this principal is not authorised to subscribe" (missing Read on the platform event's entity, a permission-set grant that covers the channel but not the subscribe right, a publish-only entitlement). Doctor currently cannot distinguish that from a fully working topic.

## Why it matters

Deployment sequence today when the integration user lacks subscribe rights on a configured channel:

1. `sf2loki doctor` prints `pubsub:/event/MyCustomEvent  PASS  topic reachable` and exits 0 (`src/sf2loki/doctor.py:818-826` derives exit 1 only from a FAIL row).
2. The operator deploys.
3. `PubSubSource._stream_topic` (`src/sf2loki/sources/pubsub_source.py:461-640`) fails the `Subscribe` stream and enters its unbounded exponential-backoff reconnect loop — `stream_up.set(0)` at lines 573-581 / 630-636, backoff capped at `max_backoff`, retried forever.
4. The problem surfaces only as `sf2loki_pubsub_stream_up=0` and repeated reconnect logs, i.e. via dashboards/alerts minutes-to-hours later, and reads as a connectivity fault rather than a permission gap.

This is exactly the class of first-run misconfiguration doctor was built to front-load (`src/sf2loki/doctor.py:1-12`, issue #22). A wrong PASS is worse than an absent check: it directs the operator away from the real cause. The fix costs one field read on a code path that only runs in a one-shot CLI.

## Proposed approach

1. Change the `get_topic` contract to surface the response instead of dropping it. Either return the raw `pb.TopicInfo`, or — preferred, to keep protobuf types out of `doctor.py` and keep `mypy --strict` clean without `type: ignore` at the call site — return a small frozen dataclass in `src/sf2loki/salesforce/pubsub_client.py`:

   ```python
   @dataclass(frozen=True, slots=True)
   class TopicProbe:
       topic_name: str
       can_subscribe: bool
       tenant_guid: str
       schema_id: str
   ```

   `async def get_topic(self, topic: str) -> TopicProbe`, built from the `GetTopic` response. Error handling stays exactly as it is (`self._handle_rpc_error(exc)` then re-raise, so the UNAUTHENTICATED token-invalidation behaviour pinned by `tests/salesforce/test_pubsub_client.py:912-928` is unchanged). Update the docstring, which currently documents the discard.

2. In `_check_pubsub` (`src/sf2loki/doctor.py:276-282`), inspect the result:
   - `can_subscribe` true -> `PASS`, `"topic reachable"` (unchanged text, so README's sample output at `README.md:308-320` stays valid).
   - `can_subscribe` false -> `FAIL` with an actionable detail naming the remedy, e.g. `"topic exists but can_subscribe=false - grant the integration user Read on the platform event / check the channel's subscribe permission in the connected app's permission set"`.
   - Leave `can_publish` unused: sf2loki never publishes.

3. Optionally include `tenant_guid` in the PASS detail only when it disagrees with the org id resolved by the `auth` check (`src/sf2loki/doctor.py:149-162`), as a wrong-org guard. Keep this out of scope if it complicates the row text — the `can_subscribe` gate is the substance.

4. Update the two test doubles that implement the old signature: `_FakePubSubClient.get_topic` in `tests/test_doctor.py:105-118` (currently `-> None`, raising `RuntimeError` for a topic containing `"bad"`), and the assertion in `tests/salesforce/test_pubsub_client.py:888-910`.

5. Document the new FAIL row in `docs/troubleshooting.md` alongside the existing doctor rows, and in the doctor section of `docs/reference/cli.md`, with the permission remedy.

No config surface changes, no generated-artifact regeneration (`just gen-config` not required), no proto change (`can_subscribe` is already in the generated stub).

---

Imported from GitHub issue #119 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 119)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `PubSubClient.get_topic` returns the topic's `can_subscribe` (and `tenant_guid`/`schema_id`) rather than `None`; docstring updated to state that callers must check `can_subscribe`.
- [ ] #2 Existing `GetTopic` error semantics unchanged: UNAUTHENTICATED still invalidates the cached token and re-raises; non-auth errors (e.g. NOT_FOUND) still propagate without invalidating.
- [ ] #3 `_check_pubsub` emits `FAIL` for a topic whose `GetTopic` succeeds with `can_subscribe=false`, with a detail naming the permission remedy; `PASS  topic reachable` retained when `can_subscribe=true`.
- [ ] #4 A doctor run containing such a topic exits 1 (`src/sf2loki/doctor.py:818-826`) and the row appears in the `--json` payload with `"status": "FAIL"`.
- [ ] #5 Test in `tests/salesforce/test_pubsub_client.py`: fake servicer returns `pb.TopicInfo(topic_name=..., can_subscribe=False, ...)`; assert `get_topic` returns a result whose `can_subscribe` is `False` (replacing the current returns-`None` assertion).
- [ ] #6 Test in `tests/salesforce/test_pubsub_client.py`: `can_subscribe=True` case returns `True` and still sends the topic name plus auth metadata.
- [ ] #7 Test in `tests/test_doctor.py`: fake `PubSubClient` returns `can_subscribe=False` for one configured topic; assert the resulting `CheckResult` is `("pubsub:<topic>", "FAIL", <detail mentioning the permission remedy>)` and that the overall exit code is 1.
- [ ] #8 Test in `tests/test_doctor.py`: happy path with `can_subscribe=True` still yields `PASS  topic reachable` (regression guard on the unchanged row text).
- [ ] #9 `docs/troubleshooting.md` and `docs/reference/cli.md` describe the new FAIL row and its fix.
- [ ] #10 `just gate` green (ruff, `mypy --strict`, pytest) with no new `type: ignore` at the doctor call site.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
