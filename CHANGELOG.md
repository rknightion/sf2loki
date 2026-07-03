# Changelog

## [1.0.0](https://github.com/rknightion/sf2loki/compare/v0.2.0...v1.0.0) (2026-07-03)


### ⚠ BREAKING CHANGES

* **config:** configs containing unknown keys, generic login_url with auth_mode=client_credentials, or invalid identifiers now fail startup validation. Generated config artifacts regenerated.
* **sink:** per-entry drop accounting moved from sf2loki_loki_push{outcome="dropped"} to sf2loki_loki_entries_dropped{reason}; sink.loki.labels may no longer set source/event_type (startup error). Grafana dashboard regenerated accordingly.

### Features

* **app:** wire ApexLogSource + apexlog checkpoint metric ([a234c34](https://github.com/rknightion/sf2loki/commit/a234c34a1d303f647f512b90a250e563a1a05487))
* **app:** wire multi-key commit, cache reset, async close, epoch fence, entry-cost, concurrent auth probe ([73101d7](https://github.com/rknightion/sf2loki/commit/73101d795416f8a068372e062904fa0c083a9aeb)), closes [#47](https://github.com/rknightion/sf2loki/issues/47) [#48](https://github.com/rknightion/sf2loki/issues/48) [#49](https://github.com/rknightion/sf2loki/issues/49) [#52](https://github.com/rknightion/sf2loki/issues/52) [#54](https://github.com/rknightion/sf2loki/issues/54) [#56](https://github.com/rknightion/sf2loki/issues/56) [#67](https://github.com/rknightion/sf2loki/issues/67) [#68](https://github.com/rknightion/sf2loki/issues/68)
* **cli:** sf2loki backfill — one-shot historical EventLogFile backfill ([df6f2f0](https://github.com/rknightion/sf2loki/commit/df6f2f00e05d6ec9178ab3b3a6c629a41775ba1c)), closes [#23](https://github.com/rknightion/sf2loki/issues/23)
* **cli:** sf2loki doctor — live end-to-end preflight diagnostics ([5a8d26d](https://github.com/rknightion/sf2loki/commit/5a8d26d1f1e3b1e678ff4e228abe480639079b2a)), closes [#22](https://github.com/rknightion/sf2loki/issues/22)
* **cli:** sf2loki state show/set/delete + poison-checkpoint recovery runbook ([62fab37](https://github.com/rknightion/sf2loki/commit/62fab379db8ec037b70df6f29c286b05d0563962)), closes [#63](https://github.com/rknightion/sf2loki/issues/63)
* **config:** add gcs state store + k8s_lease coordinator config seams ([fc55723](https://github.com/rknightion/sf2loki/commit/fc557231a4e8024603d09f816e2256af05c5be48))
* **config:** config and metric surface for operability followups ([5f00bf7](https://github.com/rknightion/sf2loki/commit/5f00bf7c5d66e964e921b80e35a01ca3ce2d4899))
* **config:** eventlog_objects per-object big_object flag ([0678da5](https://github.com/rknightion/sf2loki/commit/0678da5c596e46d930f185aef8d79f43d614fd67))
* **config:** forbid unknown keys, validate identifiers, fail fast on misconfig ([9702558](https://github.com/rknightion/sf2loki/commit/97025580cca4df8ed4bc0fa34937bd6a3980a4c9))
* **config:** sources.apexlog polling source config ([568ade7](https://github.com/rknightion/sf2loki/commit/568ade75d003653382adcad3ff9499347db07566))
* **coordinate:** Kubernetes Lease coordinator backend ([0ec2a18](https://github.com/rknightion/sf2loki/commit/0ec2a181c64d8ff5288ea1a5f38c9d7515f3469d)), closes [#36](https://github.com/rknightion/sf2loki/issues/36)
* **docs:** align docs site with m7kni.io brand + server-side SEO/LLM metadata ([4bf82de](https://github.com/rknightion/sf2loki/commit/4bf82de9f83813347679ebca4cc193e44e1344eb)), closes [#76](https://github.com/rknightion/sf2loki/issues/76)
* **doctor:** probe the configured state/OTLP/coordinator + unsalted-hash warn ([1735bef](https://github.com/rknightion/sf2loki/commit/1735bef120bb796c77d3584386cb4aac23e1e9d3)), closes [#59](https://github.com/rknightion/sf2loki/issues/59)
* **doctor:** warn when apexlog enabled but no active TraceFlags ([a861fc9](https://github.com/rknightion/sf2loki/commit/a861fc94ed4b96d7a547586651ff26f32db54c1e))
* **eventlog_objects:** abort the big-object drain promptly on shutdown ([1ac5b3a](https://github.com/rknightion/sf2loki/commit/1ac5b3a5af410f50a1e2ba58171538b3e1e5dd47)), closes [#34](https://github.com/rknightion/sf2loki/issues/34)
* **eventlog_objects:** big_object DESC descending-drain query mode ([f39a465](https://github.com/rknightion/sf2loki/commit/f39a4654425f46880f099917ec3321b20c969b53)), closes [#34](https://github.com/rknightion/sf2loki/issues/34)
* **eventlog_objects:** big-object hint, preset, docs ([14ddaa8](https://github.com/rknightion/sf2loki/commit/14ddaa83838338b2c4ec0856e5100189b669adbc)), closes [#34](https://github.com/rknightion/sf2loki/issues/34)
* freeze wave-1 seams — transforms/sampling/egress config, new metrics, doctor+backfill CLI stubs ([72a079e](https://github.com/rknightion/sf2loki/commit/72a079eb303c77c5f5f2eaffc0bdc359f7825694))
* freeze wave-2 seams — coordinate/state config, state-store factory, ELF concurrency knob ([69e446c](https://github.com/rknightion/sf2loki/commit/69e446cb76ae830edb9c0701fdd5d373b7df19e6))
* **grafana:** add leader-count anomaly alert (leaderless gap / split-brain) ([da2742f](https://github.com/rknightion/sf2loki/commit/da2742f6cd3e6120d6f8801249492e62b69cf3e1))
* **grafana:** hand-authored v2 dashboard suite + Grafana-managed rule pack ([9df258b](https://github.com/rknightion/sf2loki/commit/9df258befb4f7893741207898485a87b72c5427c)), closes [#58](https://github.com/rknightion/sf2loki/issues/58)
* **ha:** active-passive failover via a file-lease coordinator with commit fencing ([6fa5a54](https://github.com/rknightion/sf2loki/commit/6fa5a541266918723a87052f5dfb26e84e3f2169)), closes [#29](https://github.com/rknightion/sf2loki/issues/29)
* **helm:** production Helm chart, published as an OCI chart; drop deploy/k8s examples ([06a2a82](https://github.com/rknightion/sf2loki/commit/06a2a82f66561ed625aed99d4cc609ef89dd27cf)), closes [#73](https://github.com/rknightion/sf2loki/issues/73) [#74](https://github.com/rknightion/sf2loki/issues/74) [#75](https://github.com/rknightion/sf2loki/issues/75)
* **metrics:** apexlog ingest/download counters ([2657406](https://github.com/rknightion/sf2loki/commit/2657406ff97a44af9967814aa8c4814be7db69ed))
* multi-org ingestion from a single process ([57b314a](https://github.com/rknightion/sf2loki/commit/57b314a8e1e69012b37ffde6842763f0d0607add)), closes [#31](https://github.com/rknightion/sf2loki/issues/31)
* **obs:** byte-aware queue bounding and liveness-derived readiness ([2664aaa](https://github.com/rknightion/sf2loki/commit/2664aaa867cdb55670070de3016898be55181e6f)), closes [#16](https://github.com/rknightion/sf2loki/issues/16) [#17](https://github.com/rknightion/sf2loki/issues/17)
* **obs:** generated Grafana alert-rule pack + histogram lag panels ([1920764](https://github.com/rknightion/sf2loki/commit/1920764932c95538b3d59d73869b3ff2f83e2756)), closes [#28](https://github.com/rknightion/sf2loki/issues/28)
* **pipeline:** per-lane queues so bulk can't starve streaming ([33b579f](https://github.com/rknightion/sf2loki/commit/33b579fa271139fe366212e82f8c84529d780d86)), closes [#53](https://github.com/rknightion/sf2loki/issues/53)
* publish to PyPI via trusted publishing (pip/pipx/uv install path) ([0fa7f99](https://github.com/rknightion/sf2loki/commit/0fa7f99beb9e90955e8df2c5ac58d194fcb332aa)), closes [#32](https://github.com/rknightion/sf2loki/issues/32)
* **pubsub:** periodic wildcard re-discovery and runtime overlap filtering ([dd617c9](https://github.com/rknightion/sf2loki/commit/dd617c96895e5080811f8c65d0161ccd231aa9d5)), closes [#14](https://github.com/rknightion/sf2loki/issues/14) [#15](https://github.com/rknightion/sf2loki/issues/15)
* **salesforce:** ApexLogClient (tooling listing + body download) ([fb1c7d3](https://github.com/rknightion/sf2loki/commit/fb1c7d30a86b999b749c3f668faabd9f84307e73))
* **sink:** egress guardrails — rate caps + persisted daily byte budget ([1fa4de7](https://github.com/rknightion/sf2loki/commit/1fa4de791d6a230f457a583f0c53e53ebad8b614)), closes [#26](https://github.com/rknightion/sf2loki/issues/26)
* **soql:** tooling-query mode for the Tooling API ([a9c0185](https://github.com/rknightion/sf2loki/commit/a9c018519fae55c05c596eff62a941054c9f9fac))
* **sources:** ApexLogSource — Tooling API debug-log polling ([467d217](https://github.com/rknightion/sf2loki/commit/467d217be01c298fb3cdeaf099e80bdc0c46716e))
* **sources:** declarative PII transforms + deterministic sampling ([68e7356](https://github.com/rknightion/sf2loki/commit/68e7356492ffb48845694d3f29ded4f6ca7cc088)), closes [#27](https://github.com/rknightion/sf2loki/issues/27)
* **sources:** per-object poll timers, poll-error metrics, deterministic timestamps, hardening ([d802659](https://github.com/rknightion/sf2loki/commit/d8026598be4cf31f93f33325953d480d4f447167)), closes [#18](https://github.com/rknightion/sf2loki/issues/18) [#19](https://github.com/rknightion/sf2loki/issues/19) [#20](https://github.com/rknightion/sf2loki/issues/20) [#21](https://github.com/rknightion/sf2loki/issues/21)
* **state:** bounded retry on transient object-store errors ([b7dc419](https://github.com/rknightion/sf2loki/commit/b7dc419503b64668bf96c1cfb93c9f6838b85da0)), closes [#44](https://github.com/rknightion/sf2loki/issues/44)
* **state:** GCS checkpoint store backend ([e1df40a](https://github.com/rknightion/sf2loki/commit/e1df40ae936be6e200ed737bcaef93207556ab42)), closes [#37](https://github.com/rknightion/sf2loki/issues/37)
* **state:** S3-compatible checkpoint store for stateless deployments ([07ed785](https://github.com/rknightion/sf2loki/commit/07ed785f3e63bb203b89d1350cf2e27144f1d42b)), closes [#30](https://github.com/rknightion/sf2loki/issues/30)
* wire gcs store + k8s_lease coordinator into the composition root ([7ae8672](https://github.com/rknightion/sf2loki/commit/7ae8672cb2e75e7fc2a1102ea47b194f6857af5b))


### Bug Fixes

* **apexlog:** compound (StartTime, Id) cursor + checkpoint-only tokens ([4538201](https://github.com/rknightion/sf2loki/commit/45382016c21008fde9e3245c231cedddacf7a805)), closes [#39](https://github.com/rknightion/sf2loki/issues/39)
* **backfill:** namespace the checkpoint key per org ([4bd1b0c](https://github.com/rknightion/sf2loki/commit/4bd1b0c85afacfd82b9fd9b28d28c8dfe59f7635)), closes [#40](https://github.com/rknightion/sf2loki/issues/40)
* **config:** use a pollable LoginHistory example in the docker eventlog_objects block ([b3d3f72](https://github.com/rknightion/sf2loki/commit/b3d3f72f09c9a9879acaaa086bcfca659eabd49d))
* **coordinate:** file lease contests on unreadable lease + durable epoch token ([6e6ec77](https://github.com/rknightion/sf2loki/commit/6e6ec7759f241fe6b276d4ba77802856d8da7ba4)), closes [#50](https://github.com/rknightion/sf2loki/issues/50)
* **coordinate:** k8s lease observedTime staleness + null-field tolerance ([da39549](https://github.com/rknightion/sf2loki/commit/da39549d92346ae590e8a0097c98469672bea02f)), closes [#51](https://github.com/rknightion/sf2loki/issues/51) [#62](https://github.com/rknightion/sf2loki/issues/62)
* **coordinate:** surrender k8s leadership when the Lease is deleted ([bb00032](https://github.com/rknightion/sf2loki/commit/bb000329c71781df47b0404dbfb2989147bf5518)), closes [#37](https://github.com/rknightion/sf2loki/issues/37) [#36](https://github.com/rknightion/sf2loki/issues/36)
* **eventlog_objects:** compound cursor, bounded catch-up drain, checkpoint tokens ([40496fb](https://github.com/rknightion/sf2loki/commit/40496fbcc196b19e88729ec08ca4bad69c2884db)), closes [#38](https://github.com/rknightion/sf2loki/issues/38) [#46](https://github.com/rknightion/sf2loki/issues/46)
* **eventlog_objects:** dedup big-object boundary record across poll cycles ([2a7a8c3](https://github.com/rknightion/sf2loki/commit/2a7a8c3cb8c13b5aafbc7f287724447c27b92798)), closes [#34](https://github.com/rknightion/sf2loki/issues/34)
* **eventlogfile:** csv field cap, mid-file abandon, per-request clock skew ([bf2f0ff](https://github.com/rknightion/sf2loki/commit/bf2f0ff9fd49a77a5909eff5a0dee72b9c1518bf)), closes [#41](https://github.com/rknightion/sf2loki/issues/41) [#64](https://github.com/rknightion/sf2loki/issues/64) [#66](https://github.com/rknightion/sf2loki/issues/66)
* **grafana:** don't show Loki push success rate as 0% when idle ([f53a5e9](https://github.com/rknightion/sf2loki/commit/f53a5e9b4d2a7de230be8ef893a81d4d467f3056))
* **pubsub:** deterministic timestamp, in-loop checkpoint load, trailer-gated redrain, decode visibility ([7397e5a](https://github.com/rknightion/sf2loki/commit/7397e5a3539ff6a49d61a4d4706f9cace7a734e1)), closes [#42](https://github.com/rknightion/sf2loki/issues/42) [#43](https://github.com/rknightion/sf2loki/issues/43) [#45](https://github.com/rknightion/sf2loki/issues/45) [#65](https://github.com/rknightion/sf2loki/issues/65)
* **pubsub:** self-heal expired replay ids, checkpoint keepalives, detect dead streams ([6cc9946](https://github.com/rknightion/sf2loki/commit/6cc994677d74e5255e211f593ea6226448e77d49))
* **sink:** retry Loki auth errors instead of dropping; crash on consumer death ([8223fb3](https://github.com/rknightion/sf2loki/commit/8223fb39f0a6397bdb21d0808e518ec80a5eb7cf))
* **sources:** stop losing ELF files to transient failures; stream CSVs; harden polling and state ([94e7521](https://github.com/rknightion/sf2loki/commit/94e7521190d0598cf03daa812cb38d68647b2627))


### Performance Improvements

* **eventlogfile:** bounded-concurrency per-cycle type processing ([9c9a821](https://github.com/rknightion/sf2loki/commit/9c9a821673f175b6950f2d3c66c91c53396c6601)), closes [#25](https://github.com/rknightion/sf2loki/issues/25)
* **loki:** offload batch encode to a thread above a size threshold ([8ec2ae8](https://github.com/rknightion/sf2loki/commit/8ec2ae831fda1f2a98874585c3e72cb18045ecf6)), closes [#55](https://github.com/rknightion/sf2loki/issues/55)
* **pipeline:** encode each line's UTF-8 length once on the hot path ([98868dd](https://github.com/rknightion/sf2loki/commit/98868dd61e1a177a44b337e845237640efdfff5c)), closes [#69](https://github.com/rknightion/sf2loki/issues/69)


### Code Refactoring

* **coordinate:** thread acquired k8s lease into _hold, drop redundant read ([e1dddeb](https://github.com/rknightion/sf2loki/commit/e1dddebd15b5d190672254262786981213ebfc23)), closes [#36](https://github.com/rknightion/sf2loki/issues/36)
* **eventlog_objects:** extract shared _emit_record helper ([d7bcddf](https://github.com/rknightion/sf2loki/commit/d7bcddf10b7d041c2b64495edbd527062354096f))


### Documentation

* add CLAUDE.md with git/commit + issue-reference conventions ([acdeed7](https://github.com/rknightion/sf2loki/commit/acdeed79558cd6c6c0888a215d98b4c66653bf70))
* **apexlog:** document the ApexLog source, prerequisites, and API cost ([393faeb](https://github.com/rknightion/sf2loki/commit/393faebfc21c19ceb21302735487d30bc6b797d0))
* author all v1 content pages across the docs site ([4428eec](https://github.com/rknightion/sf2loki/commit/4428eec8546c6e52661fae5a5fe29f3300179812))
* **deploy:** released-tag default, k8s manifests, grace alignment, hardening, DESIGN refresh ([c0c9bba](https://github.com/rknightion/sf2loki/commit/c0c9bbae445d37155deedb0aee858a736bfa06bc)), closes [#57](https://github.com/rknightion/sf2loki/issues/57) [#60](https://github.com/rknightion/sf2loki/issues/60) [#61](https://github.com/rknightion/sf2loki/issues/61) [#72](https://github.com/rknightion/sf2loki/issues/72)
* doctor/backfill quickstart, PII+sampling and cost-control guides, alert pack reference ([1e1fad0](https://github.com/rknightion/sf2loki/commit/1e1fad0855b2152386aef4c6fef2767950c8e259))
* **eventlog_objects:** correct stale big-object guidance, note drain shutdown latency ([be28a74](https://github.com/rknightion/sf2loki/commit/be28a742f1e9b6032069614c310b3a358073c0df)), closes [#34](https://github.com/rknightion/sf2loki/issues/34)
* expand CLAUDE.md coverage with per-module files + AGENTS.md symlinks ([d7ced9a](https://github.com/rknightion/sf2loki/commit/d7ced9a355ffea06e25b91797a4e4779a82b3146))
* first-class guidance + preset for custom platform events and CDC ([6198fe3](https://github.com/rknightion/sf2loki/commit/6198fe325fcfad1a98a2ee204d9d22e6200716f4)), closes [#35](https://github.com/rknightion/sf2loki/issues/35)
* GCS checkpoint store + Kubernetes Lease coordinator ([02978a3](https://github.com/rknightion/sf2loki/commit/02978a321aa8f9900839b63be39f2cb9a6f6abba))
* **geo:** content-shape pass for LLM/search retrievability ([c85c5fb](https://github.com/rknightion/sf2loki/commit/c85c5fbd541327cfd2aa17a9ca17a547eb0c3a72))
* **presets:** align event-log-objects preset with the fragment convention ([b03ede6](https://github.com/rknightion/sf2loki/commit/b03ede611f0e30f109ab7550df5b944f2b8824cb)), closes [#34](https://github.com/rknightion/sf2loki/issues/34)
* push to main unprompted (align with global issue-workflow rule) ([547466e](https://github.com/rknightion/sf2loki/commit/547466e4f71473f2552eb428c4da9ebdc2105407))
* **readme:** high-availability and S3 checkpoint store sections ([8b6f52d](https://github.com/rknightion/sf2loki/commit/8b6f52d26e2d13dd017797d70d832a467977fc10))
* record GitHub issues as the source of truth for repo work ([c7d5e97](https://github.com/rknightion/sf2loki/commit/c7d5e977b836e84c5f95a2eb23819449088393de))
* retire DESIGN.md + flat docs, rewire links, add docs-sync workflow ([7c76654](https://github.com/rknightion/sf2loki/commit/7c766548db0893773cee638a4f9990326edd54a6))
* scaffold zensical documentation site (frozen nav seam) ([5138ee1](https://github.com/rknightion/sf2loki/commit/5138ee1bbede2b808ac4795d3d7780b3a82b9667))


### Build & CI

* **deps:** lock gcs + k8s optional extras ([8dfa8b0](https://github.com/rknightion/sf2loki/commit/8dfa8b034e8553de50393dbf89cac80d65536e52))

## [0.2.0](https://github.com/rknightion/sf2loki/compare/v0.1.0...v0.2.0) (2026-07-01)


### Features

* **app:** pipeline + composition root + CLI (T11) ([09dc1bd](https://github.com/rknightion/sf2loki/commit/09dc1bd6823673d6f8723331f4d2664e1ac12b52))
* **auth:** JWT bearer TokenProvider (T2) ([07107f2](https://github.com/rknightion/sf2loki/commit/07107f2e1305c44b9ed22c43d572b743d109b9cc))
* **auth:** OAuth client_credentials mode + sandbox/production toggle ([6bfbccd](https://github.com/rknightion/sf2loki/commit/6bfbccd1b55e66a971648e1029385a01a5f162f0))
* **cli:** --check flag for offline config + wiring validation ([5f63dea](https://github.com/rknightion/sf2loki/commit/5f63dea1833eaf99dea222f2fc8ad41dc4fae896))
* **cli:** add 'config example|reference|schema' subcommand ([6c5dac5](https://github.com/rknightion/sf2loki/commit/6c5dac5f1ea4a0991540a90216ba09a5224bedeb))
* **configdoc:** generate annotated example YAML from the model ([a54ef77](https://github.com/rknightion/sf2loki/commit/a54ef77095f0937e2a5a944a7f00499fb7b69746))
* **configdoc:** generate markdown reference + json schema ([474b620](https://github.com/rknightion/sf2loki/commit/474b620545d4635e8bcebc21fb481d69a8b98fce))
* **config:** generate example + reference from schema; add drift gate ([d733f7e](https://github.com/rknightion/sf2loki/commit/d733f7e3dbb9b9026f79ad81cd6386977f96ee41))
* **deploy:** enable org-limits metric poller in docker config ([3149d9b](https://github.com/rknightion/sf2loki/commit/3149d9b13d1790ba79265945bc6ed5bfd326d800))
* **deploy:** persist checkpoint to a bind-mounted ./state volume ([e78bf4a](https://github.com/rknightion/sf2loki/commit/e78bf4adcfee8805180cb617116a2b98ef3ce9dd))
* **docker:** docker-compose run path + readiness-based healthcheck ([9ce8836](https://github.com/rknightion/sf2loki/commit/9ce8836a2faa4a49d986efa2dca907e4c9095c20))
* **eventlogfile:** hourly-ELF resiliency hardening + close ko.md gaps ([4559c00](https://github.com/rknightion/sf2loki/commit/4559c001b1ff0de24536114c9584f51fc460df34))
* **labels:** always emit job + service_name, auto-derive environment ([e9b0b86](https://github.com/rknightion/sf2loki/commit/e9b0b8653e645ad544cefe352f307b650b9004d3))
* **loki:** label guard + protobuf/json encoders (T5) ([ae98268](https://github.com/rknightion/sf2loki/commit/ae98268f19ccd3ebc8ec83d053a73ab21a49dab3))
* **loki:** LokiSink HTTP push with retry + 413 split (T6) ([e7b6526](https://github.com/rknightion/sf2loki/commit/e7b65263e85f4ee93b33d4a91f1e79a9ec054cc4))
* **obs:** metrics, health, structured logging (T8) ([8089fa4](https://github.com/rknightion/sf2loki/commit/8089fa442c39775f77ee0e02f08e0c6869d571ef))
* **obs:** OTel-native metrics via OTLP + Salesforce org-limit metrics ([d2bfbdb](https://github.com/rknightion/sf2loki/commit/d2bfbdb5ad178496160b8863e31065b3f6f40496))
* **obs:** push metrics to Grafana Cloud over OTLP by default ([7a84ee1](https://github.com/rknightion/sf2loki/commit/7a84ee11a1e66e5c05ffd53bf0bf5c9e342caf68))
* **obs:** startup banner + pubsub/pipeline operational logging ([6306905](https://github.com/rknightion/sf2loki/commit/6306905ca2e016f2956c88ab3d9e7bf42fe79232))
* **obs:** wire the remaining unwired metrics through ([ef8482a](https://github.com/rknightion/sf2loki/commit/ef8482a01d9dc3493794eb57543289afd180c233))
* **packaging:** Dockerfile, k8s manifests, CI, docs (T12) ([34c9d16](https://github.com/rknightion/sf2loki/commit/34c9d16132a05155daf3f2f2c47e235dbd80c9a2))
* project skeleton, frozen seams, proto stubs, config (T1) ([d92f25c](https://github.com/rknightion/sf2loki/commit/d92f25c50e97068f85619dbbcc77ae472859e651))
* **salesforce:** Pub/Sub gRPC client + Avro codec (T3) ([f62a074](https://github.com/rknightion/sf2loki/commit/f62a074d2dadc279b9af81422f42febad1244b28))
* **shaping:** derive a per-event log level as structured metadata ([e44e6cf](https://github.com/rknightion/sf2loki/commit/e44e6cfd161c2d4d24668f5fb10bf7e44a4aa92c))
* **sources:** auto-discover Pub/Sub streams; precedence-aware ELF overlap ([18da0d5](https://github.com/rknightion/sf2loki/commit/18da0d507c8505c9b07e52cf0febf4dbf6618730))
* **sources:** discover EventLogFile EventTypes with a "*" wildcard ([bc2c29e](https://github.com/rknightion/sf2loki/commit/bc2c29e08201c950f2b3f21e06b5b735a7c6c679))
* **sources:** EventLogFile ingestion (Phase 3) + either/or docs ([b425acc](https://github.com/rknightion/sf2loki/commit/b425acc91d0b67602c8479e1d4df8f84339ec175))
* **sources:** EventLogFile source stub (T10) ([7ca6b45](https://github.com/rknightion/sf2loki/commit/7ca6b45fbd89811ffd1cbd3d7965dffc7821fd1e))
* **sources:** ingest a broad EventLogFile mix on the Daily interval ([9c35aa2](https://github.com/rknightion/sf2loki/commit/9c35aa2553849716614ef2aec5ef5dabd880cc5c))
* **sources:** per-event-type ELF routing + Loki per-line byte cap ([40569c2](https://github.com/rknightion/sf2loki/commit/40569c2702b8af054926168a5d7699e2aa08cc61))
* **sources:** Phase 2/3 seams — overlap guard, ELF config/metrics, timestamp + sink hardening ([dff7982](https://github.com/rknightion/sf2loki/commit/dff7982956270d937e53f0baf9c56d92a8576ffe))
* **sources:** Pub/Sub streaming source (T4) ([9171a02](https://github.com/rknightion/sf2loki/commit/9171a0261bb059c484c2f490f58e96520f4a3601))
* **sources:** SOQL client + eventlog_objects source (T9) ([856f04e](https://github.com/rknightion/sf2loki/commit/856f04ef6c11d50f886e5157482baa043aa5b859))
* **state:** file + configmap checkpoint stores (T7) ([0b34521](https://github.com/rknightion/sf2loki/commit/0b3452195e70a26036ca47a800a4f95bf3790699))
* **tooling:** broaden generator API/EventLogFile coverage ([0c31277](https://github.com/rknightion/sf2loki/commit/0c3127759f836c0c30f9501cfbed631533279358))
* **tooling:** CSV-driven diverse data + reliable generator cleanup ([3a01846](https://github.com/rknightion/sf2loki/commit/3a01846ff837ef28cdbeca81b3412347604d85a4))


### Bug Fixes

* **app:** bound graceful shutdown to shutdown_grace ([32b868e](https://github.com/rknightion/sf2loki/commit/32b868ef57bbe8ee32adcfabcae8fc845cd1c922))
* **configdoc:** comment out inline secrets in example so *_file isn't shadowed ([bc81d6d](https://github.com/rknightion/sf2loki/commit/bc81d6db7679e1edc9ce2095623d6333b7c41c4f))
* **configdoc:** mark required secret leaves; add regression tests for list/alias/undefined rendering ([1d3977b](https://github.com/rknightion/sf2loki/commit/1d3977b1e5f4720c20c914206bc54e9b86860899))
* **config:** duration shorthand + ${ENV} interpolation (DESIGN §11) ([148c5a4](https://github.com/rknightion/sf2loki/commit/148c5a43efedd372d8d469dfbd30dfecf8dbdc77))
* **pubsub:** create gRPC channel lazily inside the running event loop ([69c1150](https://github.com/rknightion/sf2loki/commit/69c1150997bc28cf0388405086fb79827740908c))
* **pubsub:** invalidate token on UNAUTHENTICATED so streams self-heal ([43c6119](https://github.com/rknightion/sf2loki/commit/43c61193f9c6f9e93418e59ef9d2d8e5d6366f37))
* **pubsub:** use ChangeEventHeader.commitTimestamp for CDC entry timestamp ([575e8a1](https://github.com/rknightion/sf2loki/commit/575e8a1194d4e8705f813b1901b4f0ad6899dffc))
* **sources:** eventlog_objects entry timestamp uses configured timestamp_field ([561946c](https://github.com/rknightion/sf2loki/commit/561946c640eb6593ec09a320a7e84741ea2fe4df))
* **sources:** ingest EventLogFile on the Hourly interval, not Daily ([faac02a](https://github.com/rknightion/sf2loki/commit/faac02ae13982fb3d4f84a834f84909ca33cc87b))
* **sources:** SOQL-safe datetime literals + dropped-metric count (review) ([0105924](https://github.com/rknightion/sf2loki/commit/0105924a1fa2476c4446ef9624206bcd077e9d6a))


### Code Refactoring

* **config:** make field docs machine-readable via Field(description) ([ca4b6e2](https://github.com/rknightion/sf2loki/commit/ca4b6e2ba867cbcb2fe930552e683971d598ca5f))
* **state:** drop Kubernetes/ConfigMap backend, file store only ([ec11398](https://github.com/rknightion/sf2loki/commit/ec113989785a4a07da2abdcb3dfac1d3f8793b96))


### Documentation

* add Phase 0 design (DESIGN.md) ([39e7cc3](https://github.com/rknightion/sf2loki/commit/39e7cc3851df0451c0407b8135445327f8df3612))
* **config:** document generated config reference + drift gate ([50f60e5](https://github.com/rknightion/sf2loki/commit/50f60e5fbee1d1ff3fd37e6fcf9b53c6264563d5))
* correct OAuth scopes — refresh_token required for pre-authorized JWT bearer ([b0c9d51](https://github.com/rknightion/sf2loki/commit/b0c9d514dfbadcdef5e7317c6abf9028125e6a49))
* dedupe to single source of truth + add generator doc ([0d0b931](https://github.com/rknightion/sf2loki/commit/0d0b931167d607516d7ccd7ae7ef1722e3769267))
* **readme:** full External Client App + JWT bearer setup walkthrough ([a9c9d8d](https://github.com/rknightion/sf2loki/commit/a9c9d8d76c30c5d6e37e7d71207f9ae1135f79d5))


### Build & CI

* **docker:** pull the GHCR :main image by default ([26aea13](https://github.com/rknightion/sf2loki/commit/26aea1370b4d21aa4169dff98256fc87e6b05403))
