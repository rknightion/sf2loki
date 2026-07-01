# Changelog

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
