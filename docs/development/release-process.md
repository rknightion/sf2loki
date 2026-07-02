# Release Process

Releases are automated with **release-please** and GitHub Actions — there is no manual version
bump or tag step.

## How releases work

1. **release-please runs on every push to `main`** (workflow:
   [`.github/workflows/release-please.yml`](https://github.com/rknightion/sf2loki/blob/main/.github/workflows/release-please.yml))
   and opens/updates a release PR based on Conventional Commits since the last release.
2. The release PR updates:
   - `CHANGELOG.md`
   - `pyproject.toml` (version bump)
   - `.release-please-manifest.json` (current released version)
3. **Merging the release PR** creates a Git tag (format `vX.Y.Z` — `include-v-in-tag: true`) and a
   GitHub Release.
4. Two follow-on jobs are gated on `release_created` so only one publish path runs per push:
   - **`docker-release`** (release only) — builds, signs, and publishes the multi-arch container
     image to `ghcr.io/rknightion/sf2loki` with the new semver tag plus `:latest`, via the local
     reusable `docker.yml` workflow.
   - **`pypi-release`** (release only) — builds an sdist + wheel with `uv build`, verifies dist
     contents, and publishes to PyPI via **Trusted Publishing (OIDC)** — no stored token; the
     publisher is registered on PyPI for this repo, the `release-please.yml` workflow, and the
     `pypi` GitHub environment.
   - An ordinary push to `main` that doesn't trigger a release instead publishes a rolling `:main`
     edge image (see [Installation](../installation.md#docker-docker-compose)) — no PyPI publish
     happens for edge pushes.

## Conventional commit types → changelog sections

Configured in [`release-please-config.json`](https://github.com/rknightion/sf2loki/blob/main/release-please-config.json):

| Commit type | Changelog section | Visible? |
|---|---|---|
| `feat` | Features | yes |
| `fix` | Bug Fixes | yes |
| `perf` | Performance Improvements | yes |
| `refactor` | Code Refactoring | yes |
| `deps` | Dependencies | yes |
| `docs` | Documentation | yes |
| `build` | Build & CI | yes |
| `chore` | Miscellaneous | hidden |
| `ci` | CI/CD | hidden |
| `test` | Tests | hidden |
| `style` | Styles | hidden |

A `feat!:` prefix or a `BREAKING CHANGE:` footer on any commit type triggers a major version bump
regardless of the type's normal section.

## Manual trigger

The **Release Please** workflow also runs on `workflow_dispatch`, so you can open or refresh the
release PR by hand from the Actions tab without waiting for the next push to `main`.

## Notes

- Release-please's own commit/PR author needs a PAT (`RELEASE_PLEASE_TOKEN`), not the default
  `GITHUB_TOKEN` — a bot-authored release PR needs a non-default-token author for CI to run without
  manual approval.
- Configuration lives in `release-please-config.json`; the manifest tracking the current released
  version is `.release-please-manifest.json`. Avoid hand-editing either — let the release-please
  flow keep tags, the manifest, and the changelog consistent.
- See [Security](../security.md) for how release artifacts (container image, PyPI package) relate
  to the project's supported-versions policy.
