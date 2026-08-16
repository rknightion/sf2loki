#!/usr/bin/env bash
# LOCAL AGENTS: If you are not a cloud agent, do not execute this script.
# Manual environment setup for Codex and Claude Code cloud tasks.
# Configure the provider's setup command as: bash scripts/cloud-environment-setup.sh

set -euo pipefail

readonly UV_VERSION="0.12.5"
readonly JUST_VERSION="1.58.0"
readonly BACKLOG_VERSION="1.50.1"
readonly LOCAL_BIN="${HOME}/.local/bin"
readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

export PATH="${LOCAL_BIN}:${PATH}"
mkdir -p "${LOCAL_BIN}"

# Setup and agent phases use separate shells. Persist the user-local bin directory
# so the tools installed here remain available when the agent starts.
readonly PATH_LINE='export PATH="$HOME/.local/bin:$PATH"'
touch "${HOME}/.bashrc"
if ! grep -Fqx "${PATH_LINE}" "${HOME}/.bashrc"; then
  printf '\n%s\n' "${PATH_LINE}" >> "${HOME}/.bashrc"
fi

version_is() {
  local command_name="$1"
  local expected="$2"
  command -v "${command_name}" >/dev/null 2>&1 &&
    "${command_name}" --version 2>/dev/null | awk -v expected="${expected}" \
      '{ for (field = 1; field <= NF; field++) if ($field == expected) found = 1 } END { exit !found }'
}

if ! version_is uv "${UV_VERSION}"; then
  # PyPI is available under both providers' default package-registry policies.
  python3 -m pip install --user --disable-pip-version-check "uv==${UV_VERSION}"
fi

if ! version_is just "${JUST_VERSION}"; then
  # Claude's GitHub proxy rejects release assets from repositories not attached
  # to the session, so build the pinned crate from the allowlisted registry.
  cargo install --locked --root "${HOME}/.local" --version "${JUST_VERSION}" just
fi

if ! version_is backlog "${BACKLOG_VERSION}"; then
  npm install --global --prefix "${HOME}/.local" "backlog.md@${BACKLOG_VERSION}"
fi

cd "${REPO_ROOT}"

# Install the repo-pinned Python and locked default development dependency group.
# Optional runtime backends are deliberately absent: strict mypy checks the guarded
# import paths used when those packages are unavailable.
uv python install "$(<.python-version)"
uv sync --locked

printf '\nCodex cloud environment ready:\n'
uv --version
just --version
backlog --version
uv run python --version
uv run ruff --version
uv run mypy --version
uv run pytest --version
