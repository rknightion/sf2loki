#!/usr/bin/env bash
set -euo pipefail

readonly helm_version=v3.21.4
readonly kubeconform_version=v0.8.0
readonly bin=.venv/bin

case "$(uname -m)" in
    x86_64) arch=amd64 ;;
    aarch64) arch=arm64 ;;
    *) echo "Unsupported chart tool architecture: $(uname -m)" >&2; exit 1 ;;
esac

scratch="$(mktemp -d "$bin/.chart-tools.XXXXXX")"
trap 'rm -rf -- "$scratch"' EXIT

download() {
    curl -q --fail --silent --show-error --location --retry 3 \
        --proto '=https' --proto-redir '=https' --output "$scratch/$2" "$1"
}

if [[ ! -x "$bin/helm" || "$("$bin/helm" version --template '{{.Version}}')" != "$helm_version" ]]; then
    archive="helm-${helm_version}-linux-${arch}.tar.gz"
    url="https://get.helm.sh/${archive}"
    download "$url" "$archive"
    download "${url}.sha256sum" helm.sha256sum
    checksum="$(awk 'NR == 1 {print $1}' "$scratch/helm.sha256sum")"
    printf '%s  %s\n' "$checksum" "$scratch/$archive" | sha256sum --check --strict -
    tar -xOzf "$scratch/$archive" "linux-${arch}/helm" > "$scratch/helm"
    chmod 0755 "$scratch/helm"
    mv -- "$scratch/helm" "$bin/helm"
fi

if [[ ! -x "$bin/kubeconform" || "$("$bin/kubeconform" -v)" != "$kubeconform_version" ]]; then
    archive="kubeconform-linux-${arch}.tar.gz"
    url="https://github.com/yannh/kubeconform/releases/download/${kubeconform_version}"
    download "$url/$archive" "$archive"
    download "$url/CHECKSUMS" kubeconform.sha256sum
    checksum="$(awk -v name="$archive" '$2 == name {print $1}' "$scratch/kubeconform.sha256sum")"
    printf '%s  %s\n' "$checksum" "$scratch/$archive" | sha256sum --check --strict -
    tar -xOzf "$scratch/$archive" kubeconform > "$scratch/kubeconform"
    chmod 0755 "$scratch/kubeconform"
    mv -- "$scratch/kubeconform" "$bin/kubeconform"
fi
