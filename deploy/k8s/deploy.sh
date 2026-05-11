#!/usr/bin/env bash
# One-shot: build runtime image, optionally push, apply Kustomize overlay.
#
# Usage:
#   ./deploy.sh                      # build + kubectl apply (local image name yikv-server:latest)
#   REGISTRY=myreg.io ./deploy.sh    # also docker push; image = myreg.io/yikv-server:${TAG:-latest}
#   PUSH=1 ./deploy.sh               # push even if REGISTRY is empty (uses IMAGE_NAME:TAG)
#   KIND_LOAD=1 ./deploy.sh          # kind load docker-image (after build)
#
# Build context = parent of yikv-server (sibling yikv/). Run kubectl against a cluster with default StorageClass.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
YIKV_SERVER_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REPO_ROOT="$(cd "${YIKV_SERVER_ROOT}/.." && pwd)"

IMAGE_NAME="${IMAGE_NAME:-yikv-server}"
TAG="${TAG:-latest}"
REGISTRY="${REGISTRY:-}"
DOCKERFILE="${DOCKERFILE:-${YIKV_SERVER_ROOT}/deploy/docker/Dockerfile}"
OVERLAY="${OVERLAY:-${YIKV_SERVER_ROOT}/deploy/k8s/overlays/prod}"

if [[ -n "${REGISTRY}" ]]; then
  FULL_IMAGE="${REGISTRY%/}/${IMAGE_NAME}:${TAG}"
else
  FULL_IMAGE="${IMAGE_NAME}:${TAG}"
fi

echo "==> docker build ${FULL_IMAGE} (context: ${REPO_ROOT})"
docker build -f "${DOCKERFILE}" -t "${FULL_IMAGE}" "${REPO_ROOT}"

if [[ "${PUSH:-0}" == "1" ]] || [[ -n "${REGISTRY}" ]]; then
  echo "==> docker push ${FULL_IMAGE}"
  docker push "${FULL_IMAGE}"
fi

if [[ "${KIND_LOAD:-0}" == "1" ]]; then
  echo "==> kind load docker-image ${FULL_IMAGE}"
  kind load docker-image "${FULL_IMAGE}"
fi

TMP_KUSTOMIZE="$(mktemp -d)"
cleanup() { rm -rf "${TMP_KUSTOMIZE}"; }
trap cleanup EXIT

# Preserve k8s/base and k8s/overlays/... layout so ../../base in overlay still resolves.
K8S_WORKTREE="${TMP_KUSTOMIZE}/k8s"
mkdir -p "${K8S_WORKTREE}/overlays"
cp -a "${YIKV_SERVER_ROOT}/deploy/k8s/base" "${K8S_WORKTREE}/base"
cp -a "${OVERLAY}/." "${K8S_WORKTREE}/overlays/prod"

export KUSTOMIZE_PATCH_DIR="${K8S_WORKTREE}/overlays/prod"
export TMP_KUSTOMIZE
export IMG_REPO="${FULL_IMAGE%:*}"
export IMG_TAG="${FULL_IMAGE##*:}"

python3 <<'PY'
import os
import pathlib
import re

tmp = os.environ["KUSTOMIZE_PATCH_DIR"]
repo = os.environ["IMG_REPO"]
tag = os.environ["IMG_TAG"]
kf = pathlib.Path(tmp) / "kustomization.yaml"
text = kf.read_text()
text = re.sub(r"(?m)^(\s*newName:)\s*\S+.*$", rf"\1 {repo}", text, count=1)
text = re.sub(r"(?m)^(\s*newTag:)\s*\S+.*$", rf"\1 {tag}", text, count=1)
kf.write_text(text)
PY

echo "==> kubectl apply -k ${K8S_WORKTREE}/overlays/prod"
kubectl apply -k "${K8S_WORKTREE}/overlays/prod"
kubectl rollout status deployment/yikv-server -n yikv --timeout=600s

echo "==> ok: rpc at yikv-server.yikv.svc.cluster.local:9000"
