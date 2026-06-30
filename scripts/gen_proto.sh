#!/usr/bin/env bash
# Generate gRPC/protobuf stubs from the vendored protos into committed
# _generated/ packages. Run via `just proto`. The generated files are
# committed so the runtime image needs no codegen step.
set -euo pipefail

cd "$(dirname "$0")/.."

SF=src/sf2loki/salesforce/_generated
LK=src/sf2loki/sinks/loki/_generated
mkdir -p "$SF" "$LK"
touch "$SF/__init__.py" "$LK/__init__.py"

uv run python -m grpc_tools.protoc -Iproto \
  --python_out="$SF" --grpc_python_out="$SF" \
  proto/pubsub_api.proto

uv run python -m grpc_tools.protoc -Iproto \
  --python_out="$LK" \
  proto/loki_push.proto

# protoc emits a top-level `import pubsub_api_pb2` in the *_grpc.py file, which
# is not importable from inside a package. Rewrite it to a package-relative
# import (portable across GNU/BSD sed).
sed -i.bak 's/^import pubsub_api_pb2/from . import pubsub_api_pb2/' \
  "$SF/pubsub_api_pb2_grpc.py"
rm -f "$SF"/*.bak

echo "generated stubs in $SF and $LK"
