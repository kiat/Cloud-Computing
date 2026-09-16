#!/usr/bin/env bash
# Regenerates battleship_pb2.py and battleship_pb2_grpc.py from the .proto file.
set -e
python3 -m grpc_tools.protoc \
  -I proto \
  --python_out=. \
  --grpc_python_out=. \
  proto/battleship.proto
echo "Generated battleship_pb2.py and battleship_pb2_grpc.py"
