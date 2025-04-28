#!/bin/bash

MODE=${1:-debug}
BUILD_DIR="build"

if [[ "$MODE" == "clean" ]]; then
  echo "🧹 Cleaning build..."
  rm -rf $BUILD_DIR
  exit 0
fi

echo "🔨 Building in $MODE mode..."

mkdir -p $BUILD_DIR
cmake -S . -B $BUILD_DIR -DCMAKE_BUILD_TYPE=$MODE
cmake --build $BUILD_DIR -j$(nproc)

BIN="$BUILD_DIR/bin/OpenSync"
if [[ -f "$BIN" ]]; then
  echo "🚀 Running OpenSync..."
  $BIN
else
  echo "❌ Binary not found!"
  exit 1
fi

