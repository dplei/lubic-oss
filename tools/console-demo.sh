#!/usr/bin/env bash
# 生成控制台演示页：用假数据起真的总控，抓下页面与接口返回，拼成不连总控、只读的静态页。
# 用法：tools/console-demo.sh <输出目录>，产出 index.html 与 favicon.svg。数据见 crates/controller/src/demo.rs。
set -euo pipefail

if [ "$#" -ne 1 ] || [ -z "${1:-}" ]; then
  echo "用法: $0 <输出目录>" >&2
  exit 2
fi

mkdir -p "$1"
OUT="$(cd "$1" && pwd)"
ROOT="$(git -C "$(dirname "$0")/.." rev-parse --show-toplevel)"
LUBIC_DEMO_OUT="$OUT" cargo test --manifest-path "$ROOT/Cargo.toml" -p lubic-controller demo::console_demo -- --exact
echo "演示页：$OUT/index.html"

CHROME=""
for bin in \
  "$(command -v google-chrome || true)" \
  "$(command -v chromium || true)" \
  "$(command -v chromium-browser || true)" \
  "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  "/Applications/Chromium.app/Contents/MacOS/Chromium"; do
  if [ -n "$bin" ] && [ -x "$bin" ]; then
    CHROME="$bin"
    break
  fi
done

if [ -n "$CHROME" ]; then
  "$CHROME" \
    --headless=new \
    --disable-gpu \
    --hide-scrollbars \
    --window-size=1200,720 \
    --force-device-scale-factor=2 \
    --virtual-time-budget=2000 \
    --screenshot="$OUT/screenshot.png" \
    "file://$OUT/index.html"
  echo "演示截图：$OUT/screenshot.png"
else
  echo "警告：未找到 Chrome/Chromium，跳过生成 $OUT/screenshot.png" >&2
fi
