#!/usr/bin/env bash
# 检测控制台 UI 或演示数据是否发生变化，并在需要时自动同步 docs/demo/。
# 用法：
#   tools/sync-demo.sh [--check] [--base <git-ref>]
#     --check        仅检查（CI/门禁模式）：若 UI 有改动但 docs/demo/index.html 未同步更新则失败退出
#     --base <ref>   指定对比基线，默认自动探测 origin/main 或 main
set -euo pipefail

MODE="sync"
BASE_REF=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --check)
      MODE="check"
      shift
      ;;
    --base)
      if [ "$#" -lt 2 ] || [ -z "${2:-}" ]; then
        echo "错误：--base 需要指定分支或提交" >&2
        exit 2
      fi
      BASE_REF="$2"
      shift 2
      ;;
    -h|--help)
      sed -n '2,6p' "$0" | sed 's/^# \?//'
      exit 0
      ;;
    *)
      echo "未知参数：$1" >&2
      exit 2
      ;;
  esac
done

ROOT="$(git -C "$(dirname "$0")/.." rev-parse --show-toplevel)"
cd "$ROOT"

if [ -z "$BASE_REF" ]; then
  if git rev-parse --verify --quiet origin/main >/dev/null; then
    BASE_REF="origin/main"
  elif git rev-parse --verify --quiet main >/dev/null; then
    BASE_REF="main"
  else
    BASE_REF="HEAD~1"
  fi
fi

# 收集基线以来的提交改动 + 当前工作区未提交改动
CHANGED_FILES="$(
  {
    git diff --name-only "${BASE_REF}...HEAD" 2>/dev/null || true
    git diff --name-only HEAD 2>/dev/null || true
    git ls-files --others --exclude-standard 2>/dev/null || true
  } | awk 'NF' | sort -u
)"

is_ui_file() {
  case "$1" in
    crates/controller/assets/*|crates/controller/src/console.rs|crates/controller/src/demo.rs|tools/console-demo.sh)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

UI_CHANGED=""
while IFS= read -r f; do
  [ -z "$f" ] && continue
  if is_ui_file "$f"; then
    UI_CHANGED="${UI_CHANGED}${f}"$'\n'
  fi
done <<< "$CHANGED_FILES"

if [ -z "$UI_CHANGED" ]; then
  echo "未检测到控制台 UI 改动（对比基线：${BASE_REF}），无需同步 docs/demo/。"
  exit 0
fi

echo "检测到控制台 UI 相关改动（对比基线：${BASE_REF}）："
printf '%s' "$UI_CHANGED" | sed 's/^/  - /'

if [ "$MODE" = "check" ]; then
  if printf '%s\n' "$CHANGED_FILES" | grep -qx 'docs/demo/index.html'; then
    echo "检查通过：docs/demo/index.html 已随本次改动同步更新。"
    exit 0
  fi

  # 支持纯后端重构等特殊场景通过 Skip-Demo-Check 显式跳过
  if [ "${SKIP_DEMO_CHECK:-0}" = "1" ] || git log -1 --format=%B HEAD | grep -qi '^Skip-Demo-Check:'; then
    echo "已检测到 Skip-Demo-Check 标记，跳过 Demo 同步检查。"
    exit 0
  fi

  echo "" >&2
  echo "错误：控制台 UI 相关文件已改动，但 docs/demo/index.html 尚未同步更新。" >&2
  echo "请执行以下命令更新演示页与截图后提交：" >&2
  echo "  tools/console-demo.sh docs/demo" >&2
  echo "若本次仅为纯 Rust 重构且不影响界面渲染，可在 commit 正文或 PR 描述中添加 'Skip-Demo-Check: <原因>'。" >&2
  exit 1
fi

echo ""
echo "正在重新生成 docs/demo/ ..."
"$ROOT/tools/console-demo.sh" "$ROOT/docs/demo"

echo ""
echo "Demo 产物已更新。请确认："
echo "  1. 若新增了可见功能或状态，crates/controller/src/demo.rs 是否已补充对应示例数据；"
echo "  2. 查看 docs/demo/screenshot.png，确认排版与中文显示正常。"
