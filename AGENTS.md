# AGENTS.md

## 测试与验证

- 提交或合入前需确保 `cargo test --workspace` 通过。
- 仓库 `main` 分支开启保护，所有改动通过 PR 合入。

## 收尾与 Demo 同步（Skill + Harness）

- 无论人工开发还是 Agent 执行收尾（`wrap-up`），提交或推送 PR 前必须执行 `.agents/skills/wrap-up/SKILL.md` 的项目级收尾扩展：
  1. 运行 `tools/sync-demo.sh` 检测是否改动了控制台 UI（`crates/controller/assets/*`、`crates/controller/src/console.rs`、`crates/controller/src/demo.rs`、`tools/console-demo.sh`）。
  2. 若有改动，确认 `crates/controller/src/demo.rs` 是否需要补充新功能示例数据，再运行 `tools/console-demo.sh docs/demo` 同步 `docs/demo/index.html` 与 `docs/demo/screenshot.png`，并将结果纳入提交。
- PR CI（`.github/workflows/pr.yml`）会在检测到 UI 改动时校验 `docs/demo/index.html` 是否已同步更新。

## 公开仓库规范

- 注释、测试数据与演示数据（含 `crates/controller/src/demo.rs`）中禁止写入个人机器名、房间名、域名、IP 或本机路径，统一使用中性示例称呼。
- 创建或更新 GitHub PR 时，标题和正文默认使用简体中文。
