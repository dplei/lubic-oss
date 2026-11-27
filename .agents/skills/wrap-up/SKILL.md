---
name: wrap-up
description: lubic-oss 项目级收尾扩展：在完成代码收尾或提交 PR 前，自动检测控制台 UI 是否发生变化，若有变化则同步更新 demo.rs 示例数据、docs/demo/index.html 与 docs/demo/screenshot.png。
---

# lubic-oss 收尾扩展（Demo 同步）

在执行标准收尾（测试、rebase、合入 / 提 PR）时，按本扩展检查并同步控制台演示产物，保证 `docs/demo/` 与 README 截图始终与当前 UI 一致。

## 步骤

1. **运行 UI 变更检测**：
   在当前分支（或 worktree）执行：
   ```bash
   tools/sync-demo.sh
   ```
   - 若输出「未检测到控制台 UI 改动」，直接跳过后续步骤，继续标准收尾。
   - 若检测到 `crates/controller/assets/*`、`crates/controller/src/console.rs`、`crates/controller/src/demo.rs` 或 `tools/console-demo.sh` 有改动，脚本会自动运行 `tools/console-demo.sh docs/demo`。

2. **核对演示数据与渲染效果**：
   - **功能增量检查**：如果本次改动新增了页面元素、状态或交互（例如新字段、新标签页），检查 `crates/controller/src/demo.rs` 是否已补充对应的虚构示例数据（注意遵循公开版脱敏规则，不写个人机器名、房间名、域名）。若补充了 `demo.rs`，重新运行一次 `tools/console-demo.sh docs/demo`。
   - **视觉核对**：直接打开或读取 `docs/demo/screenshot.png`，确认布局、中文字符与新增状态展示正常；若在无 Chrome 的环境跑脚本导致未更新 `screenshot.png`，需在有 Chrome 的环境补生成或在 PR 说明中注明。

3. **纳入提交与 PR 检查**：
   - 将更新后的 `docs/demo/index.html`、`docs/demo/favicon.svg`、`docs/demo/screenshot.png`（以及如有改动的 `crates/controller/src/demo.rs`）纳入本次 PR 分支提交。
   - PR 的 CI（`.github/workflows/pr.yml`）会在检测到 UI 改动时自动校验 `docs/demo/index.html` 是否已同步更新；若本次仅为纯 Rust 重构且不影响界面渲染，可在 PR 描述或 commit 正文中附带 `Skip-Demo-Check: <原因>`。
