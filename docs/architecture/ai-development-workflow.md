# AI Development Workflow

> 首版：2026-11-20 · 本文档描述 AI 辅助研发闭环的架构、角色边界和证据链路。
> 本文档位置不代表治理权转移——AiMemoryBrainArchitecture 仍是治理权威。

## 1. 总览

```
需求(Issue) → 治理核验 → 任务拆解 → 执行 AI 实施 → 代码/测试
→ GitHub PR → 独立审查 → Owner 验收 → Issue 回写 → 后续规划
```

五个载体各司其职：

| 载体 | 职责 | 边界 |
|---|---|---|
| **AiMemoryBrainArchitecture** | 治理与授权权威 | 定义权限、生命周期、责任矩阵 |
| **chatgpt-session-ops** | 会话生命周期与执行协调 | 投递、收割、续接、恢复；不替代 Owner 裁决 |
| **ai-memory-adapter** | 实验性记忆组件 | 开发循环记忆实验；非生产 |
| **GitHub (Issue/PR/Commit/Review)** | 工程任务与证据载体 | 可追溯、可审计、精确 SHA 绑定 |
| **ChatGPT 会话** | 研发执行与分析 | 承担实施/审查/Owner 角色（见 §3） |

## 2. ChatGPT 研发执行会话

执行 AI 通过 ChatGPT 会话承担具体代码核验、修改、测试和文档任务。

**工作单元（Work Unit）格式：**
- GitHub Issue/PR 链接
- 当前仓库和分支、目标 Commit
- 任务范围、允许/禁止修改的文件
- 前置授权状态
- 验收条件、必须返回的证据

**结构化返回：** 执行身份 → 基线 → 修改 → 验证 → GitHub 证据 → 最终状态
（COMPLETE / BLOCKED / NEEDS_REVIEW / NEEDS_OWNER_DECISION / FAILED）

**会话管理：**
- `--new` 创建新会话投递工作单元
- `<conv_id> <prompt>` 续接已有会话
- `--tail <conv_id>` 收割末条回复
- 失败重试：413 → 精简包 + 新会话；thinking 失控 → 纯文本模式

## 3. 角色分工与授权

| 角色 | 承担者 | 职责 | 失败策略 |
|---|---|---|---|
| **Session Ops Owner** | ChatGPT 会话 | 授权裁决、验收、合并批准 | NEEDS_REVISION / REJECT |
| **实施者** | ChatGPT 会话 | 代码修改、测试编写 | BLOCKED 范围外改动 |
| **独立 Reviewer** | 独立 ChatGPT 会话 | 读 diff、对照范围、审查结论 | REQUEST_CHANGES / NEEDS_MORE_EVIDENCE |
| **协调 AI** | WorkBuddy | 拆解任务、投递/收割、GitHub 回写、证据核验 | 核验失败不接受文字汇报 |

**关键约束：**
- 实施者不能自兼独立 Reviewer
- 所有裁决绑定精确 Commit SHA
- Issue #2 的规划建议不构成实施授权
- 跨仓库/跨工作项授权不可继承

## 4. GitHub 证据闭环

```
Issue（需求+状态）
  ├── PR（实施差异，关联 Issue）
  │     ├── Commit SHA（精确代码版本）
  │     ├── 测试结果（可追溯到 Commit）
  │     ├── Review（绑定实际审查的 SHA）
  │     └── Acceptance（Owner 裁决记录）
  └── Comment（阻塞原因、解除条件、后续建议）
```

**规则：**
- 需求留在原 Issue；实施用窄范围 PR
- PR 描述关联 Issue；测试结果可追溯
- Review 绑定实际审查版本；PR 更新后确认审查是否仍适用
- 不因 PR 创建关闭需求；不因提交认定验收；不因测试通过认定生产可用

**Rebase / squash 变更清单核对：**

rebase 和 squash 后，必须核对原 PR 变更是否完整保留。无冲突不代表已纳入目标分支。

- rebase 完成后，对比原 PR head 与 rebase 后 head 的变更文件清单。分别列出原 PR 相对原目标分支的文件集，以及 rebase 后相对新目标分支的文件集，并检查双方差集：

  ```bash
  git diff --name-only "$ORIGINAL_BASE...$ORIGINAL_PR_HEAD" | sort -u
  git diff --name-only "$TARGET_BASE..$REBASED_HEAD" | sort -u

  # 原 PR 有、rebase 后没有
  comm -23 \
    <(git diff --name-only "$ORIGINAL_BASE...$ORIGINAL_PR_HEAD" | sort -u) \
    <(git diff --name-only "$TARGET_BASE..$REBASED_HEAD" | sort -u)

  # rebase 后新增、原 PR 没有
  comm -13 \
    <(git diff --name-only "$ORIGINAL_BASE...$ORIGINAL_PR_HEAD" | sort -u) \
    <(git diff --name-only "$TARGET_BASE..$REBASED_HEAD" | sort -u)
  ```

- 对原 PR 的每个 commit，逐一核对其变更是否已由目标分支包含，或由 rebase 后的提交等价保留。列出原 PR commit，并使用 `git cherry` 和 `git range-diff` 辅助比对：

  ```bash
  git log --reverse --format='%H' "$ORIGINAL_BASE..$ORIGINAL_PR_HEAD"
  git cherry -v "$TARGET_BASE" "$ORIGINAL_PR_HEAD"
  git range-diff \
    "$ORIGINAL_BASE..$ORIGINAL_PR_HEAD" \
    "$TARGET_BASE..$REBASED_HEAD"
  ```

  对每个原始 commit 检查其实际 diff 和对应保留结果。工具标记或 patch-id 相同只能作为比对线索，不能替代对变更内容和行为的核验。

- 对纯 CI、文档、配置等与产品代码无冲突的提交，单独记录是否保留及其依据。不能以「无冲突」推定「已纳入」，也不能因产品代码变更已 rebase 就推定这些独立文件变更一并保留。

- squash 前，对比最终 diff 文件清单与预期变更清单：

  ```bash
  git diff --name-status "$TARGET_BASE..$SQUASH_HEAD"
  git diff --name-only "$TARGET_BASE..$SQUASH_HEAD" -- .github/workflows/ci.yml
  ```

  第二条命令用于检查指定文件是否出现在最终 diff 中；无输出表示该文件没有相对目标分支形成最终差异。对原 PR 中存在、但未出现在最终 diff 的文件，必须检查目标分支与 squash head 中的实际文件内容，并明确归类为：变更已存在于 base、经确认有意舍弃，或意外丢失。不得仅凭文件路径相同或文件清单相同认定语义等价。

- 将原 PR 文件集、rebase 后文件集、差集、逐 commit 核对结果、最终 squash diff 清单及未保留文件的归类写入合并证据记录。核对证据不完整时，状态保持阻塞，不得进入合并裁决。

本规则来自 `eb43519` 的实际遗漏：该提交只修改 `.github/workflows/ci.yml`，包含三个需要 `--ignored --test-threads=1` 串行执行的 SQLite 回滚测试 CI 覆盖。它是 PR 分支祖先，但不是 main 的祖先。rebase 到 `02e40d2` 时，该文件与 main 无冲突，却没有被带入 rebase 后的变更；squash `1e55508` 沿用了丢失后的文件树，后续 `83610a9` 也未恢复该覆盖。squash 消息列出的 12 个文件中没有 `.github/workflows/ci.yml`，这一证据当时已经存在，却未被核对。该缺陷最终由 `c7f7d57` 修复。

这项检查是防止静默丢弃的流程门禁，不要求改变产品代码。文件清单用于发现遗漏，不构成语义等价证明。

## 5. 跨会话任务交接

工作单元设计为**自包含**：接收方不依赖当前对话上下文。

交接内容：事实基线（GitHub 实测数据）、授权状态、精确范围、验收条件、返回格式。

跨会话记忆：workspace memory（日志+MEMORY.md）+ GitHub Issue/PR 证据 + conversation_search。

## 6. 失败处理与恢复

| 故障 | 检测 | 处置 |
|---|---|---|
| 413 conversation_too_large | 流大小 + 413 状态码 | 精简 prompt（≤2KB）+ 新会话 |
| thinking 模型失控生成 | respLen 异常（>10MB） | 纯文本模式（不带 `--thinking`） |
| 代理不可用 | `--proxy-check` / curl 探测 | 切换可用端口（7890/7001/7000） |
| requirements token 失败 | st=0 | 定性网络/代理，不盲等 |
| 锁获取失败 | TimeoutError / OSError | fail-closed（P1-01），调用方决定重试 |
| 执行 AI 回报不可信 | GitHub 实测 | 核验 Commit/PR/测试，不接受纯文字 |

## 7. 能力状态

| 能力 | 状态 | 说明 |
|---|---|---|
| P1-01 文件锁 fail-closed | **已实现** | 合并于 f6a0434，104/104 测试通过 |
| ChatGPT 会话投递/收割 | **已实现** | --new / 续接 / --tail，账号轮转 |
| GitHub 证据闭环 | **已实现** | Issue/PR/Commit/Review/验收可追溯 |
| 独立审查 | **已实现** | 独立会话、SHA 绑定、逐项判定 |
| Owner 授权裁决 | **已实现** | ChatGPT 会话承担，结构化输出 |
| 会话续接与恢复 | **已实现** | conv_id 续接、匿名会话补救 |
| 工作流架构文档 | **本文档** | 首版 |
| P1-02 中文检索优化 | **计划中** | 需新 Owner 决策 |
| P1-03 时间偏置基准 | **计划中** | 需新 Owner 决策 |
| 跨仓库记忆共享 | **实验中** | Adapter 实验，未生产验证 |
| 生产部署 | **未验证** | 测试通过≠生产可用 |

## 8. 变更记录

| 日期 | 变更 | Commit |
|---|---|---|
| 2026-11-20 | 首版文档创建 | — |
| 2026-11-20 | 增加 rebase / squash 变更清单核对门禁 | — |
