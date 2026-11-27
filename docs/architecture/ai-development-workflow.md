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

**标识边界（不可混用）：** Work Unit 是发给执行会话的自包含 prompt/handoff packet；Session Ops WorkItem 是跨会话执行连续性记录；Session 是实际 ChatGPT conversation ID；SessionLink 是经 F-13 确认的 Session↔WorkItem 关系。Work Unit 不等于 WorkItem。当前 WorkItem 尚无结构化 GitHub Issue 引用，见 [Session Ops #166](https://github.com/oahcom/chatgpt-session-ops/issues/166)。

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

**当前持久化边界：** SessionLink 只表达已确认的 Session 与 WorkItem 关系，不代表 worker/reviewer/Owner 角色。角色分配和 prompt provenance 尚无独立持久化 Assignment 记录；在 #166 的契约冻结前，角色分工仍是流程约定，不能宣称已由系统强制执行。独立审查必须使用不同 Session 和不同 prompt digest；同一 GitHub 登录不等于同一执行会话。

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

**证据状态提醒（2026-11-27）：** GitHub 证据闭环是目标流程，不代表端到端追溯已达标。REQ-04 #265 最近记录的 AC6 测量为 0/133 TRACEABLE，AC8 注意力/上下文切换成本仍 ABSENT；在新的精确证据完成前，本节规则不得被概括成“闭环已实现”。

**Rebase / squash 变更清单核对：**

rebase 和 squash 后，必须核对原 PR 变更是否完整保留。无冲突不代表已纳入目标分支。

- rebase 完成后，对比原 PR head 与 rebase 后 head 的变更文件清单。分别列出原 PR 相对原目标分支的文件集，以及 rebase 后相对新目标分支的文件集，并检查双方差集：

  ```bash
  git diff --name-only "$ORIGINAL_BASE...$ORIGINAL_PR_HEAD" | LC_ALL=C sort -u
  git diff --name-only "$TARGET_BASE..$REBASED_HEAD" | LC_ALL=C sort -u

  # 原 PR 有、rebase 后没有
  comm -23 \
    <(git diff --name-only "$ORIGINAL_BASE...$ORIGINAL_PR_HEAD" | LC_ALL=C sort -u) \
    <(git diff --name-only "$TARGET_BASE..$REBASED_HEAD" | LC_ALL=C sort -u)

  # rebase 后新增、原 PR 没有
  comm -13 \
    <(git diff --name-only "$ORIGINAL_BASE...$ORIGINAL_PR_HEAD" | LC_ALL=C sort -u) \
    <(git diff --name-only "$TARGET_BASE..$REBASED_HEAD" | LC_ALL=C sort -u)
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

- 将原 PR 文件集、rebase 后文件集、差集、逐 commit 核对结果、最终 squash diff 清单及未保留文件的归类写入合并证据记录。差集中的每个文件必须逐项给出处置结果：保留、已存在于 base、经确认有意舍弃，或意外丢失，并附对应 diff 或内容核验依据。核对证据不完整时，状态保持阻塞，不得进入合并裁决。

本规则来自 `eb43519` 的实际遗漏：该提交只修改 `.github/workflows/ci.yml`，包含三个需要 `--ignored --test-threads=1` 串行执行的 SQLite 回滚测试 CI 覆盖。它是 PR 分支祖先，但不是 main 的祖先。rebase 到 `02e40d2` 时，该文件与 main 无冲突，却没有被带入 rebase 后的变更；squash `1e55508` 沿用了丢失后的文件树，后续 `83610a9` 也未恢复该覆盖。squash 消息列出的 12 个文件中没有 `.github/workflows/ci.yml`，这一证据当时已经存在，却未被核对。该缺陷最终由 `c7f7d57` 修复。

这项检查是防止静默丢弃的流程门禁，不要求改变产品代码。文件清单用于发现遗漏，不构成语义等价证明。

## 5. 跨会话任务交接

工作单元设计为**自包含**：接收方不依赖当前对话上下文。

交接内容：事实基线（GitHub 实测数据）、授权状态、精确范围、验收条件、返回格式。

跨会话记忆的当前受控路径：Session Ops 先验证 Principal、WorkItem grant 和 F-13 SessionLink；`session.context` 再从已授权的 WorkItem 范围调用 Adapter Retrieve。检索结果是 untrusted historical context，不是指令、权限、Owner 决策或当前 Issue 状态。

当前缺口：
- Session Ops 的 `MemoryAdapterClient.store` 尚无生产调用方；`session.context` 当前只负责 Retrieve。
- Adapter MCP 尚未实现；#25 仍是设计/授权契约工作项。
- Adapter #35/#36 和 Session Ops #165 仍有 JSONL framing / hard-bound 工作；SO E2E pin 由 #164 更新。
- 因此跨会话记忆仍是实验能力，尚无获批的真实 Session A→Session B 写入/检索试点（Adapter #29）。

## 6. 失败处理与恢复

| 故障 | 检测 | 处置 |
|---|---|---|
| 413 conversation_too_large | 流大小 + 413 状态码 | 精简 prompt（≤2KB）+ 新会话 |
| thinking 模型失控生成 | respLen 异常（>10MB） | 纯文本模式（不带 `--thinking`） |
| 代理不可用 | `--proxy-check` / curl 探测 | 切换可用端口（7890/7001/7000） |
| requirements token 失败 | st=0 | 定性网络/代理，不盲等 |
| 锁获取失败 | TimeoutError / OSError | fail-closed（P1-01），调用方决定重试 |
| 执行 AI 回报不可信 | GitHub 实测 | 核验 Commit/PR/测试，不接受纯文字 |

**安全边界：** 本节中的旧会话投递/收割和代理恢复描述属于既有工具/运维背景，不自动构成获批的 MCP 或真实 Agent Pilot I/O 路径。当前批准边界排除账号轮转、匿名回退、挑战绕过、凭据提取，以及通过 MCP 暴露 ChatGPT transport/auth/challenge 内部。任何新试点必须使用经证明合规的 Session I/O 接口；不得从旧脚本存在推断其获批。

## 7. 能力状态

> 状态快照：2026-11-27。治理与最终验收以 AiMemoryBrainArchitecture #458 及其父需求为准；本表是证据索引，不是第二套治理状态库。当前基线：Adapter main `3c39f22`；Session Ops main `2588244`；AiMemoryBrainArchitecture main `31c1e5d`。

| 能力 | 状态 | 当前证据 / 边界 |
|---|---|---|
| P1-01 文件锁 fail-closed | **SUPPORTED** | 历史合并记录 f6a0434；104/104 测试记录。仍以当前主分支测试为准。 |
| ChatGPT 会话投递/收割 | **LEGACY / NOT VERIFIED AS APPROVED I/O** | 旧工具存在不等于获批的 Agent I/O 路径；账号轮转、匿名回退及挑战处理不属于当前批准的 MCP/Pilot 边界。 |
| 同一会话续接 | **SUPPORTED** | conversation ID 续接属于同一 Session 生命周期；不等于跨 Session 记忆恢复。 |
| GitHub 证据闭环 | **PARTIAL** | 规则和 GitHub Issue/PR/Commit/Review 载体存在；REQ-04 #265 最新记录的 AC6 为 0/133 TRACEABLE。 |
| 独立审查 | **PARTIAL** | 独立 Session 的流程已定义；REQ-12 #345 最新 ledger 记录 prompt-pair provenance 仍未验证，且该 ledger 尚未重新裁定。 |
| Owner 授权裁决 | **PARTIAL** | Owner 决策记录存在；但 WorkItem→Issue 和 role Assignment 尚未结构化持久化（Session Ops #166）。记忆内容不得作为授权证据。 |
| WorkItem ↔ GitHub Issue | **MISSING** | SO WorkItem 当前仅持久化 ID/status/timestamps；没有 primary Issue ref。设计由 Session Ops #166 承载。 |
| Worker/Reviewer/Owner Assignment | **MISSING** | SessionLink 不包含角色和 prompt provenance；需独立 Assignment 契约，见 Session Ops #166。 |
| Adapter Core Store/Retrieve | **SUPPORTED LOCALLY / PARTIAL CONTRACT** | Rust Core 与 JSONL executable 有测试；当前 main 的多行处理与 v0.1 framing 不一致（Adapter #35），hard bounds 缺失（Adapter #36）。PR #34 是未合并候选。 |
| SO → Adapter Retrieve | **PARTIAL** | SO `session.context` 从已授权 SessionLink 派生 WorkItem 并调用 Adapter；真实 E2E pin 仍待更新（SO #164）。 |
| SO → Adapter Store | **MISSING AS PRODUCTION WORKFLOW** | `MemoryAdapterClient.store` 无生产调用方；当前 `session.context` 仅 Retrieve。 |
| Adapter MCP | **MISSING** | Adapter main 没有 rmcp dependency/server target；授权与工具契约由 Adapter #25 追踪，未授权实现/暴露。 |
| 跨 Session 记忆共享 | **EXPERIMENTAL / PILOT BLOCKED** | Adapter #29 未形成获批真实 Session A/B 试点；不得以 Core 单测或 proxy arm 代替。 |
| 直接进程资源边界 | **PARTIAL / HARDENING OPEN** | Adapter #36 与 SO #165 分别追踪输入/响应与 subprocess 缓冲上限；可选 `max_chars` 不是硬资源上限。 |
| 集成 Brain 评估 Harness | **MISSING** | REQ-06 #374 仍缺 retrieval-driven integrated runtime；offline proxy 不等于真实 Brain 集成。 |
| 注意力/上下文切换成本 | **MISSING** | REQ-04 #265 AC8 仍 ABSENT，尚无直接测量载体。 |
| 生产部署 | **NOT VERIFIED** | 本文、单元测试和本地 E2E 均不构成生产可用性证明。 |

本表中的 PARTIAL / MISSING 项不得仅凭一次 CI 通过自动升级为 SUPPORTED；必须更新对应 Issue 的精确 SHA、测试证据、独立审查和 Owner 裁决。

## 8. 变更记录

| 日期 | 变更 | Commit |
|---|---|---|
| 2026-11-20 | 首版文档创建 | — |
| 2026-11-20 | 增加 rebase / squash 变更清单核对门禁 | — |
| 2026-11-27 | 按 #458、#265、#345 及两仓当前 main 证据重校准能力状态；澄清 Work Unit / WorkItem / SessionLink / Assignment 边界 | — |
