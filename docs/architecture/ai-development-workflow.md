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

执行 AI 通过 ChatGPT 会话承担具体核验、实施、测试或文档任务。会话与角色是执行载体，不自动产生 Owner 权限；同一授权主体可以承担多个明确区分的 Owner 职责，但**实施者不能把自己的会话或自审声明为独立 Reviewer**。

**工作单元（Work Unit）格式：**
- **主 GitHub Issue 引用**：repository full name + issue number + canonical URL；GitHub Issue 是需求与状态的系统记录，不在记忆中复制其状态
- Session Ops WorkItem ID（执行连续性聚合标识）；当前 WorkItem schema 尚无结构化 GitHub Issue 引用，二者关联目前必须显式写入 Work Unit，不能声称已持久化
- 当前仓库、分支、目标 Commit SHA，以及关联 PR/commit/test/CI/review 链接
- 任务范围、允许/禁止修改的文件、前置授权状态
- 验收条件、必须返回的证据、停止/升级条件

**四种载体不可混同：**
- GitHub Issue：权威需求、范围、状态和验收记录
- Session Ops WorkItem：跨执行 Session 的工作连续性聚合，不替代 GitHub Issue
- Session：一个真实会话/执行实例，通过显式、可信的 SessionLink 关联 WorkItem
- Work Unit：投递给执行会话的自包含任务包，不是数据库实体，也不是 WorkItem 的别名

**结构化返回：** 执行身份/角色 → 基线 SHA → 修改范围 → 验证命令与原始结果 → GitHub 证据 → 独立审查（精确 head）→ Owner 裁决 → 最终状态
（COMPLETE / BLOCKED / NEEDS_REVIEW / NEEDS_OWNER_DECISION / FAILED）

**会话管理（legacy tooling，不等于获批的 MCP 工作流）：**
- `--new` 创建会话并投递工作单元
- `<conv_id> <prompt>` 续接已有会话
- `--tail <conv_id>` 收割末条回复
- 这些命令的存在不证明身份授权、跨会话记忆效果或端到端证据链完整；账号轮转、匿名回退及认证/挑战处理均不属于获批 MCP 工作流。

## 3. 角色分工与授权

| 角色 | 承担者 | 职责 | 失败策略 |
|---|---|---|---|
| **Competent Owner** | 由治理记录明确授权的主体/会话 | 在授权范围内作出范围、验收和合并裁决 | NEEDS_REVISION / REJECT / ESCALATE |
| **实施者** | 执行 ChatGPT 会话 | 仅在已授权范围内修改并验证 | BLOCKED 范围外改动 |
| **独立 Reviewer** | 与实施者分离的会话/审查主体 | 独立读取精确 diff、挑战假设、核验测试与范围 | REQUEST_CHANGES / NEEDS_MORE_EVIDENCE |
| **协调 AI** | WorkBuddy / 协调会话 | 拆解、调度、收集证据、回写状态；不得代替授权主体 | 核验失败不接受文字汇报 |

**关键约束：**
- 角色名称或会话自称不构成权限；必须有可追溯的授权依据和范围
- 一个主体可承担多个 Owner 职责，但同一实施者不得将自审包装成独立审查
- 独立性必须由不同执行会话/主体及可核验审查记录证明；若工具链不能证明 prompt-pair provenance，标记 NOT_VERIFIED
- 所有技术审查绑定精确 Commit SHA；head 变化后重新确认审查适用性
- Issue #2 的规划建议不构成实施授权；REQ-12 #345 当前仍是 proposal，Owner 与 acceptance authority 未指定
- 跨仓库/跨 WorkItem 授权不可继承

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
- 需求留在原 GitHub Issue；实施用窄范围 PR；WorkItem 不复制或覆盖 Issue 状态
- 每个 WorkItem 的 Work Unit 必须携带一个 canonical primary Issue（repo full name + number + URL）；当前 SO WorkItem schema 未持久化该字段，不能把 prompt 约定描述为结构化契约
- 证据链分开记录：用户目标/Prompt provenance → WorkItem 创建与授权 → Session/SessionLink → 分支/commit → 测试与 CI → 独立 Review（精确 head）→ Owner decision
- 任何单条边缺失都应标为 PARTIAL/NOT_VERIFIED；不能用 WorkItem↔Session 链接替代 prompt→commit provenance
- Review 绑定实际审查版本；PR 更新后确认审查是否仍适用；Owner 裁决必须记录角色、授权范围和精确 head
- 不因 PR 创建关闭需求；不因提交认定验收；不因测试通过认定生产可用
- 当前 REQ-04 #265 的 prompt→commit traceability 仍为 PARTIAL：135 份 manifest 中仅 9 份同时具备 prompt hash 与 implementation commit SHA；注意力/上下文切换成本 AC8 为 ABSENT

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

跨会话记忆只能作为显式 WorkItem 范围内的非权威历史上下文；GitHub Issue/PR 才是需求与交付状态系统记录。记忆不能授予权限、替代当前指令、改变 Owner/验收结论或证明执行发生。WorkItem↔Issue 的结构化映射尚未实现。

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

状态词仅表示本节所列证据支持的能力，不代表整体生态验收或生产批准。

| 能力 | 状态 | 可核验依据与边界 |
|---|---|---|
| P1-01 文件锁 fail-closed | **SUPPORTED** | Adapter main 已包含相关实现；历史记录为 commit `f6a0434`、104/104 测试。此项不代表整个跨仓库交付闭环完成。 |
| ChatGPT 会话投递/收割工具 | **PARTIAL** | SO 有会话创建、续接和收割工具/脚本；这些是 legacy session-transport 能力，不等于获批的研发工作流。账号轮转、匿名回退及认证/挑战内部机制明确排除在 MCP 与试点之外。 |
| GitHub 证据闭环 | **PARTIAL** | GitHub Issue/PR/commit/CI/review 记录可用，但 REQ-04 #265 AC6 仅 9/135 manifests 同时具备 prompt hash 与 implementation commit SHA；AC8 注意力/上下文切换成本 ABSENT。 |
| 独立审查 | **PARTIAL** | 有独立会话审查模式和精确 SHA 绑定规则；REQ-12 #345 要求独立性有可验证证据。Reviewer prompt-pair provenance 未验证时，不得把角色标签或 Owner 自审称为独立审查。 |
| Owner 授权裁决 | **PARTIAL** | 仓库治理与授权记录存在，但角色由授权记录赋予，不由 ChatGPT 会话自称。REQ-12 #345 当前仍为 proposal，Accountable Owner / Acceptance Authority 均 NOT_ASSIGNED。 |
| Session/WorkItem 连续性 | **PARTIAL** | SO main 有 WorkItem、SessionLink、授权存储和 MCP 工具；但 Adapter Store 没有已证明的 SO 生产调用方，真实 Session A/B pilot 未获批准。 |
| WorkItem → GitHub Issue 结构化映射 | **MISSING** | 当前 SO WorkItem 记录没有 canonical repository/issue reference；目前只能在 Work Unit 中显式携带，不能声称已持久化。 |
| Session Ops MCP | **SUPPORTED** | SO main 有独立 MCP server、OIDC JWT verifier、10 个工作流工具和受限 schema/响应；Eligible ChatGPT custom-MCP 客户端的端到端连接验收仍 **NOT_VERIFIED**。 |
| Adapter MCP | **MISSING** | Adapter main 无 `rmcp` 依赖、MCP 模块或 server target。Adapter #25 的设计决策已接受用于开发规划，但明确未授予 source implementation、remote exposure 或 deployment 权限。 |
| P1-02 中文检索优化 | **SUPPORTED** | Adapter Core 的中文检索实现与回归测试存在；这只证明组件/测试能力，不证明真实 Agent 任务收益。 |
| P1-03 时间偏置基准 | **NOT_VERIFIED** | 本轮未取得足以确认当前 main 实现/测试状态的精确证据；保留待核验，不沿用旧“计划中”标签。 |
| 跨仓库记忆共享 / 真实 Agent pilot | **PARTIAL** | Adapter Core 有 WorkItem-scoped store/retrieve；SO 侧有 retrieve 路径，但无已证明的生产 Store caller。#458 的三臂真实 Agent pilot 仍需 Owner 显式批准，#374 的 policy proxy 不是 integrated Brain runtime。 |
| 生产部署 / 生产效果 | **NOT_VERIFIED / NOT_AUTHORIZED** | 当前证据不构成生产部署、生产数据访问、真实用户收益或生产 SLO 证明；相关授权仍未授予。 |

## 8. 变更记录

| 日期 | 变更 | Commit |
|---|---|---|
| 2026-11-20 | 首版文档创建 | — |
| 2026-11-20 | 增加 rebase / squash 变更清单核对门禁 | — |
| 2026-11-28 | 按 Adapter #37 / ecosystem #458 证据重分类能力状态与身份/证据边界 | pending PR |
