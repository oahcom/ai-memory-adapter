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

## 2.1 载体与术语边界

以下概念必须保持分离：

| 概念 | 权威含义 | 当前持久化/状态 |
|---|---|---|
| **GitHub Issue** | 需求、缺陷、范围、状态和治理讨论的系统记录 | GitHub 是唯一状态权威；不得把 Issue 状态复制到记忆作为第二事实源 |
| **Session Ops WorkItem** | 一个有边界的执行连续性聚合，可跨多个 Session | 当前 SO schema 仅有 `work_item_id`、`status`、`created_at`、`updated_at`；owner/tenant 另存于 authority 表；没有结构化 GitHub Issue 引用 |
| **Session** | 一次实际 ChatGPT 对话/执行实例 | 通过 SessionLink 关联 WorkItem；SessionLink 是身份/生命周期关系，不是角色分配 |
| **Work Unit** | 可独立交接的自包含 prompt/任务包 | 是交接文档，不是数据库 WorkItem，也不是新的权限载体 |
| **Assignment（目标设计）** | 把 WorkItem 分配给 worker/reviewer/owner/coordinator 角色及对应 Session | 当前不存在持久化 Assignment 记录；属于待审查设计，不得描述为已实现 |

**建议的关联契约（尚未实现）：** 每个开发 WorkItem 指向一个主 GitHub Issue，使用
`repository_full_name + issue_number` 作为规范键；URL 可派生。一个 Issue
可以拆成多个执行 WorkItem。PR、commit、CI run、review 和 Owner decision
作为带类型的证据引用关联到 WorkItem。GitHub Issue 的 title/body/state
仍只由 GitHub 管理，WorkItem 不复制或覆盖这些状态。

## 3. 角色分工与授权

| 角色 | 承担者 | 职责 | 证据/限制 |
|---|---|---|---|
| **Owner / 验收权威** | 由治理记录明确授权的主体；可与协调角色共置 | 范围裁定、验收、合并/部署授权 | 权限来自权威治理记录，不来自 ChatGPT 会话自述 |
| **实施者（worker）** | 被分配的执行 Session | 实施、测试、提交证据 | 绑定 WorkItem、Session 和 prompt provenance |
| **独立 Reviewer** | 与实施者不同的 Session | 审查 diff、测试和验收证据 | 必须绑定被审查的精确 head SHA 和独立 prompt provenance |
| **协调者（coordinator）** | WorkBuddy 或 ChatGPT 会话 | 分解、派发、收集、核验、升级阻塞 | 不因协调职责自动获得 Owner 或合并权限 |

**关键约束：**
- Owner 与 coordinator 可以共置，但共置不会产生 Owner 权限；必须有独立、可追溯的授权来源。
- worker 不得自兼独立 Reviewer。Reviewer 必须使用不同 Session 和不同 prompt digest；GitHub 登录账号是否相同不是独立性的判据。
- 所有审查与裁决绑定精确 Commit/PR head SHA；PR head 变化后，旧审查不自动适用。
- GitHub Issue、Session Ops WorkItem、Session 和 Work Unit 是不同载体，不得互相冒充。
- 记忆只提供历史上下文，不授予权限、不指定 Owner、不满足验收或合并门禁。
- 跨仓库/跨 WorkItem 授权不可继承。

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

## 7. 能力状态（证据快照：2026-11-28）

状态定义：**SUPPORTED** = 当前基线存在可观察实现和对应测试/运行证据；
**PARTIAL** = 仅覆盖部分链路或仍有关键证据/授权缺口；
**MISSING** = 当前基线不存在该持久化能力/服务；
**NOT_VERIFIED** = 缺少足以作出判断的验收证据；
**NOT_AUTHORIZED** = 没有生产/高风险操作授权。状态只适用于表中明确的范围。

| 能力 | 状态 | 当前证据与边界 |
|---|---|---|
| P1-01 文件锁 fail-closed | **SUPPORTED（限本地工具）** | 历史记录指向 `f6a0434` 和 104/104 测试；这只证明对应本地锁行为，不证明整个 AI 开发生态闭环 |
| ChatGPT 会话投递/收割 | **PARTIAL** | 存在 `--new`、`conv_id` 续接和 `--tail` 等旧工具；排队成功只证明任务已持久化，不代表远端会话已回答。账号轮转、匿名回退及挑战处理不属于已批准的 MCP/试点能力 |
| GitHub 证据闭环 | **PARTIAL** | GitHub 可承载 Issue/PR/Commit/Review，但 SO WorkItem 尚无结构化主 Issue 引用，prompt→commit provenance 未达到端到端可追溯 |
| 独立审查 | **PARTIAL** | 可以使用不同 Session 和精确 SHA 审查；但没有持久化 Assignment，也没有已验证的 worker/reviewer prompt-pair provenance。PR #34 当前无 review 记录 |
| Owner 授权裁决 | **NOT_VERIFIED** | Owner 权限必须来自 AiMemoryBrainArchitecture 的权威治理记录；ChatGPT Session 的自述或结构化文本不能自授权限。当前 WorkItem 不持久化 Owner assignment/decision reference |
| 会话续接与恢复 | **PARTIAL** | Session ID、SessionLink 和续接工具存在；没有获批的真实 Session A/B 价值试点，不能将工具可用性等同于经验证的连续工作能力 |
| SO MCP | **SUPPORTED（当前工具边界）** | SO main 有 JWT 验证、scope 检查、Principal/Tenant/WorkItem 授权和 10 个 MCP 工具；`session.context` 只通过授权的 active SessionLink 派生 WorkItem |
| SO→Adapter 直接检索 | **PARTIAL** | `mcp_server.py` 的 `session.context` 经授权 active link 调用 `MemoryAdapterClient.retrieve`；当前生产路径没有发现 `MemoryAdapterClient.store` 调用，不能宣称完整记忆写入闭环 |
| Adapter MCP | **MISSING** | Adapter main 没有 first-class MCP server；实现目标仍在 Adapter #25，远程 MCP 的 Principal→Tenant→WorkItem 授权协议未冻结 |
| WorkItem→GitHub Issue 关联 | **MISSING** | SO WorkItem schema 仅含 ID/status/timestamps；没有规范化 repository+issue number 主引用 |
| WorkItem 角色分配与 prompt provenance | **MISSING** | 没有持久化 Assignment；SessionLink 不能代替 worker/reviewer/Owner 角色分配 |
| 真实跨会话 A/B 试点 | **NOT_VERIFIED / BLOCKED** | AiMemoryBrainArchitecture #458 要求单独实验计划与 Owner 批准；当前没有获批的真实 Session A/B 试点 |
| P1-02 中文检索优化 | **NOT_VERIFIED** | 仍需独立范围和 Owner 决策 |
| P1-03 时间偏置基准 | **NOT_VERIFIED** | 仍需独立范围和 Owner 决策 |
| 生产部署/生产数据接入 | **NOT_AUTHORIZED** | #458 明确不包含生产部署、生产数据接入或生产激活授权 |

### 7.1 证据解释与未决度量

- SO 基线：`25882441ddb1d23bea355737653f6fc09c1068ed`；Adapter main 基线：`3c39f22f6b1c0b6b75a35008afe5970f01718ba0`。
- SO 直接检索证据：`src/session_ops/mcp_server.py`、`src/session_ops/authority.py`、`src/session_ops/mcp_auth.py`、`src/session_ops/memory_adapter_client.py`。
- Adapter stdio 证据：`src/bin/memory-stdio.rs`、`tests/stdio_protocol.rs`。当前 Adapter main 的 19/19 协议测试中，`multiple_requests_in_one_process_produce_one_response_each` 明确测试了与当前 v0.1 §5 单请求/进程规则相反的行为；测试全绿不等于契约符合。
- REQ-04 #265 将 prompt→commit traceability 记为 PARTIAL、attention/context-switch cost 记为 ABSENT。该 Issue 当前记录的 manifest 双字段统计与 #37 使用的 TRACEABLE 统计口径不同；在统一分母、谓词和测量时间前，不应把任一数字包装为端到端覆盖率。
- REQ-12 #345 最新记录为 1 SATISFIED / 10 PARTIALLY-SATISFIED / 1 NOT-DEMONSTRATED；reviewer Session 分离已有记录，但 prompt-pair provenance 仍未验证。
- Adapter #36 与 SO #165 追踪 JSONL 输入/内容/元数据/响应及 subprocess stdout/stderr 的硬字节上限。字符预算 `max_chars` 不是硬字节上限。
- Adapter #37、SO #166、AiMemoryBrainArchitecture #458 是本节状态与术语的配对治理载体。这里的目标设计不代表 schema 变更已经获批。


## 8. 变更记录

| 日期 | 变更 | Commit |
|---|---|---|
| 2026-11-20 | 首版文档创建 | — |
| 2026-11-20 | 增加 rebase / squash 变更清单核对门禁 | — |
| 2026-11-28 | 按 Adapter #37 / SO #166 / Ecosystem #458 重分类能力状态并澄清 Issue、WorkItem、Session、Work Unit 与角色授权边界 | — |
