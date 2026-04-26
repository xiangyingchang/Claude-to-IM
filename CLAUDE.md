# Claude-to-IM

Claude Code ↔ IM 平台桥接（Telegram / 飞书）。通过 IM 消息与 Claude 交互。

## 快速开始

```bash
npm start          # 启动桥接
npm run typecheck  # 类型检查
```

## 项目结构

```
host/              # 宿主应用（核心修改都在这里）
  index.ts          - 入口，加载配置、初始化桥接上下文
  json-store.ts     - JSON 文件实现的 BridgeStore
  llm-provider.ts   - 封装 Claude Code SDK
  permissions.ts    - 权限网关
src/lib/bridge/    # 桥接库（一般不改）
  adapters/         - Telegram / 飞书 / Discord 适配器
  bridge-manager.ts - 编排器，管理适配器生命周期
  conversation-engine.ts - 处理消息流
  context.ts        - DI 容器
  host.ts           - 宿主接口定义
```

## 配置

通过 `.env` 文件配置，关键变量：
- `CLAUDE_TO_IM_ENABLED` — 主开关
- `TELEGRAM_BOT_TOKEN` + `TELEGRAM_ALLOWED_USERS` — Telegram
- `FEISHU_APP_ID` + `FEISHU_APP_SECRET` — 飞书
- `CLAUDE_TO_IM_WORK_DIR` — 默认工作目录

## 架构要点

- 桥接库通过 4 个 DI 接口（BridgeStore / LLMProvider / PermissionGateway / LifecycleHooks）与宿主解耦
- LLMProvider 的 `streamChat()` 返回 `ReadableStream<string>`（SSE 格式事件流）
- SDK 事件 → SSE 转换在 `host/llm-provider.ts` 中，关键映射：
  - SDK `assistant` 消息 → SSE `text` / `tool_use`
  - SDK `stream_event` content_block_delta → SSE `text`
  - SDK `result` → SSE `result`
- 权限：`canUseTool` 回调 → SSE `permission_request` → IM 按钮 → `PermissionGateway.resolvePendingPermission()`

## 已知注意事项

- Claude Code SDK（`@anthropic-ai/claude-agent-sdk`）需指定 `pathToClaudeCodeExecutable` 指向系统 `claude` 二进制
- SDK `session.sessionId` 在流消费第一条消息后才可用，不能提前访问
- `allowedTools: ['*']` 会导致问题，依赖 `canUseTool` + `permissionMode` 处理权限即可
- JSON 存储文件位置：`~/.claude-to-im/store.json`
