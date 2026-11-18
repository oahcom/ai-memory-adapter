# AI Memory Adapter

用于 AI 大脑项目的记忆层实现，解决 ChatGPT 会话上下文碎片化问题。

## 架构

```
Memory Adapter (接口层)
├── Storage (SQLite + 向量库)
├── Context Injection (智能上下文注入)
└── Pruning (Ebbinghaus 遗忘)
```

## 开发指南

本项目按照 AI-STD-001 规范开发，由 ChatGPT 在远程服务器 43.138.133.238 上执行。

