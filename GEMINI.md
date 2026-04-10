# GEMINI.md — Antigravity 专属配置

## Antigravity 行为设置
- 使用中文回复所有问题和解释
- 代码注释使用英文（便于国际化），但 JavaDoc 关键描述可使用中文
- 生成代码时，自动添加 `@author` 和 `@since` 标签
- 回答前先确认是否涉及安全护栏中需确认的操作

## 工作区特定覆盖
### 优先使用以下工具链
- 构建工具: Maven（优于 Gradle）
- HTTP 客户端: RestClient（Spring Boot 3.2+）或 WebClient，不使用 RestTemplate
- 序列化: Jackson（默认），日期格式统一使用 ISO-8601

### 代码生成偏好
- Controller 类命名: `XxxController`
- Service 接口命名: `XxxService`，实现类: `XxxServiceImpl`
- 测试类命名: `XxxTest` 或 `XxxIntegrationTest`
- DTO 命名: `XxxRequest` / `XxxResponse`

### 日志规范
- 使用 Lombok 的 `@Slf4j` 注解
- 关键操作记录 INFO 级别，调试信息使用 DEBUG
- 异常日志必须包含完整堆栈：`log.error("message", exception)`

## 与 OpenSpec 协作
- 进行重大功能开发前，检查 `openspec/specs/` 中的相关规范
- 功能完成后，提醒用户执行 `/openspec:archive` 归档变更[reference:6]

## Antigravity 特定命令参考
- 规则管理: 通过代理管理器右上角 `...` → 附加选项 → 自定义 添加/修改规则[reference:7]
- 嵌套规则: 可在子目录放置 `AGENTS.md` 定义该目录专属规则