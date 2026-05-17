# 黑马点评（学习笔记项目）

本仓库为 B 站「黑马程序员」开源实战项目 **黑马点评**（HMDP）的学习与练习代码整理。

- 学习时间：**2024 年 9–10 月**
- 课程/项目来源（公开信息）：GitHub 上的公开仓库 `cs001020/hmdp` 的 README 明确说明该项目来自 B 站黑马程序员 Redis 教程，并给出视频地址 **BV1cr4y1671t**（实战篇从 P24 起）。

## 重要说明（脱敏与合规）

为了避免泄露任何敏感信息，本仓库已做如下处理：

- 已移除/替换历史提交中的数据库、Redis 等连接信息中的公网 IP、账号与口令（改为本地地址或环境变量占位）。
- `src/main/resources/db/hmdp.sql` 中的测试手机号已统一替换为示例值，避免包含真实个人信息。

## 快速开始

### 1）准备依赖

- JDK 8（项目 `pom.xml` 标注 Java 1.8）
- MySQL（导入 `src/main/resources/db/hmdp.sql`）
- Redis

### 2）配置

本项目使用环境变量读取本地密钥/连接信息，示例见：

- `.env.example`
- `src/main/resources/application-local.yaml`

你可以创建 `.env`（不要提交），并设置例如：

```
HMDP_DB_URL=jdbc:mysql://127.0.0.1:3306/hmdp?useSSL=false&serverTimezone=UTC
HMDP_DB_USERNAME=root
HMDP_DB_PASSWORD=你的密码
HMDP_REDIS_HOST=127.0.0.1
HMDP_REDIS_PORT=6379
HMDP_REDIS_PASSWORD=
```

### 3）运行

- 启动 Redis、MySQL
- 运行 `com.hmdp.HmDianPingApplication`

## 文档

- 详细说明见：`docs/ABOUT.md`

