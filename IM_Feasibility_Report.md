# 基于 Netty+Protobuf 项目搭建高可用可扩展 IM 服务 —— 可行性报告

> 评审对象：`nettyProtobuf` 与 `wsHtml` Demo 工程
> 目标：从当前单节点原型演进为支持高可用、可水平扩展的生产级即时通讯（IM）服务

---

## 一、项目现状分析

### 1.1 当前已具备的能力

| 模块 | 文件 | 能力描述 |
|------|------|----------|
| TCP 长连接服务端 | [ReqServer.java](file:///workspace/nettyProtobuf/src/Serialization_ProtoBuf/Server/ReqServer.java) | 基于 Netty Reactor 模型，支持 Protobuf 编解码与半包处理（`ProtobufVarint32FrameDecoder`） |
| TCP 长连接客户端 | [ReqClient.java](file:///workspace/nettyProtobuf/src/Serialization_ProtoBuf/Client/ReqClient.java) | 同步连接、发送 `Person` 消息 |
| WebSocket 服务端 | [NettyServer.java](file:///workspace/nettyProtobuf/src/Websocket/NettyServer.java) | 监听 7397 端口，支持 HTTP 升级 WebSocket |
| WebSocket 处理器 | [MyWebSocketServerHandler.java](file:///workspace/nettyProtobuf/src/Websocket/MyWebSocketServerHandler.java) | 支持文本帧群发（`Global.group`）与二进制帧 Protobuf 解析 |
| 协议定义 | [Message.proto](file:///workspace/nettyProtobuf/src/Serialization_ProtoBuf/ProtoBuf/Message.proto) | `WSMessage { id, content, sender, time }` |
| Web 客户端 | [Client.html](file:///workspace/wsHtml/Client.html)、`PFClient.html` | 浏览器直连 WebSocket，演示文本与 Protobuf 收发 |

### 1.2 核心技术栈（当前版本）

- Netty `5.0.0.Alpha2`（Alpha 版，已停止维护）
- protobuf-java `2.6.1`（2015 年版本）
- 纯 JVM 内存 `ChannelGroup` 管理连接
- 无任何持久化、鉴权、集群能力

### 1.3 现状差距（面向高可用可扩展 IM）

| 维度 | 现状 | 生产级 IM 要求 | 差距 |
|------|------|----------------|------|
| **高可用** | 单进程、单端口，无容灾 | 多节点 + 负载均衡 + 故障转移 | ❌ 完全缺失 |
| **可扩展性** | 连接/消息全部存单 JVM 内存 | 连接层无状态化、水平扩展 | ❌ 完全缺失 |
| **消息可靠性** | 发后即弃，无 ACK、无重试 | 可靠投递（at-least-once）、幂等、有序 | ❌ 缺失 |
| **离线消息** | 无 | 离线推送、拉取历史 | ❌ 缺失 |
| **鉴权** | 握手即连接，无身份校验 | Token 鉴权、黑名单、限流 | ❌ 缺失 |
| **跨节点路由** | `Global.group` 仅本进程群发 | 用户↔节点映射、跨节点 Pub/Sub | ❌ 缺失 |
| **协议** | 仅 4 字段，无类型/状态 | 多消息类型、已读回执、心跳、系统通知 | ⚠️ 不足 |
| **存储** | 无 | 用户/会话/消息/群组持久化 | ❌ 缺失 |
| **监控** | 仅 `System.out` | 指标、链路、日志聚合 | ❌ 缺失 |

---

## 二、可行性结论

**结论：技术路线可行，建议升级改造后落地。**

当前项目已验证了「Netty + Protobuf + WebSocket」这一核心通信链路，这恰好是 IM 服务的关键基础设施。但要达到**高可用 + 可扩展**的目标，需要在现有 Demo 基础上进行系统性重构，而非简单增量开发。

可行性依据：
1. **技术成熟**：Netty 是业界 IM 长连接的事实标准（微信、网易云信、融云等均基于 Netty）。
2. **协议可演进**：Protobuf 天然支持协议向前/向后兼容，适合 IM 多端通信。
3. **扩展空间大**：当前代码体量小，重构成本低于维护一个陈旧单节点系统。
4. **生态完善**：Redis/Kafka/Elasticsearch 等中间件均可无缝接入。

---

## 三、目标架构设计

### 3.1 整体分层架构

```
                        ┌─────────────────────────────┐
                        │        客户端 (Web/App)      │
                        └──────────────┬──────────────┘
                                       │ TLS + WebSocket / TCP
                        ┌──────────────▼──────────────┐
                        │   接入层 (LB / 网关)          │
                        │  Nginx/HAProxy/LVS + 粘性会话 │
                        └──────────────┬──────────────┘
                                       │
              ┌────────────────────────┼────────────────────────┐
              │                        │                        │
   ┌──────────▼──────────┐  ┌──────────▼──────────┐  ┌──────────▼──────────┐
   │  连接网关节点 A       │  │  连接网关节点 B       │  │  连接网关节点 N       │
   │  (Netty WebSocket)   │  │  (Netty WebSocket)   │  │  (Netty WebSocket)   │
   │  无状态、水平扩展     │  │  无状态、水平扩展     │  │  无状态、水平扩展     │
   └──────────┬──────────┘  └──────────┬──────────┘  └──────────┬──────────┘
              │                        │                        │
              └────────────┬───────────┴────────────┬───────────┘
                           │                        │
                ┌──────────▼──────────┐  ┌──────────▼──────────┐
                │   Redis Cluster      │  │   消息队列           │
                │  路由表 + Pub/Sub    │  │  (Kafka/RocketMQ)    │
                │  userId → nodeId     │  │  削峰/解耦/异步持久化 │
                └──────────┬──────────┘  └──────────┬──────────┘
                           │                        │
              ┌────────────┼────────────┬───────────┼────────────┐
              │            │            │           │            │
   ┌──────────▼──┐ ┌──────▼───────┐ ┌──▼────────┐ ┌▼─────────┐ ┌▼──────────┐
   │  用户服务    │ │  消息服务     │ │  群组服务  │ │ 推送服务  │ │  搜索服务  │
   │  (鉴权/关系) │ │ (收发/存储)   │ │ (群管理)  │ │(离线推送) │ │ (ES 检索) │
   └──────┬──────┘ └──────┬───────┘ └────┬───────┘ └────┬─────┘ └─────┬─────┘
          │               │              │              │             │
   ┌──────▼───────────────▼──────────────▼──────────────▼─────────────▼─────┐
   │                         存储层                                          │
   │  MySQL(用户/会话/群)  |  MongoDB/Cassandra(消息历史)  |  Redis(缓存)    │
   └─────────────────────────────────────────────────────────────────────────┘
```

### 3.2 各层职责

| 层级 | 职责 | 关键技术 |
|------|------|----------|
| **接入层** | TLS 终结、限流、粘性路由、健康检查 | Nginx/HAProxy/LVS，`ip_hash` 或基于 `userId` 一致性哈希 |
| **连接网关层** | 长连接管理、编解码、心跳、协议解析、Ack | Netty 4.1.x（稳定版）+ Protobuf 3.x |
| **路由层** | `userId → gatewayNode` 映射、跨节点消息广播 | Redis Cluster + Pub/Sub |
| **业务服务层** | 鉴权、消息持久化、群组、离线推送、搜索 | Spring Boot 微服务 |
| **消息队列** | 异步解耦、削峰填谷、消息可靠投递 | Kafka / RocketMQ |
| **存储层** | 元数据 + 消息历史 + 缓存 | MySQL + MongoDB/Cassandra + Redis |

---

## 四、核心技术方案

### 4.1 连接网关无状态化（解决可扩展性）

**问题**：当前 `Global.group` 把所有连接存在单 JVM，无法扩展。

**方案**：
- 网关节点只负责「连接 + 编解码 + 转发」，不持有业务状态。
- 用户登录时，将 `userId → gatewayNodeId` 写入 Redis（带 TTL，心跳续期）。
- 发送消息时：
  1. 网关收到消息 → 查 Redis 拿到接收者所在节点
  2. 若接收者在本节点 → 直接投递；否则通过 Redis Pub/Sub 发布到目标节点频道
  3. 目标节点订阅自身频道，收到后本地 `ChannelGroup` 投递

```
发送方节点 A  ──Pub/Sub──▶  接收方节点 B
     │                          │
     └── 查 Redis: toUser→NodeB ─┘
```

### 4.2 高可用方案

| 单点风险 | 消除方案 |
|----------|----------|
| 接入 LB 单点 | LVS DR 集群 + 双机热备（Keepalived）或云厂商 SLB |
| 网关节点单点 | 多节点部署 + LB 健康检查，故障节点自动摘除 |
| Redis 单点 | Redis Sentinel（3 节点）或 Redis Cluster（6 节点起） |
| MySQL 单点 | 主从复制 + 读写分离 + MHA/Orchestrator 自动故障转移 |
| MQ 单点 | Kafka 多 Broker + 副本，或 RocketMQ 主从 |
| 消息历史库单点 | MongoDB 副本集 / Cassandra 多副本 |

### 4.3 消息可靠性（解决"发后即弃"）

引入 **消息 ID + ACK + 重试 + 去重** 机制：

1. **唯一消息 ID**：Snowflake 算法生成（趋势递增、全局唯一）。
2. **发送方 → 服务端 ACK**：客户端发送后等待服务端 `ACK(msgId)`，超时重发。
3. **服务端 → 接收方 ACK**：接收方收到后回 `ACK`，服务端未收到则推送至离线库。
4. **幂等去重**：服务端用 Redis `SETNX msgId` 去重，防止重发导致重复入库。
5. **消息状态机**：`SENT → DELIVERED → READ`，支持已读回执。

### 4.4 离线消息与历史

- 在线消息：实时投递（Redis Pub/Sub）。
- 离线消息：写入 MongoDB（或 Cassandra），用户上线后按 `lastAckMsgId` 拉取增量。
- 历史消息：MongoDB 分片（按 `conversationId` + 时间），支持分页拉取。
- 推送通知：离线时对接 APNs/FCM/厂商推送。

### 4.5 协议升级

当前 `WSMessage` 仅 4 字段，需扩展为通用 IM 协议：

```protobuf
syntax = "proto3";

message IMMsg {
  int64  msg_id      = 1;   // 全局唯一 ID (Snowflake)
  int64  from_user   = 2;   // 发送者
  int64  to_user     = 3;   // 接收者 (单聊)
  int64  group_id    = 4;   // 群 ID (群聊时填)
  int32  msg_type    = 5;   // 1文本 2图片 3语音 4视频 5系统 ...
  int32  conv_type   = 6;   // 1单聊 2群聊
  bytes  content     = 7;   // 消息体 (结构化嵌套)
  int64  timestamp   = 8;   // 服务端时间
  int32  status      = 9;   // 0未读 1已读 ...
  int64  client_seq  = 10;  // 客户端序列号 (客户端去重)
}

// 控制帧
message IMControl {
  int32  type        = 1;   // 1心跳 2登录 3登出 4ACK 5拉取历史 ...
  string token       = 2;
  int64  msg_id      = 3;   // ACK 时使用
  int64  last_seq    = 4;   // 拉取历史时使用
}
```

### 4.6 心跳与连接保活

- 客户端每 30s 发 Ping，服务端 10s 内未收到心跳则断开并清理路由表。
- Netty `IdleStateHandler` 实现：`readerIdleTime=60s, writerIdleTime=0`。
- 客户端断连自动重连 + 指数退避，重连后同步未确认消息。

### 4.7 鉴权与安全

- WebSocket 握手时携带 `token`（JWT），网关校验后才建立连接。
- TLS 加密（wss://）。
- 接口限流：Redis + 令牌桶，防消息轰炸。
- 内容安全：对接敏感词过滤服务。

---

## 五、技术栈升级建议

| 组件 | 当前版本 | 推荐版本 | 理由 |
|------|----------|----------|------|
| Netty | 5.0.0.Alpha2 | **4.1.x（最新稳定）** | 5.x Alpha 已废弃，4.1 长期维护 |
| Protobuf | 2.6.1 | **3.25.x** | 支持 `proto3`、官方维护、生态好 |
| 构建工具 | Eclipse `.project` | **Maven/Gradle** | 依赖管理、CI 友好 |
| 路由/缓存 | 无 | **Redis 7 Cluster** | Pub/Sub + 路由表 + 限流 |
| 消息队列 | 无 | **Kafka 3.x** 或 **RocketMQ** | 削峰、可靠投递、顺序消息 |
| 业务框架 | 无 | **Spring Boot 3.x** | 微服务生态 |
| 关系库 | 无 | **MySQL 8.0** | 用户/会话/群组元数据 |
| 消息库 | 无 | **MongoDB 7** 或 **Cassandra 4** | 海量消息历史、水平扩展 |
| 搜索 | 无 | **Elasticsearch 8** | 消息全文检索 |
| 注册中心 | 无 | **Nacos** 或 **Consul** | 服务发现 |
| 配置中心 | 无 | **Nacos** / Apollo | 动态配置 |
| 监控 | `System.out` | **Prometheus + Grafana** | 指标采集 |
| 日志 | 无 | **ELK / Loki** | 日志聚合 |
| 链路追踪 | 无 | **SkyWalking / Jaeger** | 分布式追踪 |
| 容器化 | 无 | **Docker + K8s** | 弹性伸缩、滚动发布 |

---

## 六、演进路线图（分阶段落地）

### 阶段一：基础稳固（2~3 周）
- [ ] 升级 Netty 至 4.1.x、Protobuf 至 3.x
- [ ] 迁移至 Maven/Gradle 工程
- [ ] 定义统一 IM 协议（`IMMsg`、`IMControl`）
- [ ] 实现心跳、登录鉴权（JWT）、ACK 机制
- [ ] 引入 Snowflake 消息 ID

### 阶段二：单节点完整版（3~4 周）
- [ ] 实现单聊/群聊消息收发
- [ ] 消息持久化（MySQL + MongoDB）
- [ ] 离线消息拉取
- [ ] 已读回执、消息状态机
- [ ] 基础监控指标（连接数、消息量）

### 阶段三：集群化与高可用（4~6 周）
- [ ] 网关无状态化改造
- [ ] Redis Cluster 路由表 + Pub/Sub 跨节点通信
- [ ] 接入 LB（Nginx/HAProxy）粘性会话
- [ ] Kafka 异步消息处理
- [ ] MySQL 主从、Redis Sentinel
- [ ] 压测：单节点 10w 连接、集群 100w 连接

### 阶段四：生产化增强（3~4 周）
- [ ] K8s 容器化部署、HPA 弹性伸缩
- [ ] 全链路监控（Prometheus + Grafana + SkyWalking）
- [ ] 消息搜索（Elasticsearch）
- [ ] 离线推送（APNs/FCM/厂商）
- [ ] 内容安全审核
- [ ] 灰度发布、多环境隔离

---

## 七、风险评估与应对

| 风险 | 等级 | 应对策略 |
|------|------|----------|
| Netty 5 Alpha 不兼容升级 | 中 | 直接切 4.1.x，API 差异通过重构解决 |
| 海量长连接内存压力 | 高 | 调优 JVM（堆外内存）、Linux `ulimit`、TCP 参数；单节点目标 10w 连接 |
| 消息顺序性 | 中 | 同一会话路由到同一 MQ 分区（Kafka `key=conversationId`） |
| Redis Pub/Sub 不持久 | 中 | Pub/Sub 仅用于实时投递；持久化走 MQ + DB，不丢消息 |
| 协议向后兼容 | 低 | Protobuf 3 天然支持，字段只增不删、不用 `required` |
| 运维复杂度上升 | 中 | K8s + Helm 统一编排，监控告警前置 |

---

## 八、资源与能力需求

| 角色 | 人数 | 职责 |
|------|------|------|
| 后端/IM 架构师 | 1 | 架构设计、核心模块 |
| Java 开发 | 2~3 | 网关、业务服务、存储 |
| 前端/客户端 | 1~2 | Web/App SDK、重连/ACK |
| 运维/DevOps | 1 | K8s、监控、中间件运维 |
| 测试 | 1 | 压测、可靠性测试 |

**预估周期**：4~5 个月达到生产可用（参考阶段一~四）。

---

## 九、关键性能指标（目标）

| 指标 | 目标值 |
|------|--------|
| 单网关节点并发连接 | 10 万（32C/64G） |
| 单节点消息吞吐 | 5 万 msg/s |
| 端到端消息延迟 | < 100ms（同机房） |
| 集群规模 | 100 万+ 长连接 |
| 可用性 | 99.95% |
| 消息可靠性 | 0 丢失（at-least-once + 去重） |

---

## 十、总结

1. **技术路线完全可行**：Netty + Protobuf 是 IM 长连接的成熟方案，当前 Demo 验证了核心链路。
2. **必须重构而非修补**：当前版本（Netty 5 Alpha、Protobuf 2.6、单节点内存）不具备生产可用性，建议升级到稳定版并按分层架构重建。
3. **核心改造点**：网关无状态化 + Redis 路由 + MQ 解耦 + 消息可靠投递 + 多层存储。
4. **建议分阶段交付**：先单节点跑通业务闭环，再做集群化，最后生产化增强，降低一次性上线风险。

---

*报告生成日期：2026-09-06*
