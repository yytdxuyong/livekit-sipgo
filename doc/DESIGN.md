# livekit-sipgo SIP 协议栈设计文档

本文档详细分析 **livekit-sipgo**（`github.com/livekit/sipgo`）SIP 协议栈的实现架构。sipgo 是 LiveKit 生态中 SIP 协议栈的 Go 实现，基于上游 `emiago/sipgo` 封装并添加了 LiveKit 特定的功能层。

---

## 目录

1. [概述](#1-概述)
2. [整体架构](#2-整体架构)
3. [SIP 消息层 — sip 包](#3-sip-消息层--sip-包)
4. [传输层 — transport 包](#4-传输层--transport-包)
5. [事务层 — transaction 包](#5-事务层--transaction-包)
6. [UA / Server / Client — sipgo 包](#6-ua--server--client--sipgo-包)
7. [Dialog 支持 — ServerDialog](#7-dialog-支持--serverdialog)
8. [RFC 3261 定时器实现](#8-rfc-3261-定时器实现)
9. [与上游 emiago/sipgo 的关系](#9-与上游-emigagosipgo-的关系)
10. [关键设计决策](#10-关键设计决策)

---

## 1. 概述

### 1.1 项目定位

livekit-sipgo 是一个**薄封装层**，基于上游 `github.com/emiago/sipgo`（v0.24.1）构建，提供：

- **SIP 消息类型** — 复用上游的 Request、Response、Header、URI、Parser 等
- **自定义传输层** — UDP（含 MTU 管理）、TCP、TLS、WS、WSS
- **自定义事务层** — 完整 RFC 3261 状态机 + RFC 6026 扩展
- **UA/Server/Client** — 顶层应用接口
- **Dialog 支持** — 通话/会话生命周期管理

### 1.2 设计目标

| 目标 | 说明 |
|------|------|
| **薄封装** | 尽量复用上游的 SIP 解析和消息类型，仅封装 LiveKit 需要的额外功能 |
| **独立传输/事务层** | 将传输和事务从上游的 sip 子包提升为独立包，方便 livekit-sip 集成 |
| **RFC 6026 支持** | 完整实现 Accepted 状态，支持 2xx 响应的特殊处理 |
| **零额外依赖** | 除上游和 ws 库外，不引入额外第三方依赖 |

### 1.3 README 说明

> "LiveKit wrapper on top of emiago/sipgo. It will be gone once all changes are merged upstream. For now, this package preserves an older version of signaling functions, while using the upstream SIP parser, formatter and the message types."

### 1.4 代码仓库

| 信息 | 值 |
|------|-----|
| **GitHub** | `https://github.com/livekit/sipgo` |
| **上游** | `github.com/emiago/sipgo` v0.24.1 |
| **语言** | Go 1.22+ |
| **本地路径** | `d:\xuyong\source\AITest\livekit\livekit-sipgo` |

---

## 2. 整体架构

### 2.1 分层架构

```
┌─────────────────────────────────────────────────────────┐
│  应用层 (livekit-sip)                                     │
│  Service / Server / Client / inboundCall / outboundCall   │
├─────────────────────────────────────────────────────────┤
│              UA 层 (sipgo package)                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────────┐          │
│  │ UserAgent│  │  Server  │  │ ServerDialog │          │
│  │          │  │          │  │              │          │
│  │ ip/tls   │  │ OnInvite │  │ OnDialog     │          │
│  │ transport│  │ OnBye    │  │ Dialog FSM   │          │
│  │ transact │  │ OnAck    │  │              │          │
│  └────┬─────┘  └────┬─────┘  └──────┬───────┘          │
│       │             │               │                    │
│  ┌────▼─────────────▼───────────────▼───────┐           │
│  │            Client (sipgo)                 │           │
│  │  TransactionRequest / WriteRequest        │           │
│  │  自动补充 Via/From/To/CallID/CSeq 等 Header│          │
│  └──────────────────────────────────────────┘           │
├─────────────────────────────────────────────────────────┤
│          事务层 (transaction package)                     │
│  ┌──────────────────────────────────────────────────┐   │
│  │  Layer: handleMessage → 分发 Request/Response    │   │
│  │           ServerTx / ClientTx 创建与管理         │   │
│  │                                                  │   │
│  │  ┌────────────────────┐ ┌────────────────────┐  │   │
│  │  │  ServerTx          │ │  ClientTx           │  │   │
│  │  │  INVITE FSM        │ │  INVITE FSM        │  │   │
│  │  │  Non-INVITE FSM    │ │  Non-INVITE FSM    │  │   │
│  │  │  RFC 6026 Accepted │ │  RFC 6026 Accepted │  │   │
│  │  │  Timer G/H/I/J/L   │ │  Timer A/B/D/M     │  │   │
│  │  └────────────────────┘ └────────────────────┘  │   │
│  └──────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────┤
│          传输层 (transport package)                       │
│  ┌──────────────────────────────────────────────────┐   │
│  │  Layer: 消息路由 + 连接管理                        │   │
│  │                                                   │   │
│  │  ┌──────┐ ┌──────┐ ┌──────┐ ┌────┐ ┌────┐     │   │
│  │  │ UDP  │ │ TCP  │ │ TLS  │ │ WS │ │ WSS│     │   │
│  │  └──────┘ └──────┘ └──────┘ └────┘ └────┘     │   │
│  │                                                   │   │
│  │  ConnectionPool (连接复用 + 引用计数)              │   │
│  │  bufPool (bytes.Buffer sync.Pool)                 │   │
│  └──────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────┤
│         SIP 消息层 (sip package)                         │
│  类型重导出 + 常量 + 工具函数                              │
│  实际解析委托给 emiago/sipgo/sip 的 Parser               │
└─────────────────────────────────────────────────────────┘
```

### 2.2 包依赖关系

```
sipgo (根包)
 ├── sip/              ← 类型重导出层（委托 emiago/sipgo/sip）
 ├── transport/        ← 传输层（独立实现）
 └── transaction/      ← 事务层（独立实现）

依赖层级:
 sipgo → transaction → transport → sip → emiago/sipgo/sip
  (UA)     (事务FSM)    (网络I/O)   (类型导出) (上游解析器)
```

### 2.3 文件清单

```
livekit-sipgo/
│
├── ua.go              # UserAgent — 顶层管理器，创建传输层和事务层
├── server.go          # Server — SIP 服务端，请求注册和回调
├── server_dialog.go   # ServerDialog — 支持 Dialog 管理的扩展 Server
├── client.go          # Client — SIP 客户端，发送请求和自动 Header 补充
├── client_test.go     # 客户端测试
├── server_test.go     # 服务端测试
├── server_integration_test.go # 集成测试
├── server_dialog_test.go     # Dialog 测试
│
├── sip/               # SIP 消息类型 — 重导出层
│   ├── sip.go         #   常量（端口、Method、StatusCode）、工具函数
│   ├── message.go     #   Message 接口（委托上游）
│   ├── request.go     #   Request 结构体（委托上游）
│   ├── response.go    #   Response 结构体（委托上游）
│   ├── headers.go     #   Header 类型（To/From/Via/Contact 等）
│   ├── header_params.go # Header 参数解析
│   ├── uri.go         #   URI 结构体
│   ├── transport.go   #   传输抽象（Addr 等）
│   ├── transaction.go #   事务接口（Transaction/ServerTransaction/ClientTransaction）
│   ├── dialog.go      #   Dialog 结构体
│   └── utils.go       #   工具函数（ResolveSelfIP、ASCIIToLower 等）
│
├── transaction/       # 事务层（RFC 3261 §17）
│   ├── layer.go       #   Layer — 事务层入口，消息分发
│   ├── transaction.go #   事务 Key 生成、定时器常量、事务存储
│   ├── tx.go          #   commonTx — 公共事务基类
│   ├── fsm.go         #   FSM 状态和输入定义
│   ├── server_tx.go   #   ServerTx — 服务端事务
│   ├── server_tx_fsm.go # ServerTx FSM 状态机
│   ├── client_tx.go   #   ClientTx — 客户端事务
│   └── client_tx_fsm.go # ClientTx FSM 状态机
│
├── transport/         # 传输层
│   ├── layer.go       #   Layer — 传输层入口，消息路由
│   ├── transport.go   #   Transport 接口 + Addr 定义
│   ├── conn.go        #   Connection 接口 + bufPool
│   ├── connection_pool.go # 连接池（引用计数）
│   ├── udp.go         #   UDP 传输（MTU 管理、多 reader）
│   ├── tcp.go         #   TCP 传输（连接池复用）
│   ├── tls.go         #   TLS 传输
│   ├── ws.go          #   WebSocket 传输
│   └── wss.go         #   WSS 安全 WebSocket 传输
│
├── fakes/             # 测试用 mock
│   ├── conn.go
│   ├── udp_conn.go
│   └── tcp_conn.go
│
├── go.mod             # 依赖: emiago/sipgo v0.24.1
└── testdata/          # TLS 证书生成脚本
```

### 2.4 核心依赖

```go
// go.mod
require (
    github.com/emiago/sipgo v0.24.1    // 上游 SIP 解析器和消息类型
    github.com/gobwas/ws v1.4.0         // WebSocket 库
    github.com/google/uuid v1.6.0       // UUID 生成（Call-ID）
    github.com/prometheus/client_golang v1.12.0  // 传输层指标
)
```

---

## 3. SIP 消息层 — sip 包

### 3.1 设计模式

livekit-sipgo 的 `sip/` 包是**类型重导出层**，几乎所有类型定义都委托给 `emiago/sipgo/sip`：

```go
// sip/message.go
type Message = sipgo.Message                    // 类型别名
type RequestMethod = sipgo.RequestMethod

// sip/request.go
type Request = sipgo.Request                    // 完全委托
func NewRequest(method, recipient) *Request {
    return sipgo.NewRequest(method, recipient)  // 调用上游构造器
}

// sip/response.go
type Response = sipgo.Response
func NewResponse(statusCode, reason) *Response {
    return sipgo.NewResponse(statusCode, reason)
}
```

### 3.2 常量与工具函数

```go
// sip/sip.go — 端口常量
const (
    DefaultUdpPort = 5060
    DefaultTcpPort = 5060
    DefaultTlsPort = 5061
    DefaultWsPort  = 80
    DefaultWssPort = 443
    RFC3261BranchMagicCookie = "z9hG4bK"
)

// 工具函数
func GenerateBranch() string     // 生成唯一 branch ID
func GenerateBranchN(n int) string
func GenerateTagN(n int) string
func DefaultPort(transport string) int  // 根据传输协议返回默认端口
```

### 3.3 重导出的消息类型

| livekit-sipgo 类型 | 实际类型（来自上游） | 说明 |
|-------------------|-------------------|------|
| `Message` | `=` | 消息接口 |
| `*Request` | `=` | SIP 请求 |
| `*Response` | `=` | SIP 响应 |
| `Uri` | `=` | SIP URI |
| `Header` | `=` | 通用 Header |
| `ViaHeader` | `=` | Via 头 |
| `FromHeader` | `=` | From 头 |
| `ToHeader` | `=` | To 头 |
| `ContactHeader` | `=` | Contact 头 |
| `CallIDHeader` | `=` | Call-ID 头 |
| `CSeqHeader` | `=` | CSeq 头 |
| `RecordRouteHeader` | `=` | Record-Route 头 |
| `RouteHeader` | `=` | Route 头 |
| `MaxForwardsHeader` | `=` | Max-Forwards 头 |
| `Params` | `=` | Header 参数 |
| `Transaction` | `=` | 事务接口（来自上游 sip/transaction.go） |
| `ServerTransaction` | `=` | 服务端事务接口 |
| `ClientTransaction` | `=` | 客户端事务接口 |
| `Dialog` | `=` | Dialog 结构体 |

### 3.4 事务接口定义

livekit-sipgo 在 `sip/transaction.go` 中定义了事务接口，供上层使用：

```go
type Transaction interface {
    Terminate()
    Done() <-chan struct{}
    Err() error
}

type ServerTransaction interface {
    Transaction
    Respond(res *Response) error
    Acks() <-chan *Request
}

type ClientTransaction interface {
    Transaction
    Responses() <-chan *Response
}
```

> 这些接口的实现来自 **livekit-sipgo 自己的 transaction 包**，而非上游。这是关键设计：接口定义在 sip 包（供应用层使用），实现在 transaction 包（独立 FSM 实现）。

---

## 4. 传输层 — transport 包

### 4.1 架构

```go
type Layer struct {
    udp *UDPTransport
    tcp *TCPTransport
    tls *TLSTransport
    ws  *WSTransport
    wss *WSSTransport
    
    transports map[string]Transport  // 快速访问映射
    
    handlers   []sip.MessageHandler  // 消息处理回调链
    dnsResolver *net.Resolver
    
    ConnectionReuse bool              // 连接复用（默认 true）
}
```

### 4.2 Transport 接口

```go
type Transport interface {
    Network() string                                    // "UDP"/"TCP"/"TLS"/"WS"/"WSS"
    GetConnection(addr string) (Connection, error)      // 获取或复用连接
    CreateConnection(laddr, host, raddr, handler) (Connection, error)  // 创建新连接
    String() string
    Close() error
}
```

### 4.3 Connection 接口

```go
type Connection interface {
    LocalAddr() net.Addr
    WriteMsg(msg sip.Message) error     // 序列化并发送消息
    Ref(i int) int                      // 引用计数 +/-/查询
    TryClose() (int, error)             // 引用归零时关闭
    Close() error
}
```

### 4.4 连接池

```go
type ConnectionPool struct {
    sync.RWMutex
    m map[string]Connection  // 地址 → 连接
}
```

连接池使用**引用计数**管理连接生命周期：
- `pool.Add(addr, conn)` — 添加连接（默认为 ref=1）
- `pool.Get(addr)` — 获取连接（ref+1）
- `conn.TryClose()` — 尝试关闭（ref-1，归零时关闭）

### 4.5 传输协议详情

| 传输 | 实现 | 关键特性 |
|------|------|---------|
| **UDP** | `udp.go` | MTU 1500、单 reader goroutine（`UDPReadWorkers=1`）、连接池按地址复用 |
| **TCP** | `tcp.go` | Listen/Accept 模型、流式读取（stream parser）、连接池复用 |
| **TLS** | `tls.go` | 基于 TCP 封装、tls.Config 外部传入 |
| **WS** | `ws.go` | 使用 `gobwas/ws` 库、WebSocket 连接管理 |
| **WSS** | `wss.go` | WS + TLS |

#### UDP 传输核心

```go
type UDPTransport struct {
    parser    *sipgo.Parser
    pool      *ConnectionPool
    listeners []*UDPConnection
}
```

**UDP 消息接收循环**：
```
goroutine (UDPReadWorkers 个):
  for {
    buf := make([]byte, transportBufferSize) // 65535
    n, addr := conn.ReadFrom(buf)
    msg := parser.ParseSIP(buf[:n])
    msg.SetSource(addr)
    msg.SetTransport("UDP")
    for _, handler := range handlers {
        handler(msg)
    }
  }
```

**UDP MTU 检测**：
```
if len(data) > UDPMTUSize (1500) {
    return ErrUDPMTUCongestion  // 数据包超过 MTU
}
```

### 4.6 传输层消息流

```
网络接口
    │
    ▼
┌──────────────────────────┐
│ UDP/TCP/TLS/WS/WSS       │ ← 各 Transport 监听 goroutine
│   接收原始数据            │
└──────────┬───────────────┘
           │ []byte
           ▼
┌──────────────────────────┐
│   上游 Parser             │ ← emiago/sipgo/sip.Parser
│   ParseSIP(data) → Message│
└──────────┬───────────────┘
           │ *Request / *Response
           ▼
┌──────────────────────────┐
│   Layer.OnMessage →      │ ← 传输层 Layer
│   遍历 handlers 回调链    │
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────┐
│   transaction.Layer      │ ← 注册为第一个也是主要 handler
│   handleMessage          │
└──────────────────────────┘
```

---

## 5. 事务层 — transaction 包

### 5.1 Layer — 事务层核心

```go
type Layer struct {
    tpl                *transport.Layer
    reqHandler         RequestHandler
    unRespHandler      UnhandledResponseHandler
    clientTransactions *transactionStore
    serverTransactions *transactionStore
}
```

**消息分发逻辑**：

```go
func (txl *Layer) handleMessage(msg sip.Message) {
    switch msg := msg.(type) {
    case *sip.Request:
        // 1. 生成 ServerTx Key
        // 2. 尝试匹配已有 ServerTx
        // 3. 若匹配 → 交由 ServerTx.Receive() 处理（重传识别）
        // 4. 若不匹配且非 CANCEL → 创建新 ServerTx
        // 5. 若 CANCEL → 丢弃（对应事务已结束）
        // 6. 调用 reqHandler 回调
        txl.handleRequest(msg)
    
    case *sip.Response:
        // 1. 生成 ClientTx Key
        // 2. 尝试匹配已有 ClientTx
        // 3. 若匹配 → 交由 ClientTx.Receive() 处理
        // 4. 若不匹配 → unRespHandler（RFC 3261 §17.1.1.2 孤儿响应）
        txl.handleResponse(msg)
    }
}
```

### 5.2 事务 Key 生成

```go
// 服务端 Key: branch__host__port__method (RFC 3261)
// 或 fromTag__callId__method__cseq__via (RFC 2543 兼容)
func MakeServerTxKey(msg) (string, error) {
    via := msg.Via()
    branch := via.Params.Get("branch")
    if isRFC3261(branch) {
        return branch + "__" + via.Host + "__" + port + "__" + method
    }
    return fromTag + "__" + callId + "__" + method + "__" + cseq + "__" + via
}

// 客户端 Key: branch__method (RFC 3261)
func MakeClientTxKey(msg) (string, error) {
    return branch + "__" + method
}
```

### 5.3 ServerTx — 服务端事务 FSM

**状态机类型**（`fsm.go`）：

| 状态 | 使用场景 |
|------|---------|
| `server_state_trying` | 初始状态，等待用户响应 |
| `server_state_proceeding` | 已发送 1xx 响应 |
| `server_state_completed` | 已发送 300+ 最终响应 |
| `server_state_confirmed` | 已收到 ACK（仅 INVITE） |
| `server_state_accepted` | 已发送 2xx 响应（RFC 6026） |
| `server_state_terminated` | 已终止 |

**INVITE 服务端 FSM**：

```
                    ┌────────┐
                    │ TRYING │ ← 收到 INVITE（自动发送 100 Trying）
                    └───┬────┘
                        │ 用户发送 1xx/2xx/300+
                        ▼
              ┌─────────────────┐
              │   PROCEEDING    │ ← 1xx（可重传）
              └───┬──────┬──────┘
                  │      │
          2xx     │      │ 300+
                  ▼      ▼
         ┌──────────┐ ┌──────────┐
         │ ACCEPTED │ │COMPLETED │ ← Timer G（重传响应）
         │(RFC 6026)│ └────┬─────┘
         └────┬─────┘      │ ACK
              │            ▼
              │     ┌───────────┐
              │     │ CONFIRMED │ ← Timer I（T4）
              │     └─────┬─────┘
              │           │ Timer I 超时
              │           ▼
              │     ┌───────────┐
              └────→│ TERMINATED│
                    └───────────┘
```

### 5.4 ClientTx — 客户端事务 FSM

**INVITE 客户端 FSM**：

```
    ┌──────────┐
    │ CALLING  │ ← 发送 INVITE
    └───┬──────┘
        │ Timer A（UDP 重传）
        │ Timer B（超时 64*T1）
        │ 收到 1xx
        ▼
    ┌────────────┐
    │ PROCEEDING │ ← 继续等待最终响应
    └───┬────┬───┘
        │    │
 2xx    │    │ 300+
        ▼    ▼
  ┌──────────┐ ┌───────────┐
  │ ACCEPTED │ │ COMPLETED │ ← 发送 ACK
  │(RFC 6026)│ └─────┬─────┘
  └────┬─────┘       │ Timer D（32s）
       │ Timer M     ▼
       │ (64*T1) ┌───────────┐
       └────────→│ TERMINATED│
                 └───────────┘
```

### 5.5 ServerTx 初始化

```go
func (tx *ServerTx) Init() error {
    tx.initFSM()
    
    // 设置定时器
    if tx.reliable {
        tx.timer_i_time = 0       // 可靠传输无需 Timer I
    } else {
        tx.timer_g_time = Timer_G  // 非可靠传输启动重传定时器
        tx.timer_i_time = Timer_I
    }
    
    // 对 INVITE 自动启动 100 Trying 定时器
    if tx.Origin().IsInvite() {
        tx.timer_1xx = time.AfterFunc(Timer_1xx, func() {
            trying := sip.NewResponseFromRequest(tx.Origin(), 100, "Trying", nil)
            tx.Respond(trying)
        })
    }
    return nil
}
```

### 5.6 ClientTx 初始化

```go
func (tx *ClientTx) Init() error {
    tx.initFSM()
    
    // 发送请求
    tx.conn.WriteMsg(tx.origin)
    
    if !tx.reliable {
        tx.timer_a = time.AfterFunc(Timer_A, func() {
            tx.spinFsm(client_input_timer_a)  // 重传
        })
    }
    
    tx.timer_b = time.AfterFunc(Timer_B, func() {
        tx.lastErr = fmt.Errorf("Timer_B timed out. %w", ErrTimeout)
        tx.spinFsm(client_input_timer_b)  // 超时
    })
    return nil
}
```

### 5.7 FSM 实现模式

sipgo 使用**函数指针状态机**：

```go
// 状态函数签名
type FsmContextState func(s FsmInput) FsmInput

// 状态切换
func (tx *commonTx) spinFsm(in FsmInput) {
    tx.fsmMu.Lock()
    for i := in; i != FsmInputNone; {
        i = tx.fsmState(i)  // 当前状态函数处理输入，返回下一个状态函数
    }
    tx.fsmMu.Unlock()
}

// 状态函数示例
func (tx *ServerTx) inviteStateProceeding(s FsmInput) FsmInput {
    switch s {
    case server_input_request:
        tx.fsmState, _ = tx.inviteStateProcceeding, tx.actRespond
    case server_input_user_2xx:
        tx.fsmState, _ = tx.inviteStateAccepted, tx.actRespondAccept  // RFC 6026
    case server_input_user_300_plus:
        tx.fsmState, _ = tx.inviteStateCompleted, tx.actRespondComplete
    }
    return action()
}
```

---

## 6. UA / Server / Client — sipgo 包

### 6.1 UserAgent

```go
type UserAgent struct {
    name        string           // UA 名称
    ip          net.IP           // 本地 IP
    dnsResolver *net.Resolver    // DNS 解析器
    tlsConfig   *tls.Config      // TLS 配置
    tp          *transport.Layer // 传输层
    tx          *transaction.Layer // 事务层
}
```

**创建流程**：
```go
func NewUA(options ...UserAgentOption) (*UserAgent, error) {
    ua := &UserAgent{name: "sipgo", dnsResolver: net.DefaultResolver}
    for _, o := range options { o(ua) }
    
    if ua.ip == nil { ua.ip = sip.ResolveSelfIP() }  // 自动检测 IP
    
    ua.tp = transport.NewLayer(ua.dnsResolver, sipgo.NewParser(), ua.tlsConfig)
    ua.tx = transaction.NewLayer(ua.tp)
    return ua, nil
}
```

**关闭**：
```go
func (ua *UserAgent) Close() error {
    ua.tx.Close()     // 停止所有事务
    return ua.tp.Close()  // 关闭传输层
}
```

### 6.2 Server

```go
type Server struct {
    *UserAgent
    requestHandlers map[sip.RequestMethod]RequestHandler  // 方法→处理器映射
    noRouteHandler  RequestHandler    // 默认 405 回复
    requestMiddlewares  []func(r *sip.Request)
    responseMiddlewares []func(r *sip.Response)
    log *slog.Logger
}
```

**事件注册**：

```go
srv.OnInvite(handler)    // 注册 INVITE 处理
srv.OnBye(handler)       // 注册 BYE 处理
srv.OnAck(handler)       // 注册 ACK 处理
srv.OnCancel(handler)    // 注册 CANCEL 处理
srv.OnRegister(handler)  // 注册 REGISTER 处理
srv.OnOptions(handler)   // 注册 OPTIONS 处理
srv.OnNotify(handler)    // 注册 NOTIFY 处理
srv.OnRefer(handler)     // 注册 REFER 处理
srv.OnSubscribe(handler) // 注册 SUBSCRIBE 处理
srv.OnNoRoute(handler)   // 无匹配处理器时调用（默认 405）
```

**消息处理**：

```go
// 事务层回调 → onRequest
func (srv *Server) onRequest(req *sip.Request, tx sip.ServerTransaction) {
    go srv.handleRequest(req, tx)  // 每个请求单独 goroutine
}

func (srv *Server) handleRequest(req *sip.Request, tx sip.ServerTransaction) {
    for _, mid := range srv.requestMiddlewares { mid(req) }
    handler := srv.getHandler(req.Method)
    handler(req, tx)
    if tx != nil { tx.Terminate() }  // 防止事务泄漏
}
```

**网络监听**：

```go
// UDP
srv.ServeUDP(udpConn)   // 或 srv.ListenAndServe(ctx, "udp", ":5060")
// TCP
srv.ServeTCP(tcpListener)
// TLS
srv.ServeTLS(tlsListener)  // 或 srv.ListenAndServeTLS(ctx, "tls", ":5061", tlsConf)
// WS
srv.ServeWS(wsListener)
// WSS
srv.ServeWSS(wssListener)
```

### 6.3 Client

```go
type Client struct {
    *UserAgent
    host string     // 默认路由主机
    port int        // 默认路由端口
    log  *slog.Logger
}
```

**核心方法**：

```go
// 通过事务层发送请求（非 ACK）
func (c *Client) TransactionRequest(req, options) (ClientTransaction, error) {
    if len(options) == 0 {
        clientRequestBuildReq(c, req)  // 自动补充缺失 Header
    }
    return c.tx.Request(req)
}

// 直接发送（用于 ACK）
func (c *Client) WriteRequest(req, options) error {
    clientRequestBuildReq(c, req)
    return c.tp.WriteMsg(req)
}
```

**自动 Header 补充**（`clientRequestBuildReq`）：

```go
// RFC 3261 §8.1.1 要求:
// INVITE sip:user@host SIP/2.0
// Via: SIP/2.0/UDP 192.168.1.1:5060;branch=z9hG4bK...
// From: "sipgo" <sip:sipgo@192.168.1.1>;tag=xxx
// To: <sip:user@host>
// Call-ID: uuid@host
// CSeq: 1 INVITE
// Max-Forwards: 70
// Content-Length: 0
//
// 以下 Header 会被自动补充:
// - Via（含 branch 参数）
// - From（含 tag 参数）
// - To
// - Call-ID（UUID v4）
// - CSeq（SeqNo=1）
// - Max-Forwards（70）
// - Content-Length（0）
```

---

## 7. Dialog 支持 — ServerDialog

### 7.1 设计

```go
type ServerDialog struct {
    Server
    onDialog func(d sip.Dialog)  // Dialog 状态变更回调
}
```

### 7.2 Dialog 状态

```go
const (
    DialogStateEstablished = iota  // 收到 200 响应
    DialogStateConfirmed           // 收到 ACK
    DialogStateEnded               // 收到 BYE
)
```

### 7.3 Dialog 事件触发

```go
// ServerDialog 拦截以下事件发布 Dialog 状态:
// ACK  → DialogStateConfirmed
// BYE  → DialogStateEnded
// 200  → DialogStateEstablished

// 订阅方式:
srv.OnDialog(func(d sip.Dialog) {
    log.Printf("Dialog %s: %s", d.ID, d.StateString())
})
// 或通过 channel:
ch := make(chan sip.Dialog)
srv.OnDialogChan(ch)
```

### 7.4 实现机制

```go
// dialogServerTx 包装了 ServerTransaction，拦截 Respond() 调用
type dialogServerTx struct {
    sip.ServerTransaction
    s *ServerDialog
}

func (tx *dialogServerTx) Respond(r *sip.Response) error {
    if r.IsSuccess() {
        // 发送 200 时发布 DialogStateEstablished
        tx.s.publish(r, Dialog{State: DialogStateEstablished})
    }
    return tx.ServerTransaction.Respond(r)
}
```

---

## 8. RFC 3261 定时器实现

livekit-sipgo 严格遵守 RFC 3261 定义的 SIP 定时器：

### 8.1 定时器常量

| 定时器 | 默认值 | RFC 参考 | 用途 |
|--------|-------|---------|------|
| `T1` | 500ms | §17.1.1.1 | RTT 估计 |
| `T2` | 4s | §17.1.1.1 | 最大重传间隔 |
| `T4` | 5s | §17.1.1.1 | 网络最长存活时间 |
| `Timer_A` | T1 | §17.1.1.2 | 客户端 INVITE 重传 |
| `Timer_B` | 64×T1 | §17.1.1.2 | 客户端 INVITE 超时 |
| `Timer_D` | 32s | §17.1.1.2 | 客户端 Completed 超时 |
| `Timer_E` | T1 | §17.1.2.2 | 客户端非 INVITE 重传 |
| `Timer_F` | 64×T1 | §17.1.2.2 | 客户端非 INVITE 超时 |
| `Timer_G` | T1 | §17.2.1 | 服务端 300+ 重传 |
| `Timer_H` | 64×T1 | §17.2.1 | 服务端等待 ACK 超时 |
| `Timer_I` | T4 | §17.2.1 | 服务端 Confirmed 超时 |
| `Timer_J` | 64×T1 | §17.2.2 | 服务端非 INVITE 超时 |
| `Timer_K` | T4 | §17.2.2 | 客户端非 INVITE 超时 |
| `Timer_L` | 64×T1 | RFC 6026 | 服务端 Accepted 超时 |
| `Timer_M` | 64×T1 | RFC 6026 | 客户端 Accepted 超时 |
| `Timer_1xx` | 200ms | §17.2.1 | 自动 100 Trying |

### 8.2 定时器实现

所有定时器使用 `time.AfterFunc` 实现异步回调：

```go
// 客户端重传示例 (client_tx.go)
tx.timer_a = time.AfterFunc(tx.timer_a_time, func() {
    tx.spinFsm(client_input_timer_a)  // 触发重传（翻倍间隔，上限 T2）
})

// 服务端 100 Trying 示例 (server_tx.go)
tx.timer_1xx = time.AfterFunc(Timer_1xx, func() {
    trying := sip.NewResponseFromRequest(tx.Origin(), 100, "Trying", nil)
    tx.Respond(trying)
})
```

---

## 9. 与上游 emiago/sipgo 的关系

### 9.1 对比

| 维度 | emiago/sipgo v0.24.1 | livekit/sipgo |
|------|---------------------|---------------|
| **定位** | 通用 SIP 协议栈 | LiveKit 专用薄封装层 |
| **SIP 解析器** | 内置 | **复用上游** |
| **消息类型** | 内置 | **复用上游**（类型别名） |
| **传输层** | 内置在 sip 包 | **独立 transport 包** |
| **事务层** | 内置在 sip 包 | **独立 transaction 包** |
| **事务接口** | 内置 | **重新定义**在 sip/transaction.go |
| **Server/Client** | 嵌套在 sipgo 包 | 同左，但使用本地事务/传输层 |
| **RFC 6026** | 基础支持 | **完整支持**（Accepted 状态） |
| **Dialog** | 无 | **ServerDialog** 封装 |
| **WS/WSS** | 基础 | 使用 `gobwas/ws` 库 |
| **度量指标** | 无 | 传输层集成 Prometheus |

### 9.2 协作方式

```
应用层 (livekit-sip)
    │
    ▼
livekit/sipgo
    ├── sip/  ───── 类型别名 ────→ emiago/sipgo/sip (Parser, Request, Response, Header, URI)
    ├── transport/ ─── 独立传输层 ──→ 使用上游 Parser 解析数据
    ├── transaction/ ─ 独立事务层 ──→ 使用上游 Message 接口
    └── sipgo         ─ UA/Server/Client ──→ 使用上述各层
```

### 9.3 README 说明原文

> "LiveKit wrapper on top of emiago/sipgo. It will be gone once all changes are merged upstream."

说明此仓库的定位是**临时封装**，待 LiveKit 的修改合并到上游后，此仓库将被废弃。

---

## 10. 关键设计决策

1. **薄封装策略** — 不重复实现 SIP 消息解析和序列化，完全复用上游 `emiago/sipgo` 的 Parser 和消息类型，减少代码量和维护负担

2. **独立传输层** — 将 transport 从上游的 `sip/` 子包提升为独立包，便于 livekit-sip 直接调用传输层的 `ServeUDP/ServeTCP` 等方法管理网络监听

3. **独立事务层** — 同样将 transaction 提升为独立包，支持 RFC 6026 扩展（Accepted 状态 + Timer_L/Timer_M），livekit-sip 不需要

4. **函数指针 FSM** — 状态机使用函数指针而非 switch-case，每个状态是一个函数（`FsmContextState`），输入产生新状态，代码清晰且易于扩展

5. **100 Trying 自动响应** — ServerTx 自动为 INVITE 启动 200ms 定时器发送 100 Trying，符合 RFC 3261 要求，应用层无需手动处理

6. **连接池引用计数** — Connection 接口通过 `Ref()` / `TryClose()` 管理引用计数，确保 TCP/TLS 连接在事务结束后不被过早关闭

7. **UDP 单 reader 设计** — `UDPReadWorkers=1`，单 goroutine 读取 UDP 避免并发读取导致的顺序问题。实测中低并发 worker 性能更优

8. **ServerDialog 的拦截模式** — 通过包装 `ServerTransaction`（`dialogServerTx`），在 `Respond()` 时自动发布 Dialog 事件，无需应用层额外代码

9. **TCP 流式读取** — TCP 传输依赖 Content-Length Header 确定消息边界，支持同一条 TCP 连接上的多条 SIP 消息

10. **Proxy 支持** — Client 的 `ClientRequestAddRecordRoute` 和 `ClientRequestDecreaseMaxForward` 方法支持 SIP 代理场景，但当前 livekit-sip 主要使用 B2BUA 模式