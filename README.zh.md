# Insula

[English](README.md) | 中文

Insula（`insula`）是建在 [Cordis](https://github.com/corecraft-io/cordis) 之上的开源多租户
agent 服务平台。Cordis 是《
[一种时空可组合的程序设计范式](https://arxiv.org/abs/2608.25512)》（_A Programming
Paradigm for Spatiotemporal Composability_）所述时空可组合组件模型的 Go 实现。

平台建立在 **每个租户都是一个域**（tenant-is-a-realm）这一架构公理上：每个租户获得一个
私有的隔离域，分片界定故障影响半径，数据面永不进入运行时调度器。

## 开发预览

Insula 处于**开发预览**阶段，迭代很快。
**会出现破坏兼容性的变更。**

运行本项目之前，请先阅读[安全须知](SAFETY.zh.md)。

## 运行

### 从源码运行

```sh
git clone https://github.com/corecraft-io/insula.git
cd insula
go test -race ./...
go run ./cmd/insula demo
```

首次构建会从模块代理取 `cordis`——它是本项目唯一的依赖。

`demo` 在运行时不触网、不需要凭证——它在真实的回环监听器上把平台装配起来，走完整条
链路（`/v1/runs` 的工具调用、身份字段剥离、能力快照、隔离自检、指标、审计、优雅停机），
并把每一步打印出来。

要运行 HTTP 服务，`serve` 需要显式加 `-dev` 确认，因为本仓库不附带模型上游、凭证提供者
与鉴权这三样生产适配器：

```sh
go run ./cmd/insula serve -dev
#   -addr   127.0.0.1:8080   HTTP 监听地址
#   -admin  127.0.0.1:8081   运维监听地址（/metrics、/healthz；空串关闭）
#   -shards 4                分片数
```

然后：

```sh
TOKEN=dev-token-tenant-a
curl -s -XPOST localhost:8080/v1/runs -H "Authorization: Bearer $TOKEN" \
  -d '{"input":"/tool hi"}'                      # 工具调用路径
curl -s localhost:8080/v1/tenants/self -H "Authorization: Bearer $TOKEN"
curl -s localhost:8081/metrics
```

## 作为依赖使用

```sh
go get github.com/corecraft-io/insula
```

```go
import insula "github.com/corecraft-io/insula"
```

`cordis` 是唯一的依赖，和其它模块一样由模块代理提供。要同时开发 insula 与 cordis，请用
Go workspace，而不是 `replace` 指令——模块被当作依赖使用时，`replace` 会被忽略；写在
发布版里的本地 `replace` 还会弄坏 `go install`。把下面这个文件放在两个检出目录**之上**、
两条仓库之外：

```
// go.work
go 1.22

use (
    ./cordis
    ./insula
)
```

平台装配的可运行示例在 [`example_test.go`](example_test.go)：它演示三个由外部提供的
依赖（`Upstream` / `Creds` / `Auth`）的形状、装配、以及发一次请求。
`go test -run Example .` 会真的把它跑一遍。

示例里的适配器是替身，理由与 `demo` 用桩件相同——本仓库不附带生产适配器。
抄形状，不要抄适配器。

## 架构

`insula` 是装配根与运维门面，其余每个子包各承担一行角色：

```
shard/   分片池与租户粘性路由
tenant/  租户模型、开通、入口子树构造
realm/   隔离域构造与不变量断言
admit/   准入：令牌桶、并发上限、平台级总量
caps/    能力句柄（模型网关 / 记忆 / 工具注册表）
agent/   agent 主循环（数据面）
gateway/ 模型网关：进程级共享池 + 租户额度分账
memory/  会话历史、长期记忆、向量命名空间
session/ 会话生命周期与持久化
tools/   工具注册表与调用
sandbox/ 不可信代码的进程外执行
guard/   循环守卫与工具守卫
creds/   凭证短期句柄
audit/   审计日志与租户用量分账
metrics/ 可观测性：租户级 RED 指标
edge/    唯一对外出口：鉴权、准入、HTTP/SSE
cmd/insula/  进程入口（serve / demo）
```

读代码之前值得先知道三条边界：

- **控制面与数据面分开。** cordis 负责装配、生命周期与隔离；agent 主循环跑在普通 Go
  goroutine 上。某个租户一次缓慢的上游调用不得冻结整个分片。
- **运维面与外部世界分开。** `Handler()` 是给外面的；接入层持有的 `Workspace` 接口只声明
  了 `Capabilities` 与 `Runtime`——`Provision` 不在它的类型里，所以「一个能注销租户的
  HTTP 处理器」是编译期错误，而不是一条代码评审事项。
- **共享与隔离分开。** 模型池、记忆存储、准入器、指标、审计是进程级单例，各分片只注册瘦
  句柄。隔离是存储结构本身的性质（租户是一级索引维度），而不是靠约定维持的属性。

不可协商的规则与信任分级见 [SAFETY.zh.md](SAFETY.zh.md)；性能门禁与取数纪律见
[BENCHMARK.zh.md](BENCHMARK.zh.md)；这些边界背后的八条架构决策见
[`docs/adr/`](docs/adr/README.zh.md)——其中 0008 仍是 Proposed，并且自己写明了这一点。

## 社区与支持

- 通过 [GitHub Discussions](https://github.com/corecraft-io/insula/discussions)
  提交反馈或缺陷报告。
- 给你自己的插件仓库加上
  [`insula-plugin`](https://github.com/topics/insula-plugin) 话题，便于被发现。

## 引用

```bibtex
@misc{insula2026,
  title={Insula: Every Tenant is a Realm},
  author={corecraft-io},
  year={2026},
  publisher={GitHub},
  howpublished={\url{https://github.com/corecraft-io/insula}},
}
```

## 许可

[Apache License 2.0](LICENSE)
