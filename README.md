# Insula

English | [中文](README.zh.md)

Insula (`insula`) is an open-source multi-tenant agent service platform built on
[Cordis](https://github.com/corecraft-io/cordis) — a Go implementation of the
spatiotemporal composability model described in
[_A Programming Paradigm for Spatiotemporal Composability_](https://arxiv.org/abs/2608.25512).

It is built on a **tenant-is-a-realm** architecture: every tenant gets a private
isolate realm, shards bound the blast radius, and the data plane never enters the
runtime scheduler.

## Developer preview

Insula is in _developer preview_ and iterating rapidly.
**THERE WILL BE COMPATIBILITY-BREAKING CHANGES.**

Review the [safety notice](SAFETY.md) before running the project.

## Run

### Run from source

```sh
git clone https://github.com/corecraft-io/insula.git
cd insula
go test -race ./...
go run ./cmd/insula demo
```

The first build fetches `cordis` — the only dependency — from the module proxy.

`demo` needs no network at run time and no credentials — it assembles the platform
on real loopback listeners and walks the whole path (`/v1/runs` with a tool call,
identity stripping, capability snapshot, isolation self-check, metrics, audit,
graceful shutdown), printing each step as it goes.

To run the HTTP service, `serve` requires an explicit `-dev` acknowledgement,
because this repository ships no production adapters for the model upstream,
the credential provider, or authentication:

```sh
go run ./cmd/insula serve -dev
#   -addr   127.0.0.1:8080   HTTP listener
#   -admin  127.0.0.1:8081   admin listener (/metrics, /healthz; empty disables)
#   -shards 4                shard count
```

Then:

```sh
TOKEN=dev-token-tenant-a
curl -s -XPOST localhost:8080/v1/runs -H "Authorization: Bearer $TOKEN" \
  -d '{"input":"/tool hi"}'                      # tool path
curl -s localhost:8080/v1/tenants/self -H "Authorization: Bearer $TOKEN"
curl -s localhost:8081/metrics
```

## Use as a dependency

```sh
go get github.com/corecraft-io/insula
```

```go
import insula "github.com/corecraft-io/insula"
```

`cordis` is the only dependency, and it comes from the module proxy like any
other. To work on insula and cordis at the same time, use a Go workspace instead
of a `replace` directive — `replace` is ignored when a module is used as a
dependency, and a local one also breaks `go install`. Put this in a `go.work`
**above both checkouts**, outside either repository:

```
// go.work
go 1.22

use (
    ./cordis
    ./insula
)
```

## Architecture

`insula` is the assembly root and operations façade. Everything else is a
subpackage with a one-line role:

```
shard/    shard pool + tenant-sticky routing
tenant/   tenant model, provisioning, entry-subtree construction
realm/    isolate-realm construction and invariant assertions
admit/    admission control: token bucket, concurrency cap, platform total
caps/     capability handles (model gateway / memory / tool registry)
agent/    agent main loop (data plane)
gateway/  model gateway: process-wide pool + per-tenant accounting
memory/   session history, long-term memory, vector namespaces
session/  session lifecycle and persistence
tools/    tool registry and invocation
sandbox/  out-of-process execution of untrusted code
guard/    loop and tool guards
creds/    short-lived credential handles
audit/    audit log and per-tenant usage accounting
metrics/  observability: per-tenant RED metrics
edge/     the only external surface: auth, admission, HTTP/SSE
cmd/insula/  process entry point (serve / demo)
```

Three boundaries are worth knowing before reading the code:

- **Control plane vs. data plane.** cordis owns assembly, lifecycle, and
  isolation; the agent loop runs on plain Go goroutines. A slow upstream call
  from one tenant must not freeze a shard.
- **Operations vs. the world.** `Handler()` is what the outside gets; the
  `Workspace` interface the edge layer holds declares only `Capabilities` and
  `Runtime` — `Provision` is not in its type, so "an HTTP handler that can
  deprovision a tenant" is a compile error rather than a review item.
- **Shared vs. isolated.** Model pool, memory store, admission, metrics, and
  audit are process-level singletons; each shard registers only a thin handle.
  Isolation is a property of the storage structure itself (tenant is a
  first-class index dimension), not an attribute maintained by convention.

See [SAFETY.md](SAFETY.md) for the non-negotiable rules and the trust tiers,
[BENCHMARK.md](BENCHMARK.md) for the performance gate and its measurement
discipline, and [`docs/adr/`](docs/adr/README.md) for the seven architecture
decisions these boundaries come from.

## Community and support

- Submit feedback or bug reports through
  [GitHub Discussions](https://github.com/corecraft-io/insula/discussions).
- Add the [`insula-plugin`](https://github.com/topics/insula-plugin) topic to
  your plugin repository for discoverability.

## Citation

```bibtex
@misc{insula2026,
  title={Insula: Every Tenant is a Realm},
  author={corecraft-io},
  year={2026},
  publisher={GitHub},
  howpublished={\url{https://github.com/corecraft-io/insula}},
}
```

## License

[Apache License 2.0](LICENSE)
