# Safety notice

English | [中文](SAFETY.zh.md)

Insula is in _developer preview_. This document is normative: it states which
safety properties the platform actually enforces, and — more importantly —
which ones it does not.

## Read this first: what realm isolation is *not*

`cordis`'s isolate realms (`Context.Get`'s realm check, `Reflect`'s realm-keyed
index) are a **correctness guarantee**, not a **security boundary**. They ensure
that a component you wrote cannot accidentally receive another tenant's instance
because two services happened to share a name.

They do not stop malicious Go code. Every package in this repository runs in one
process, and in-process Go code can reach around realm checks through reflection,
`unsafe`, a global singleton, a shared `*http.Client`, or a shared logger. The Go
language offers no way to prevent that.

The practical consequence is the trust-tier model below: realm isolation is
sufficient for code you trust to be *mistaken*, and is never sufficient for code
you must assume to be *hostile*.

## Trust tiers

| Tier | Typical source | Isolation | Covers |
| --- | --- | --- | --- |
| **1 · Trusted** | Platform-authored plugins, the core agent loop, model-gateway adapters | In-process + private realm + code review | The large majority of real needs. Realm isolation is already enough here, because the failure being prevented is "someone wrote it wrong" |
| **2 · Semi-trusted** | Third-party tool adapters, plugins contributed by internal teams | In-process + realm isolation + capability minimisation (`Inject` declares only the services actually needed) + egress audit | Needs a **capability-list review**: which `Inject` entries does this plugin declare? Does it get `models` or a credential handle? |
| **3 · Untrusted** | User-uploaded tool code, arbitrary-code-execution tools, external MCP services | **Must be out-of-process**: gRPC / WASM sandbox, no credentials, no network (or a whitelisted egress). In-process there is only a per-tenant `tool-proxy` service holding the identity | The sandbox process does not know which tenant it is; the host-side proxy injects identity at call time |

Tier 3's rule is not a policy switch. It follows from the language fact above,
and it is encoded as such — `sandbox.Tier.AllowsInProcess` returns `false` for
`TierUntrusted` with no option to override it.

## The four hard rules

These hold regardless of trust tier. They are the rules that a platform built on
cordis must add itself, because cordis has no opinion about any of them.

### R1 · Identity is never model-specified

`tenant_id` / `session_id` / `user_id` — and every spelling-normalised variant —
arriving in a tool's arguments are **discarded and then overwritten by the
runtime**. This is the most direct privilege-escalation path through prompt
injection: a model that can name the tenant it is acting for is a model that can
name someone else's.

Enforcement: `tools.IdentityStripper` (keys from `tools.DefaultIdentityKeys`,
matched after `tools.NormalizeKey`, so `tenant_id`, `tenantId`, `TENANT-ID` all
normalise to the same key) runs inside `Registry.Call`, recursively through
nested structures. At the request boundary, `edge` additionally *detects* — and
audits — a body that carried identity fields, counting them in
`Server.ForgedIdentityAttempts`. Detection subtracts `RunRequest`'s own field
names (derived by reflection over its JSON tags) from the shared key set, so a
legitimate `session` field is not reported as forgery while a smuggled
`tenant_id` is.

### R2 · Credentials never touch disk, logs, or prompts

The process holds only short-lived handles. A handle's struct has no plaintext
field, only an opaque handle ID generated with `crypto/rand`; plaintext leaves
the package for the duration of a `Resolve` call and nowhere else. The default
TTL is 15 minutes — a leak's window is the TTL, not "until somebody notices".

The model gateway injects credentials **server-side**. The model never sees them.
`creds.Redact` exists because handle IDs are themselves bearer capabilities: they
must not appear in `String()` output or in error messages verbatim.

Enforcement: `creds` (see its package doc for the three rules it specialises from
this one), `gateway`.

### R3 · Shared realms (`@label`) are disabled at the platform layer

`Isolate` accepts only `true`. A string label is a shared realm — a backdoor for
cross-tenant service sharing. Unless there is an explicit, reviewed requirement
for a shared service, the capability is simply not granted.

Enforcement, primary: `realm.Tenant()` and `realm.Session()` are the only places
in the platform that emit `Isolate` literals, and both hard-code `true`. The
rule holds **by construction**, not by inspection — that is what
`realm`'s package comment and `TestTenantIsolateCoversEveryService` are for.

Enforcement, secondary: construction is not enough on its own, because a
hand-built entry could still slip a label in and `cordis` would honour it
silently (a string becomes a `@label` shared realm; `false` drops the entry out
of the enclosing realm; any other type reads as "no declaration"). So there is
a runtime layer: `realm.CheckIsolateDeclarations` walks a tenant's whole entry
subtree and rejects **any** `Isolate` value that is not literally `true`.
`shard.Pool.SelfCheck` runs it for every tenant, reports the number of
declarations scanned as `SelfCheckReport.Declarations`, and records a breach in
the metrics on failure.

That count is not decoration: without it, "the declaration layer ran and was
clean" and "the declaration layer never ran" are identical in the report, and
those two mean opposite things. `realm.SharedRealmLabel` is the narrower
predicate (`is this value a string label?`) kept for callers that only care
about that question; the enforcement uses the stricter rule.

### R4 · Cross-tenant resources must be explicit platform services

A shared vector-store pool, a shared model pool — these are registered as
platform-level singletons (the in-process shared layer) and exposed to cordis as
a **thin handle**. They are never reached by tenants through a shared realm to
grab someone else's instance.

Enforcement: the model pool, memory store, admission controller, metrics
registry, and audit sink are process-level singletons constructed once in
`insula.Open`; each shard registers a handle to the same instance. This is why
platform-level quotas must be global — a per-shard admission controller would
turn "the platform total" into N times the total.

## What this repository does not do

Stated plainly, so that nobody discovers it in production:

- **No production adapters.** `serve` refuses to start without `-dev`. The model
  upstream, the credential provider, and the authenticator all point into the
  outside world, and the platform cannot decide on the deployer's behalf where
  they come from. Passing credentials as CLI flags is specifically not supported:
  they would be visible to `ps`.
- **No persistent storage backend.** `memory` and `session` ship in-memory
  implementations. Row-level security is a property of wherever you put the real
  data; this repository cannot assert it for you.
- **No process isolation for tier 3.** `sandbox` defines the `Executor` interface,
  the tier dispatch, and output truncation — but no sandbox runtime. The
  out-of-process executor is the deployer's to supply.
- **No certificate/mTLS, no rate limiting by source IP, no WAF.** `edge` does
  bearer authentication, per-tenant admission, and body-size caps. Everything
  upstream of that is yours.
- **Metrics output has no `# HELP` / `# TYPE` headers.** `metrics.Registry.Render`
  emits bare sample lines. The format is valid (a missing `TYPE` means `untyped`),
  but tooling that discovers metric families by parsing `# TYPE` will see zero
  families.
- **Isolation self-check granularity.** `shard.SelfCheck` compares tenant pairs
  that actually co-reside in a shard. With more shards than tenants no shard has
  two tenants, so `PairsChecked == 0` and no checks run. `PairsChecked == 0` is
  therefore ambiguous between "no comparable pairs" and "the self-check did not
  run"; distinguishing those needs a separate invocation counter.

## Reporting a vulnerability

Report security issues through
[GitHub Security Advisories](https://github.com/corecraft-io/insula/security/advisories/new)
rather than a public issue. Please include the version or commit, a minimal
reproduction, and which of the properties above you believe is violated.

## Non-negotiables

The following are not tunable parameters. Changing them is not a configuration
decision, it is a redesign:

1. cordis sits on the control plane; the data plane runs off the runtime scheduler.
2. Tier-3 code executes out of process.
3. Tenant identity is a required parameter and is never model-specified.

Shard count and hot/cold tenant policy, by contrast, *are* tunable and should
follow the measured numbers in [BENCHMARK.md](BENCHMARK.md).

## Where each rule came from

This notice states *what* must hold. The reasoning behind each rule — including
the cost each one accepted — is in the ADRs:

| Rule / tier | ADR |
| --- | --- |
| R1 · identity is never model-specified | [0005](docs/adr/0005-tenant-identity-is-an-explicit-parameter.md) |
| R2 · credentials never on disk, in logs, or in prompts | [0005](docs/adr/0005-tenant-identity-is-an-explicit-parameter.md) |
| R3 · shared realms disabled at the platform layer | [0003](docs/adr/0003-tenant-is-an-entry-subtree.md) |
| R4 · cross-tenant resources are explicit platform services | [0002](docs/adr/0002-app-shard-pool.md) |
| Trust tiers / tier 3 must leave the process | [0006](docs/adr/0006-untrusted-code-runs-out-of-process.md) |
| Data plane off the scheduler | [0001](docs/adr/0001-cordis-is-the-control-plane.md) |
| Request-scope state and identity are split | [0004](docs/adr/0004-request-scope-stays-in-go-context.md) |
