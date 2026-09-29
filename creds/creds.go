// Package creds 管理模型与工具凭证的短期句柄。
//
// # 三条硬规则（对应 SAFETY.md）
//
//  1. **凭证不落盘、不进日志、不进 prompt。** Handle 结构体里没有明文字段，
//     只有不透明的句柄 ID；明文只在 Resolve 调用的瞬间离开本包。
//  2. **句柄是不可伪造的能力。** 句柄 ID 用 crypto/rand 生成——
//     任何能猜出句柄 ID 的代码都能兑换明文。
//  3. **TTL 有界。** 默认 15 分钟。凭证泄漏的时间窗就是 TTL，
//     而不是「直到有人注意到」。
//
// 本包用内存实现（StaticProvider）以便本地运行与测试；
// 生产环境应替换为 KMS / Secrets Manager 适配器，接口不变。
package creds

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/corecraft-io/insula/ident"
)

// DefaultTTL 是句柄默认有效期。
const DefaultTTL = 15 * time.Minute

// ErrNotFound 该租户在指定 scope 下没有配置凭证。
var ErrNotFound = errors.New("insula/creds: no credential for scope")

// ErrExpired 句柄已过期。
var ErrExpired = errors.New("insula/creds: handle expired")

// ErrForeignHandle 句柄不属于该租户。
var ErrForeignHandle = errors.New("insula/creds: handle belongs to another tenant")

// ErrNoHandle 调用方没有交出句柄（nil 或空）。
//
// 与 ErrExpired 分开：过期是「曾经有效的凭证失效了」，讲的是时间；
// 无句柄是「你什么都没给我」，讲的是调用契约。在鉴权路径上这两者
// 对应的运维动作完全不同（轮转凭证 vs 修调用方代码），
// 合成一个错误会让排查方向从一开始就是错的。
var ErrNoHandle = errors.New("insula/creds: no handle presented")

// Handle 是凭证的短期引用。它**不包含明文**，因此可以安全地
// 放进结构体、日志字段与错误消息里。
type Handle struct {
	id        string
	tenant    ident.Tenant
	scope     string
	expiresAt time.Time
}

// Tenant 返回句柄所属租户。
func (h *Handle) Tenant() ident.Tenant { return h.tenant }

// Scope 返回句柄的作用域（如 "model-api"、"vector-db"）。
func (h *Handle) Scope() string { return h.scope }

// ExpiresAt 返回到期时刻。
func (h *Handle) ExpiresAt() time.Time { return h.expiresAt }

// Expired 报告句柄在 now 时刻是否已过期。
func (h *Handle) Expired(now time.Time) bool { return !now.Before(h.expiresAt) }

// String 实现 fmt.Stringer，输出不含明文。
func (h *Handle) String() string {
	if h == nil {
		return "creds.Handle(nil)"
	}
	return fmt.Sprintf("creds.Handle{%s/%s exp=%s}", h.tenant, h.scope,
		h.expiresAt.UTC().Format(time.RFC3339))
}

// GoString 实现 fmt.GoStringer，防止 %#v 打印出内部字段。
func (h *Handle) GoString() string { return h.String() }

// Provider 签发与兑换凭证句柄。
type Provider interface {
	// Issue 为租户签发 scope 下的短期句柄。
	Issue(t ident.Tenant, scope string) (*Handle, error)
	// Resolve 把句柄兑换为明文。句柄过期、已被撤销、不属于任何租户，
	// 或其自述身份与凭据库记录不符时返回 false。
	Resolve(h *Handle) (string, bool)
	// Revoke 撤销某租户的全部在途句柄（租户注销或凭证轮转时调用）。
	Revoke(t ident.Tenant)
}

type liveEntry struct {
	key       string
	tenant    ident.Tenant
	scope     string
	expiresAt time.Time
}

// StaticProvider 是内存实现：凭证在启动时注入，句柄在内存里签发与回收。
type StaticProvider struct {
	ttl  time.Duration
	rand io.Reader

	mu      sync.Mutex
	secrets map[string]string     // "tenant|scope" → 明文
	live    map[string]*liveEntry // 句柄 ID → 条目
	clock   func() time.Time
}

// Option 调整 StaticProvider 行为。
type Option func(*StaticProvider)

// WithTTL 覆盖句柄有效期。
func WithTTL(d time.Duration) Option {
	return func(p *StaticProvider) {
		if d > 0 {
			p.ttl = d
		}
	}
}

// WithClock 注入时钟，便于测试过期路径。
func WithClock(clock func() time.Time) Option {
	return func(p *StaticProvider) {
		if clock != nil {
			p.clock = clock
		}
	}
}

// NewStaticProvider 构造内存凭证提供者。
func NewStaticProvider(opts ...Option) *StaticProvider {
	p := &StaticProvider{
		ttl:     DefaultTTL,
		rand:    rand.Reader,
		secrets: make(map[string]string),
		live:    make(map[string]*liveEntry),
		clock:   time.Now,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Put 配置某租户在某 scope 下的凭证明文。
//
// 调用方应只从环境变量或密钥管理系统读取，绝不要写进代码或配置文件。
func (p *StaticProvider) Put(t ident.Tenant, scope, secret string) error {
	if !t.Valid() {
		return fmt.Errorf("insula/creds: empty tenant identity")
	}
	if scope == "" {
		return fmt.Errorf("insula/creds: empty scope")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.secrets[key(t, scope)] = secret
	return nil
}

// Issue 实现 Provider。
func (p *StaticProvider) Issue(t ident.Tenant, scope string) (*Handle, error) {
	if !t.Valid() {
		return nil, fmt.Errorf("insula/creds: empty tenant identity")
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.secrets[key(t, scope)]; !ok {
		return nil, fmt.Errorf("%w: %s/%s", ErrNotFound, t, scope)
	}

	var raw [16]byte
	if _, err := p.rand.Read(raw[:]); err != nil {
		return nil, fmt.Errorf("insula/creds: generate handle id: %w", err)
	}
	id := hex.EncodeToString(raw[:])
	h := &Handle{
		id:        id,
		tenant:    t,
		scope:     scope,
		expiresAt: p.clock().Add(p.ttl),
	}
	p.live[id] = &liveEntry{key: key(t, scope), tenant: t, scope: scope, expiresAt: h.expiresAt}
	return h, nil
}

// lookup 是**唯一**的兑换实现体。它返回明文，以及凭据库里记录的
// 所有者与作用域。
//
// 授权判断必须建立在这两个返回值上，而不是句柄自己的 tenant/scope
// 字段。理由：句柄是能力（capability），能力的身份由**签发方记账**，
// 不由携带方声明。当前 Handle 的字段未导出，外部代码伪造不出句柄；
// 但这个不变量是**结构性的**（靠"没提供构造入口"），一旦将来加上
// 「由 ID 重建句柄」之类的路径（反序列化、进程间传递、缓存恢复、
// 给测试用的构造函数），自述身份立刻变成攻击面。把检查放在 store
// 一侧，以后加任何入口都绕不过去。
func (p *StaticProvider) lookup(id string) (secret string, owner ident.Tenant, scope string, ok bool) {
	if id == "" {
		return "", "", "", false
	}
	now := p.clock()

	p.mu.Lock()
	defer p.mu.Unlock()

	e, live := p.live[id]
	if !live {
		return "", "", "", false
	}
	if !now.Before(e.expiresAt) {
		delete(p.live, id) // 过期即回收，避免 map 无界增长
		return "", "", "", false
	}
	secret, found := p.secrets[e.key]
	if !found {
		// 配置被删（Put 覆盖不了这种情况，但适配器可能返回）：
		// 记录还在、明文没了，只能当成兑换失败。
		return "", "", "", false
	}
	return secret, e.tenant, e.scope, true
}

// Resolve 实现 Provider。
//
// 除了「句柄在 store 里且在有效期内」，还要求句柄的自述租户/作用域
// 与记录一致——不一致说明这个句柄被改造过，一律拒绝。
func (p *StaticProvider) Resolve(h *Handle) (string, bool) {
	if h == nil {
		return "", false
	}
	secret, owner, scope, ok := p.lookup(h.id)
	if !ok || owner != h.tenant || scope != h.scope {
		return "", false
	}
	return secret, true
}

// ResolveFor 是 Resolve 的严格版本：额外校验句柄确实属于 t。
//
// 这是防「句柄转交」的检查——若某租户的子组件拿到了别的租户的句柄
// （例如经由共享的全局单例泄漏），兑换会被拒绝。
//
// 注意判据来自 lookup 返回的 owner，即**凭据库里的记录**，而不是
// h.tenant。先看记录再看自述，顺序不能反：只看自述等于让携带者
// 给自己发通行证。
func (p *StaticProvider) ResolveFor(t ident.Tenant, h *Handle) (string, error) {
	if h == nil || h.id == "" {
		return "", ErrNoHandle
	}
	secret, owner, scope, ok := p.lookup(h.id)
	if !ok {
		return "", ErrExpired
	}
	if owner != t {
		return "", fmt.Errorf("%w: handle for %s presented by %s", ErrForeignHandle, owner, t)
	}
	if owner != h.tenant || scope != h.scope {
		// 句柄被改造过：自述身份与凭据库记录不符。用 Redact 而不是
		// 原样打印句柄 ID —— 句柄 ID 本身就是 bearer 能力（能猜出它
		// 就能兑换明文），错误消息会被写进日志。
		return "", fmt.Errorf("%w: presented handle is not bound to %s/%s (%s)",
			ErrForeignHandle, h.tenant, h.scope, Redact(h.id))
	}
	return secret, nil
}

// Revoke 实现 Provider。
func (p *StaticProvider) Revoke(t ident.Tenant) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, e := range p.live {
		if e.tenant == t {
			delete(p.live, id)
		}
	}
}

// Live 返回某租户当前在途的句柄数（指标用，不暴露句柄本身）。
//
// 「在途」按**签发出去还没回收**计：已过期但从未被兑换或撤销的句柄
// 也计入。这是有意的——这个数字的用途是发现泄漏（签发了很多、
// 回收得很少），偏高比偏低安全。真正的可用性判断只有 Resolve 能给。
func (p *StaticProvider) Live(t ident.Tenant) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, e := range p.live {
		if e.tenant == t {
			n++
		}
	}
	return n
}

// String 实现 fmt.Stringer，绝不输出明文。
func (p *StaticProvider) String() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return fmt.Sprintf("creds.StaticProvider{ttl=%s, scopes=%d, live=%d}",
		p.ttl, len(p.secrets), len(p.live))
}

// GoString 实现 fmt.GoStringer，防止 %#v 泄漏。
func (p *StaticProvider) GoString() string { return p.String() }

// Redact 把可能含机密的字符串降级为可安全日志的形式。
//
// 保留前 3 个字符以便运维区分不同凭证，其余一律星号化。
// 长度不足 6 时全部星号化——短串的前缀本身就是信息。
func Redact(s string) string {
	if len(s) < 6 {
		return strings.Repeat("*", len(s))
	}
	return s[:3] + "***" + strings.Repeat("*", min(len(s)-6, 12))
}

func key(t ident.Tenant, scope string) string { return string(t) + "|" + scope }
