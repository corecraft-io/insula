// Package realm 是 Insula 租户隔离域的唯一权威来源：平台服务名清单、
// 入口 ID 命名空间、以及把这两件事钉死的断言。
//
// # 为什么需要这个包
//
// cordis 的隔离域是「按服务名声明」的：isolateKey{name, realm} 中的 realm
// 来自上下文链上最近一次 Isolate 声明（见 cordis/context.go 的 isolateKey）。
// 于是有一个必须正视的失效模式——**漏声明一个服务名，该服务就落到默认域
// （realm == ""），退化成全平台共享**，而且不报错：
//
//   - 两个租户都注册该服务 → 第二个撞上 ErrServiceDuplicate（还算好，当场报错）；
//   - 只有一个租户注册 → 其他租户的 Get 会**静默解析到那个租户的实例**。
//
// 因此平台侧不允许在业务代码里手写 Isolate 映射：唯一的来源是 Tenant()，
// 它由 Services() 生成，新增服务必须先进 Services()。
//
// # 入口 ID 为什么要带租户哈希
//
// cordis 的 EntryTree.store 以短 ID 为扁平索引键，短 ID 必须全树唯一；
// 而 EntryGroup.reconcile 遇到重复 ID 只记一条日志然后跳过（不报错）。
// 若用 "s1"、"s2" 这类租户内自增序号作短 ID，第二个租户的 "s1" 会被静默
// 跳过，随后任何按短 ID 或路径的寻址都会命中**第一个租户的入口**——
// 一条静默的跨租户误路由路径。
//
// 所以本包强制所有入口 ID 为 "<scope>-<hash(tenant)>[-<seq>]"：
// 冗余，但全局唯一由构造保证，不依赖调用方的纪律。
package realm

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"reflect"
	"strings"

	"github.com/metaRobin/cordis"
)

// ---------------------------------------------------------------------------
// 平台服务名
// ---------------------------------------------------------------------------

// 租户级服务：每个租户各自一份实例，跨租户绝不可见。
const (
	ServiceModels = "models" // 模型网关（含租户额度分账）
	ServiceMemory = "memory" // 会话历史与长期记忆存储
	ServiceTools  = "tools"  // 工具注册表（含租户白名单）
	ServiceGuard  = "guard"  // 循环/工具守卫策略
	ServiceCaps   = "caps"   // 能力快照：由探测插件提供，供会话依赖
)

// 会话级服务：每个会话各自一份，域键比租户域更细一层。
const (
	ServiceHistory = "history" // 会话历史窗口
	ServiceScratch = "scratch" // 会话级临时资源
)

// 插件名：loader 的插件解析器以这些名字定位插件定义。
const (
	PluginTenantRoot   = "tenant-root"
	PluginSessions     = "sessions"
	PluginModelGateway = "model-gateway"
	PluginMemoryStore  = "memory-store"
	PluginToolRegistry = "tool-registry"
	PluginLoopGuard    = "loop-guard"
	PluginCapsProbe    = "caps-probe"
	PluginSession      = "session"
)

// tenantServices 是租户级服务的完整清单，顺序即声明顺序（便于断言与日志）。
var tenantServices = []string{
	ServiceModels, ServiceMemory, ServiceTools, ServiceGuard, ServiceCaps,
}

// sessionServices 是会话级服务的完整清单。
var sessionServices = []string{ServiceHistory, ServiceScratch}

// Services 返回租户级服务名清单的副本。
func Services() []string { return append([]string(nil), tenantServices...) }

// SessionServices 返回会话级服务名清单的副本。
func SessionServices() []string { return append([]string(nil), sessionServices...) }

// ServiceCount 返回租户级服务数量（隔离自检的循环上界）。
func ServiceCount() int { return len(tenantServices) }

// Tenant 返回租户根入口的 Isolate 声明：全部租户级服务 → 私有域（true）。
//
// 这是平台中唯一允许出现 Isolate 字面量的地方。租户根入口声明一次，
// 整棵子树经上下文父链继承（子入口 ctx 由分组 fiber 的 ctx 派生）。
func Tenant() map[string]any {
	m := make(map[string]any, len(tenantServices))
	for _, name := range tenantServices {
		// true 表示私有域：cordis 会把域键解析为 "#" + 入口ID，
		// 即 "#t-<hash>"。绝不使用字符串标签——那是共享域，
		// 是跨租户共享服务的后门。
		m[name] = true
	}
	return m
}

// Session 返回会话入口的 Isolate 声明：会话级服务 → 私有域。
//
// 会话入口在租户子树内部，因此租户级服务仍继承租户域，
// 只有 history/scratch 被收窄到 "#<会话入口ID>"。
func Session() map[string]any {
	m := make(map[string]any, len(sessionServices))
	for _, name := range sessionServices {
		m[name] = true
	}
	return m
}

// SharedRealmLabel 判断一个 Isolate 声明值是否为共享域标签。
// 平台层禁止共享域（见 SAFETY.md 的四条硬规则），本函数供自检使用。
func SharedRealmLabel(v any) bool {
	_, isString := v.(string)
	return isString
}

// ---------------------------------------------------------------------------
// 入口 ID 命名空间
// ---------------------------------------------------------------------------

// 入口 ID 的对象层（第一个路径段）。
const (
	PrefixTenant  = "t"
	PrefixSession = "s"
)

// ID 的十六进制长度。48 bit 在 10 万租户下碰撞概率约 2e-5；
// 即便碰撞也只会让 EntryTree.Create 返回错误（快速失败），不会静默串租。
const hashHexLen = 12

// Hasher 生成租户命名空间哈希。
//
// 默认（secret 为空）用 FNV-1a：确定性、零依赖，但可预测。
// 传入 secret 时改用 HMAC-SHA256 截断：入口 ID 不可被外部构造，
// 为将来任何「按 ID 寻址」的接口预先上好 IDOR 防线。
//
// Hasher 必须全进程唯一：同一租户在不同分片、不同进程上必须算出同一个哈希，
// 否则粘性路由与持久层的租户映射会对不上。
type Hasher struct {
	secret []byte
}

// NewHasher 构造哈希器。secret 可为 nil。
func NewHasher(secret []byte) *Hasher {
	if len(secret) == 0 {
		return &Hasher{}
	}
	return &Hasher{secret: append([]byte(nil), secret...)}
}

// Sum 返回 id 的稳定短哈希（小写十六进制，hashHexLen 位）。
func (h *Hasher) Sum(id string) string {
	if len(h.secret) > 0 {
		mac := hmac.New(sha256.New, h.secret)
		mac.Write([]byte(id))
		return hex.EncodeToString(mac.Sum(nil))[:hashHexLen]
	}
	f := fnv.New64a()
	f.Write([]byte(id))
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], f.Sum64())
	return hex.EncodeToString(buf[:])[:hashHexLen]
}

// TenantEntry 返回租户根入口的短 ID：t-<hash>。
func (h *Hasher) TenantEntry(tenantID string) string {
	return PrefixTenant + "-" + h.Sum(tenantID)
}

// Child 在给定父入口短 ID 下派生子入口短 ID：<parent>-<name>。
//
// 冗余地带上了父 ID（因此带上了租户哈希），正是为了满足全树唯一。
// 不要改成「只返回 name」——那会重新打开静默串租的缺口。
func Child(parent, name string) string {
	return parent + "-" + name
}

// SessionEntry 返回会话入口的短 ID：<tenantEntry>-s<seq>。
//
// seq 由调用方（租户句柄）单调分配；用十六进制而非十进制以固定长度、
// 避免与 Child 生成的名字产生歧义。
func SessionEntry(tenantEntry string, seq uint64) string {
	return tenantEntry + "-" + PrefixSession + fmt.Sprintf("%012x", seq)
}

// SessionsGroupEntry 返回会话分组入口的短 ID。
func SessionsGroupEntry(tenantEntry string) string {
	return Child(tenantEntry, PluginSessions)
}

// CheckTenantEntry 校验短 ID 是否为合法租户根入口 ID。
func CheckTenantEntry(id string) error {
	prefix := PrefixTenant + "-"
	if !strings.HasPrefix(id, prefix) {
		return fmt.Errorf("%w: tenant entry id %q must start with %q", ErrBadEntryID, id, prefix)
	}
	if len(id) != len(prefix)+hashHexLen {
		return fmt.Errorf("%w: tenant entry id %q must be %q + %d hex chars",
			ErrBadEntryID, id, prefix, hashHexLen)
	}
	return nil
}

// CheckChildEntry 校验子入口短 ID 是否属于给定租户命名空间。
//
// 这是租户开通/会话创建路径上的强制断言：任何一个不符合前缀的 ID
// 都意味着调用方绕过了本包，必须立即失败而不是交给 cordis 静默处理。
func CheckChildEntry(tenantEntry, id string) error {
	if !strings.HasPrefix(id, tenantEntry+"-") {
		return fmt.Errorf("%w: child entry id %q is not in namespace of %q",
			ErrBadEntryID, id, tenantEntry)
	}
	if strings.Contains(id, ":") {
		return fmt.Errorf("%w: entry id %q must not contain ':' (path separator)",
			ErrBadEntryID, id)
	}
	return nil
}

// Path 把一串**短 ID** 拼成全路径。
//
// 这个函数存在的唯一原因，是 cordis 的入口树有两套并存寻址，混用会静默失败：
//
//   - **store 索引**以短 ID 为扁平键（查重、Insert 都走它）；
//   - **Resolve / Create / Update / Remove 认的是路径**，以 ":" 分隔
//     （见 loader.go 的 EntryTree 注释）。
//
// 于是"会话分组在租户根之下"这件事用短 ID 表达不出来：把
// "<租户根>-sessions" 当 parent 传进 Create，会被当成**根组下的一个
// 同名入口**去解析，报 "cannot resolve entry"，而报错信息里那个 ID
// 看起来完全正确——这是最容易看错的一类失败。
//
// 反过来，路径也不能当短 ID 用：CheckChildEntry 明确禁止短 ID 含 ":"。
// 两者必须由本包一处分界，不要让调用方各自拼字符串。
func Path(segments ...string) string {
	return strings.Join(segments, ":")
}

// SessionsGroupPath 返回某租户的会话分组入口路径。
func SessionsGroupPath(tenantEntry string) string {
	return Path(tenantEntry, SessionsGroupEntry(tenantEntry))
}

// SessionPath 返回某租户下第 seq 个会话入口的路径。
func SessionPath(tenantEntry string, seq uint64) string {
	return Path(tenantEntry, SessionsGroupEntry(tenantEntry), SessionEntry(tenantEntry, seq))
}

// ErrBadEntryID 入口 ID 不满足命名空间约定。
var ErrBadEntryID = errors.New("insula/realm: bad entry id")

// ---------------------------------------------------------------------------
// 隔离断言
// ---------------------------------------------------------------------------

// ErrIsolationBreach 两个应当隔离的上下文解析到了同一个服务实例。
//
// 这是产品级安全声明被违反的信号，绝不能降级为日志。
var ErrIsolationBreach = errors.New("insula/realm: tenant isolation breach")

// SameInstance 判断两个服务值是否为同一个实例。
//
// 只对句柄类（指针/映射/通道/函数/切片）做身份比较——平台约定
// 「注册的服务值必须是稳定的句柄对象」，因此这也是实际使用形态。
// 值类型退化为 DeepEqual，调用方应避免依赖它的结论。
func SameInstance(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	if va.Type() != vb.Type() {
		return false
	}
	switch va.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Chan, reflect.Func,
		reflect.UnsafePointer, reflect.Slice:
		return va.Pointer() == vb.Pointer()
	case reflect.Interface:
		return SameInstance(va.Elem().Interface(), vb.Elem().Interface())
	default:
		return reflect.DeepEqual(a, b)
	}
}

// AssertIsolated 黑盒断言：a 与 b 两个上下文对 name 的解析结果必须不是同一个实例。
//
// 这是「隔离确实生效」的唯一可编程验证手段——isolateKey 未导出，
// 白盒断言做不到，因此平台侧一律用这个黑盒形式：
// 在回归测试里逐服务断言，在运行时自检里作为哨兵比对。
func AssertIsolated(a, b *cordis.Context, name string) error {
	if a == nil || b == nil {
		return fmt.Errorf("%w: nil context", ErrIsolationBreach)
	}
	va, oka := a.Get(name)
	vb, okb := b.Get(name)
	switch {
	case !oka && !okb:
		// 两侧都不可见：隔离成立（且恰好是「双方都没注册」的退化情形）。
		return nil
	case oka != okb:
		// 只有一侧可见：隔离成立，但通常说明装配不对称，调用方可能想关注。
		return nil
	}
	if SameInstance(va, vb) {
		return fmt.Errorf("%w: service %q resolves to the same instance in two scopes; "+
			"most likely its name is missing from realm.Services()", ErrIsolationBreach, name)
	}
	return nil
}
