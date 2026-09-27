// Command insula 是平台的进程入口。
//
// # 为什么入口这么薄
//
// insula 包的注释写了「门面纪律」：Handler 是给世界的，Provision /
// Deprovision / SelfCheck 是给运维者的。本文件是那条纪律在**进程边界**
// 上的落点——运维面只在这里被调用，而且只在启动与停机两个时刻。
//
// 启动之后就没有任何 HTTP 路由通向 Provision 了。这不是"我们没挂",
// 而是那条路由的类型不存在（见 insula.go 的门面纪律）：接入层拿到的
// Workspace 接口里根本没有 Provision。
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
)

const usage = `insula —— 多租户 agent 服务平台

用法：
  insula serve [选项]     启动 HTTP 服务（需要 -dev，见下）
  insula demo  [选项]     跑一遍完整流程，无需任何外部依赖
  insula help             显示本帮助

serve 的选项：
  -addr    127.0.0.1:8080   HTTP 监听地址
  -admin   127.0.0.1:8081   运维监听地址（/metrics、/healthz；空串关闭）
  -shards  4                分片数

demo 的选项：
  -shards  2                分片数

重要：本仓库不附带生产适配器。模型上游、凭证提供者、鉴权这三样都指向
外部世界，平台无法替使用方决定它们从哪来（见 insula.Config）。因此
serve 必须显式加 -dev 才会以开发桩件启动。
`

// errUsage 表示「命令行用错了」。它与运行期错误分开，因为两者的正确
// 反应不同：前者是看帮助，后者是看日志。
var errUsage = errors.New("usage")

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if !errors.Is(err, errUsage) {
			fmt.Fprintln(os.Stderr, "insula:", err)
		}
		os.Exit(1)
	}
}

// run 是 main 的全部逻辑，输出走注入的 writer 以便测试。
func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return errUsage
	}
	switch args[0] {
	case "serve":
		return runServe(args[1:], stdout, stderr)
	case "demo":
		return runDemo(args[1:], stdout, stderr)
	case "help", "-h", "-help", "--help":
		fmt.Fprint(stdout, usage)
		return nil
	default:
		fmt.Fprintf(stderr, "insula: 未知子命令 %q\n\n%s", args[0], usage)
		return errUsage
	}
}

// newFlagSet 造一个把错误与用法都写到 stderr 的 FlagSet。
//
// ContinueOnError + 把输出接到 stderr：默认的 ExitOnError 会在解析失败时
// 直接 os.Exit，那样 run 就没法被测试覆盖，"命令行用错"这条路径也就没有
// 任何测试守着。
func newFlagSet(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs
}
