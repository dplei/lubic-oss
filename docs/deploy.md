# 部署详细步骤

README 里的三种场景都由下面几块拼起来：

| 场景 | 总控 | 录制（Agent） | 控制台怎么打开 |
|---|---|---|---|
| Windows 家用 | [Docker](#1-docker-部署) | 同一份 compose | `http://127.0.0.1:7070` |
| Mac 家用 | [原生 + launchd](#2-mac-原生部署) | 原生 + launchd | `http://127.0.0.1:7070` |
| 云服务器 | [Docker](#1-docker-部署) | 同一份 compose | [域名 + Caddy](#3-从外面访问控制台) 或 SSH 隧道 |
| 混合多机 | 云服务器上 Docker | 各台机器选 Docker 或 Mac 原生，只跑 Agent | [域名 + Caddy](#3-从外面访问控制台)（必需） |

先读一下：

- 请只录制、投稿你有权使用的直播内容。投稿用的是 B 站网页和客户端接口，账号有风控风险，建议先用小号试。
- 每录一个房间约占 **10 Mbps 下行 + 10 Mbps 上行**（边录边传），每小时约 4～5 GB。视频传完、总控确认后才删本地，磁盘要放得下没传完的部分。
- 凭证、数据库、录像都放在源码目录之外。下文的运行目录 Linux 用 `/srv/lubic`，Windows 用 `C:\lubic`，Mac 用 `~/Library/Application Support/lubic`。

## 1. Docker 部署

适用于 Windows（装 [Docker Desktop](https://www.docker.com/products/docker-desktop/)，设置里勾上「开机启动」）和 Linux 服务器（`curl -fsSL https://get.docker.com | sh`）。

镜像由公开仓库的 CI 构建，`ghcr.io/dplei/lubic-oss:latest`，只有 x86_64（amd64）版本。ARM 机器或拉不动 ghcr.io 时，在源码目录自建：`docker build -t ghcr.io/dplei/lubic-oss:latest --build-arg GIT_COMMIT="$(git rev-parse HEAD)" .`（会先跑全量测试，十几分钟，建议 4 GB 内存以上）。

### 1.1 目录与 compose

```text
lubic/
├── docker-compose.yml
├── controller/   # 容器里的 /data：controller.toml、controller.db、日志
├── agent/        # 容器里的 /data：agent.toml、状态文件、日志
└── rec/          # 录像
```

```yaml
# docker-compose.yml
name: lubic

x-common: &common
  image: ghcr.io/dplei/lubic-oss:latest
  restart: unless-stopped
  init: true
  stop_signal: SIGINT          # 与 Ctrl-C 一样：收尾当前分段后退出
  logging:
    driver: json-file
    options: { max-size: 20m, max-file: "5" }

services:
  controller:
    <<: *common
    command: ["lubic-controller", "-c", "/data/controller.toml", "--log-dir", "/data/logs"]
    volumes: [./controller:/data]
    ports: ["127.0.0.1:7070:7070"]   # 只对本机开放
    stop_grace_period: 30s

  agent:
    <<: *common
    command: ["lubic-agent", "-c", "/data/agent.toml", "--log-dir", "/data/logs"]
    volumes: [./agent:/data, ./rec:/rec]
    stop_grace_period: 60s
```

混合多机里只跑录制的机器，删掉 `controller` 服务即可。

### 1.2 总控

`controller/controller.toml`：

```toml
listen = "0.0.0.0:7070"         # 容器内监听；对外开放范围由 compose 的 ports 决定
db = "/data/controller.db"
admin_password_hash = "<下一步生成>"
```

生成控制台密码哈希（输入时不回显），把输出的 `$argon2id$...` 整行填回 `admin_password_hash`：

```bash
docker compose run --rm -it controller sh -c 'stty -echo; read PW; stty echo; echo; printf "%s\n" "$PW" | lubic-controller -c /data/controller.toml hash-password'
```

启动：

```bash
docker compose up -d controller
```

### 1.3 Agent

浏览器打开控制台，用上面的密码登录，在「机器」里新增一台，复制给出的 token。写 `agent/agent.toml`：

```toml
controller = "http://controller:7070"   # 同一份 compose 里用服务名；混合多机填总控的 https 域名
token = "<控制台给的 token>"
state = "/data/agent-state.json"
output = "/rec/{name}"
# line = "tx"                           # UPOS 线路，默认 tx
# disk_threshold = 0.85                 # 磁盘到这个使用率就停录
# record_cookies = "/data/record-cookies.json"   # 可选，录 B 站高画质用，建议用小号
```

```bash
docker compose up -d agent
```

控制台「机器」里这台显示在线即可，然后看 [第 4 节](#4-在控制台里收尾)。

### 1.4 更新

```bash
docker compose pull && docker compose up -d
```

重启时正在录的房间会断一小段。多机部署时可以先在控制台给这台开「让出网络」，等房间交接走再更新。

## 2. Mac 原生部署

Mac 上不用 Docker（省掉虚拟机和它的编译缓存），从源码编译两个程序，交给 launchd 开机自启、崩溃重启。

### 2.1 编译

```bash
xcode-select --install                                   # 已装过会提示已安装
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
git clone https://github.com/dplei/lubic-oss.git ~/lubic-oss && cd ~/lubic-oss
cargo build --release -p lubic-controller -p lubic-agent
mkdir -p ~/.local/bin && cp target/release/lubic-controller target/release/lubic-agent ~/.local/bin/
```

更新时 `git pull` 后重跑后两行，再 `launchctl kickstart -k gui/$(id -u)/com.lubic.agent`（总控同理）。

### 2.2 配置

```bash
D="$HOME/Library/Application Support/lubic"
mkdir -p "$D/rec" ~/Library/Logs/lubic && chmod 700 "$D" && cd "$D"
```

`controller.toml`（只跑 Agent 的机器跳过）：

```toml
listen = "127.0.0.1:7070"
db = "/Users/you/Library/Application Support/lubic/controller.db"
admin_password_hash = "<下一步生成>"
```

```bash
read -rs PW && printf '%s\n' "$PW" | ~/.local/bin/lubic-controller -c controller.toml hash-password; unset PW
```

`agent.toml`，token 在总控启动后从控制台「机器」里新增拿到：

```toml
controller = "http://127.0.0.1:7070"   # 混合多机填总控的 https 域名
token = "<控制台给的 token>"
state = "/Users/you/Library/Application Support/lubic/agent-state.json"
output = "/Users/you/Library/Application Support/lubic/rec/{name}"
```

把 `/Users/you` 换成 `echo $HOME` 的结果。

### 2.3 launchd

下面是 Agent 的 plist，保存为 `~/Library/LaunchAgents/com.lubic.agent.plist`。总控再存一份 `com.lubic.controller.plist`：`Label` 改成 `com.lubic.controller`，程序换成 `lubic-controller`、配置换成 `controller.toml`，stderr 日志换个文件名。

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.lubic.agent</string>
  <key>ProgramArguments</key>
  <array>
    <string>/Users/you/.local/bin/lubic-agent</string>
    <string>-c</string>
    <string>/Users/you/Library/Application Support/lubic/agent.toml</string>
    <string>--log-dir</string>
    <string>/Users/you/Library/Logs/lubic</string>
  </array>
  <key>WorkingDirectory</key><string>/Users/you/Library/Application Support/lubic</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>10</integer>
  <key>StandardOutPath</key><string>/dev/null</string>
  <key>StandardErrorPath</key><string>/Users/you/Library/Logs/lubic-agent.stderr.log</string>
</dict>
</plist>
```

先启动总控，拿到 token 写进 `agent.toml` 后再启动 Agent：

```bash
plutil -lint ~/Library/LaunchAgents/com.lubic.*.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.lubic.controller.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.lubic.agent.plist
```

launchd 只在登录后运行，Mac 要设成自动登录、不自动睡眠（系统设置 → 电池 / 节能 → 防止自动进入睡眠）。

## 3. 从外面访问控制台

总控默认只对本机开放。

**没有域名**：用 SSH 隧道，在自己电脑上执行 `ssh -N -L 7070:127.0.0.1:7070 用户@服务器IP`，然后打开 `http://127.0.0.1:7070`。不能用于混合多机。

**有域名**：把域名解析到服务器，服务器安全组放行 80、443 端口，在 compose 里加一个 Caddy，它会自动申请和续期 HTTPS 证书：

```yaml
# 加到 docker-compose.yml 的 services 下
  caddy:
    image: caddy:2
    restart: unless-stopped
    ports: ["80:80", "443:443"]
    volumes: [./Caddyfile:/etc/caddy/Caddyfile:ro, ./caddy:/data]
```

```text
# Caddyfile
lubic.example.com {
	reverse_proxy controller:7070
}
```

```bash
docker compose up -d caddy
```

之后控制台是 `https://lubic.example.com`，其他机器的 `agent.toml` 里 `controller` 也填这个地址。国内服务器的域名需要先备案。

## 4. 在控制台里收尾

1. **账号**：扫码添加投稿用的 B 站账号。
2. **模板**：设好投稿标题、分区、标签。
3. **房间**：填直播间地址，选优先机器（多机时开播会分给其中在录最少的一台），选投稿模板。

之后开播自动录制、边录边传，下播后自动投稿。房间、模板、账号、机器都在控制台里增删改，立即生效。

## 5. 日常操作

下面的 `lubic-controller ...` 在不同部署里这样跑：

- Docker：`docker compose exec controller lubic-controller -c /data/controller.toml <子命令>`，在 compose 所在目录执行。
- Mac：`~/.local/bin/lubic-controller -c "$HOME/Library/Application Support/lubic/controller.toml" <子命令>`。

### 取回原片

分P传上 B 站后本地就删了。总控签发上传凭证时会把它记进库里、保留 5 天，凭它能把传上去的**原始文件**整个下载回来（不是 B 站转码后的版本），适合稿件被退回要重新剪、或者想留一份本地备份。

```bash
lubic-controller ... upos '主播A 2026-11-01 13:04'   # 按 part_id 或录制路径的一段查
```

每个匹配的分P输出两行：`# <part_id> <字节数> bytes, ticket <N>h old`，和一条 `curl` 命令。在想存文件的机器上运行那条 `curl`，下完用 `ls -l` 核对字节数，和输出里的一致才算下全。

- 只能取回 5 天内上传的分P，超过就查不到了。
- `tx`、`bda2`、`alia` 线路能取回；`bldsa` 线路只给 HEAD，GET 一律 403。`agent.toml` 的 `line` 默认是 `tx`。
- 命令里带凭证，别贴进 issue、群聊或会保留的日志。

### 手动场末

总控按下播宽限期判断一场结束。想让某个房间这一场立刻结束、分P传完就投稿：

```bash
lubic-controller ... end 主播A
```

总控运行中也能用。

### 投稿结果未知

投稿超时或总控在提交中途崩溃时，稿件在控制台显示「需人工」（库里的 `phase` 是 `attention`），lubic 不会自动重投，以免重复投稿。先到 B 站创作中心确认这份稿件到底投上没有：

- **没投上，也不要了**：在控制台「投稿」里点「丢弃」。
- **没投上，要重投**，或者**已经投上了**：要改库。先停总控（Docker：`docker compose stop controller`；停的时候 Agent 照常录，事件排队等总控回来），用主机上的 `sqlite3` 打开 `controller.db`，把这份稿件的 `phase` 改成 `'ended'`（重投）或 `'submitted'`（已投上），再启动总控：

  ```sql
  SELECT id, phase FROM archives WHERE phase = 'attention';
  UPDATE archives SET phase = 'ended' WHERE id = <id>;
  ```

### 暂停房间

在控制台取消这个房间的全部优先机器并保存。当前这一场录完后就不再录，已录的照常上传投稿。

### 看日志

- Docker：`docker compose logs --tail 200 controller`（或 `agent`），完整日志在 `controller/logs`、`agent/logs`。
- Mac：`~/Library/Logs/lubic/`；进程起不来时看 `~/Library/Logs/lubic-*.stderr.log`。

## 附：只录不传

只想把直播存到本地、不投稿时，不用总控和 Agent，单个命令行程序就够：

```bash
cargo build --release
./target/release/lubic "主播A=https://live.bilibili.com/123" https://live.douyin.com/456 \
  --split-time 1h -o '{name}/{name}_%Y-%m-%dT%H_%M_%S'
```

`Ctrl-C` 让所有房间收尾当前分段后退出，完整参数见 `lubic --help`。

## 附：试跑

总控加 `--dry-run` 不会真的投稿，但其余照常：照样上传视频和封面，稿件在数据库里标为已投稿（bvid 记为 `dry-run`）。试跑请用单独的数据库。
