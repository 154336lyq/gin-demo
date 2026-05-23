# chain-backend 小型区块链后端实战

本程序把 **Gin HTTP / JWT / Viper / gRPC(Protobuf) / go-ethereum / 并发流水线** 放在同一进程内，便于本地联调与面试演示。

---

## 一、你需要事先手动安装的环境

以下内容无法在仓库内替你自动安装，请按需在本机完成。

### 1. Go

安装与你的项目 `go.mod` 匹配的 Go（当前仓库为 **Go 1.25**）。命令行可执行 `go version`。

### 2. 本地以太坊节点（强烈推荐 Foundry Anvil）

用于 **HTTP RPC（8545）** 与 **WebSocket（同源端口 ws）**，默认 **Chain ID = 31337**，与本仓库 `configs/config.yaml` 一致。

- **安装 Foundry**：请打开官方文档 [Foundry Book](https://book.getfoundry.sh/getting-started/installation) ，按平台安装后确保终端可用：
  - `anvil --version`
- **不要用 npm** 安装 Foundry；若你已安装 Rust/Cargo，也可用对应渠道安装 `foundryup`。

> **仅 Windows 注意**：请用 **支持 long path** 的目录克隆本仓库，并确保终端能执行 `anvil`（可加入 PATH）。

### 3. 重新生成 Protobuf（一般不需要）

本仓库已提交 `pb/chainpb/*.pb.go`。若你修改了 `pb/chain.proto`，需要本机安装：

- `protoc`（Protocol Buffers 编译器）
- `protoc-gen-go`、`protoc-gen-go-grpc`（`go install google.golang.org/protobuf/cmd/protoc-gen-go@latest` 等）

然后在仓库根目录执行（示例）：

```bash
protoc --proto_path=pb --go_out=pb/chainpb --go_opt=paths=source_relative \
  --go-grpc_out=pb/chainpb --go-grpc_opt=paths=source_relative pb/chain.proto
```

### 4. 可选工具

- **grpcurl**：调试 gRPC（安装方法见 [grpcurl](https://github.com/fullstorydev/grpcurl)）。
- **curl**：下文 REST 示例默认你有 curl（Windows PowerShell 也可用 `curl.exe`）。

---

## 二、本地测试网（Anvil）标准演示步骤

### 步骤 A：启动本地链

终端 1：

```bash
anvil
```

保持窗口不关。默认监听：

- HTTP：`http://127.0.0.1:8545`
- WS：`ws://127.0.0.1:8545`（Anvil 同源端口即可订阅）

### 步骤 B：复制 / 检查配置

仓库根目录应存在 `configs/config.yaml`（若缺失，请复制 `configs/config.example.yaml` 并重命名）。

关键字段说明：

- `eth.chain_id`：必须与节点一致，**Anvil 默认 31337**。
- `eth.http_rpc` / `eth.ws_rpc`：指向本机 Anvil。
- `eth.erc20_contract`：**留空**则只订阅「新区块头」；填入合约地址则额外订阅该 ERC-20 的 `Transfer` 日志。

### 步骤 C：启动本服务

在仓库根目录开终端 2：

```bash
go run ./cmd/chain-backend/
```

预期日志包含：`HTTP(Gin) 监听 ...`、`gRPC 监听 ...`，以及 `[eth/listener] 已连接 SubscribeNewHead`（若 WS 配置正确）。

### 步骤 D：制造区块（验证订阅）

终端 3（任意发出一笔链上交易即可；示例用 Anvil 默认账户发空转账）：

```bash
cast send 0x70997970C51812dc3A010C7d01b50e0d17dc79C8 --value 0.01ether --private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80 --rpc-url http://127.0.0.1:8545
```

回到终端 2，应看到 `[pipeline] worker-* 处理区块头` 一类日志。

---

## 三、REST 调用示例（JWT）

### 1. 登录拿 Token

```bash
curl -s -X POST http://127.0.0.1:8080/api/v1/auth/login ^
  -H "Content-Type: application/json" ^
  -d "{\"username\":\"admin\",\"password\":\"admin123\"}"
```

PowerShell 可把 `^` 换成 `` ` `` 或写成一行。

响应里的 `token` 用于 `Authorization: Bearer ...`。

### 2. 查询最新区块（迷你浏览器）

```bash
curl -s http://127.0.0.1:8080/api/v1/blocks/latest -H "Authorization: Bearer <TOKEN>"
```

### 3. 流水线统计（并发演示）

```bash
curl -s http://127.0.0.1:8080/api/v1/pipeline/stats -H "Authorization: Bearer <TOKEN>"
```

### 4. 文件上传（SHA-256 + sync.Map 索引）

```bash
curl -s -X POST http://127.0.0.1:8080/api/v1/files/upload -H "Authorization: Bearer <TOKEN>" -F "file=@README.md"
```

响应 JSON 中含 **`download_url`**（仅含 SHA 的路径，无中文）、**`download_path`**；可将 **`download_url`** 直接存库，下载时 **GET 同一地址并带 Bearer** 即可（勿把中文原始文件名拼进 URL path）。`configs/config.yaml` 中 **`server.public_base_url`** 应与浏览器访问的根地址一致。

```bash
curl -s -L -o downloaded.bin -H "Authorization: Bearer <TOKEN>" "<粘贴 upload 返回的 download_url>"
```

### 5. HTTP 探测（网络编程 / 爬虫入门扩展位）

```bash
curl -s "http://127.0.0.1:8080/api/v1/tools/http-probe?url=https://example.com" -H "Authorization: Bearer <TOKEN>"
```

---

## 四、gRPC 示例

```bash
grpcurl -plaintext -d "{}" 127.0.0.1:9090 chainpb.ChainQuery/GetLatestBlock
```

---

## 五、可选：启用 ERC-20 转账监听

1. 在 Anvil 上部署任意标准 ERC-20，把合约地址写入 `eth.erc20_contract`。
2. 重启 `chain-backend`。
3. 调用代币 `transfer` 后，终端应出现 `[pipeline] Transfer 日志 ...`。

若暂无合约，保持 `erc20_contract` 为空即可，区块订阅仍可用。

---

## 五点五、ABI 与 Solidity 合约「读交互」（本项目新增）

**ABI**（Application Binary Interface）约定：函数如何编码进交易的 `data` 字段、返回值如何解码。Solidity 编译器会产出 ABI JSON；Go 侧用 `accounts/abi` 做 **Pack（编码调用）** 与 **Unpack（解码结果）**，再通过 **`eth_call`** 向节点发起**只读模拟调用**（不写链，适合 `view`/`pure`）。

此前本仓库只有区块/交易/余额等 JSON-RPC，**没有**走 ABI；现已增加示例：

| REST（均需 JWT） | 说明 |
|------------------|------|
| `GET /api/v1/contracts/erc20/balance?token=0x…&holder=0x…` | `balanceOf` + 可选 `decimals/symbol` 展示余额 |
| `GET /api/v1/contracts/erc20/info?token=0x…` | `decimals`、`symbol` |
| `GET /api/v1/contracts/counter/number?contract=0x…` | 读取示例合约 `Counter.number()` |

仓库根目录 `contracts/Counter.sol` 为极简演示合约。本地可用 Foundry 部署（Anvil 已启动、`--private-key` 使用 Anvil 默认账户私钥之一）：

```bash
forge create contracts/Counter.sol:Counter --rpc-url http://127.0.0.1:8545 --private-key <你的测试私钥>
```

将输出里的合约地址填到 `counter/number` 的 `contract` 参数即可验证读接口。

**说明**：「写合约」（`increment`、`transfer`）需要签名交易，通常由钱包或托管后端发送；本演示 REST 仅覆盖 **只读 eth_call**，与商业项目里「后端校验链上状态」的读路径一致。

---

## 六、代码结构说明（便于你对照课程）

| 路径 | 作用 |
|------|------|
| `internal/config` | Viper 加载 YAML |
| `internal/api` | Gin 路由、JWT、文件上传（`crypto/sha256`）、REST |
| `internal/eth` | go-ethereum：查询 + WS 订阅 + ABI 只读调用（`abi_contract.go`） |
| `internal/pipeline` | Channel + WaitGroup + Mutex/原子操作（事件消费） |
| `internal/grpcsvc` | Protobuf / gRPC `ChainQuery` |
| `internal/store` | 内存用户表（`RWMutex`） |
| `pb/chain.proto` | 接口契约 |

---

## 七、常见问题

**Q：`连接以太坊节点失败`？**  
先确认 Anvil 已启动，且 `configs/config.yaml` 中 RPC 地址正确。

**Q：`Chain ID 校验失败`？**  
节点实际 chain id 与配置不一致。Anvil 一般为 `31337`；若连 Sepolia 等公链测试网，请改成对应 id。

**Q：没有 `[eth/listener] 已连接 SubscribeNewHead`？**  
检查 `ws_rpc` 是否可访问；若仅保留 HTTP，程序会跳过订阅（仍可 REST 查询）。

**Q：Ctrl+C 后进程不退？**  
等待 HTTP/gRPC 优雅关闭与流水线排空；若长时间卡住，再按一次中断（生产环境应配合超时排查）。

---

更多以太坊基础概念（账户模型、Gas、主网/测试网）建议配合官方文档与 `go-ethereum` 文档阅读；本 README 侧重 **本地可跑通**。
