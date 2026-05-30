package indexer

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"gin-demo/internal/config"
)

const defaultERC20ABIPath = "abis/erc20.json"

type watchedContract struct {
	address common.Address
	parsed  abi.ABI
	events  map[string]struct{} // 允许的事件名；空表示全部
}

// EventRegistry 按合约地址 + topic0 路由 ABI，通用解析链上 Logs。
type EventRegistry struct {
	byAddress map[common.Address]watchedContract
}

// ParsedEvent 解析后的链上事件。
type ParsedEvent struct {
	ContractAddress string
	EventName       string
	Topic0          string
	Topics          []string
	Args            map[string]interface{}
}

// NewEventRegistry 从配置加载 watch_contracts；eth.erc20_contract 非空时自动追加 ERC-20。
func NewEventRegistry(cfg *config.Config) (*EventRegistry, error) {
	r := &EventRegistry{byAddress: make(map[common.Address]watchedContract)}
	watches := append([]config.WatchContract{}, cfg.Indexer.WatchContracts...)

	if addr := strings.TrimSpace(cfg.Eth.ERC20Contract); addr != "" {
		watches = append(watches, config.WatchContract{
			Address: addr,
			ABI:     defaultERC20ABIPath,
			Events:  []string{"Transfer", "Approval"},
		})
	}

	for _, w := range watches {
		addr := strings.TrimSpace(w.Address)
		if addr == "" || !common.IsHexAddress(addr) {
			continue
		}
		abiPath := strings.TrimSpace(w.ABI)
		if abiPath == "" {
			abiPath = defaultERC20ABIPath
		}
		raw, err := os.ReadFile(abiPath)
		if err != nil {
			return nil, fmt.Errorf("read abi %s: %w", abiPath, err)
		}
		parsed, err := abi.JSON(strings.NewReader(string(raw)))
		if err != nil {
			return nil, fmt.Errorf("parse abi %s: %w", abiPath, err)
		}
		evSet := make(map[string]struct{})
		for _, name := range w.Events {
			evSet[name] = struct{}{}
		}
		a := common.HexToAddress(addr)
		r.byAddress[a] = watchedContract{address: a, parsed: parsed, events: evSet}
		log.Printf("[indexer/events] 已注册合约 %s abi=%s events=%v", a.Hex(), abiPath, w.Events)
	}
	return r, nil
}

// WatchCount 返回已注册合约数。
func (r *EventRegistry) WatchCount() int {
	if r == nil {
		return 0
	}
	return len(r.byAddress)
}

// ParseLog 尝试用已注册 ABI 解码 Log；非监听合约或未知事件返回 nil。
func (r *EventRegistry) ParseLog(lg types.Log) (*ParsedEvent, error) {
	if r == nil || len(lg.Topics) == 0 {
		return nil, nil
	}
	w, ok := r.byAddress[lg.Address]
	if !ok {
		return nil, nil
	}
	event, err := w.parsed.EventByID(lg.Topics[0])
	if err != nil {
		return nil, nil
	}
	if len(w.events) > 0 {
		if _, ok := w.events[event.Name]; !ok {
			return nil, nil
		}
	}

	args := make(map[string]interface{})
	if len(lg.Data) > 0 {
		if err := w.parsed.UnpackIntoMap(args, event.Name, lg.Data); err != nil {
			return nil, fmt.Errorf("unpack data %s: %w", event.Name, err)
		}
	}
	var indexed abi.Arguments
	for _, inp := range event.Inputs {
		if inp.Indexed {
			indexed = append(indexed, inp)
		}
	}
	if len(indexed) > 0 {
		if err := abi.ParseTopicsIntoMap(args, indexed, lg.Topics[1:]); err != nil {
			return nil, fmt.Errorf("parse topics %s: %w", event.Name, err)
		}
	}

	topics := make([]string, len(lg.Topics))
	for i, t := range lg.Topics {
		topics[i] = t.Hex()
	}
	return &ParsedEvent{
		ContractAddress: strings.ToLower(lg.Address.Hex()),
		EventName:       event.Name,
		Topic0:          lg.Topics[0].Hex(),
		Topics:          topics,
		Args:            normalizeArgs(args),
	}, nil
}

func normalizeArgs(in map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		switch t := v.(type) {
		case common.Address:
			out[k] = strings.ToLower(t.Hex())
		case []byte:
			out[k] = "0x" + common.Bytes2Hex(t)
		case *[32]byte:
			out[k] = "0x" + common.Bytes2Hex(t[:])
		default:
			out[k] = t
		}
	}
	return out
}

// ArgsJSON 序列化解析参数。
func (p *ParsedEvent) ArgsJSON() (string, error) {
	raw, err := json.Marshal(p.Args)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}
