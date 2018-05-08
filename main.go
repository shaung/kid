package main

import (
    "fmt"
    "log"
    "strings"
    "flag"
    "math"
    "time"
    "strconv"

    "github.com/tidwall/redcon"
    "github.com/hashicorp/consul/api"
)

type Config struct {
    port string
    node_id int
    machine_id int
    consul_addr string
}

func (c *Config) parse() {
    flag.StringVar(&c.port, "port", "6379", "port, defaults to 6379")
    flag.IntVar(&c.machine_id, "machine", 0, "0-31, machine id, unique per host")
    flag.IntVar(&c.node_id, "node", 0, "0-31, node id, unique per process")

    flag.StringVar(&c.consul_addr, "consul_addr", "127.0.0.1:8500", "consul host, defaults to 127.0.0.1:8500")
    flag.Parse()
}

func (c *Config) welcome() {
    log.Printf("  machine_id: %d", c.machine_id)
    log.Printf("     node_id: %d", c.node_id)
    log.Printf("        port: %s", c.port)
}


const epoch = 1451606400  // 2016-1-1

const TS_OFFSET = 29
const MACHINE_OFFSET = 23
const NODE_OFFSET = 17

const TS_MASK = 0x1FFFFFFF
const MACHINE_MASK = 0x3F
const NODE_MASK = 0x3F
const SEQ_MASK = 0x1FFFF

// 每秒最多可以生成 2^18-1 个ID
var MAX_SEQ int64 = int64(math.Pow(2, 18)) - 1

// 上次的时间戳
var last_ts int64 = -1
// 计数器(每秒清零)
var seq int64 = 0

var kv *api.KV

var config *Config


func get_last_ts_key(machine_id int) string {
    return fmt.Sprintf("machine_%d/ts", machine_id)
}


func check_last_ts(ts int64, machine_id int, kv *api.KV) (result int, err error) {
    // 检查时钟是否有后退
    key := get_last_ts_key(machine_id)
    kvpair, _, err := kv.Get(key, nil)
    if err != nil {
        log.Panic(err)
    }

    if kvpair == nil {
        return 1, nil
    }

    _last_ts, err := strconv.ParseInt(string(kvpair.Value), 10, 64)
    if err != nil {
        log.Panic(err)
    }

    if _last_ts == 0 {
        return 1, nil
    } else {
        if ts < _last_ts {
            return -1, fmt.Errorf("Bad datetime: ts %d < last %d", ts, _last_ts)
        }
    }

    return 1, nil
}

func update_last_ts(ts int64, machine_id int, kv *api.KV) (result int, err error) {
    key := get_last_ts_key(machine_id)
    value := []byte(strconv.FormatInt(ts, 10))
    kvpair := api.KVPair{Key: key, Value: value}
    _, err = kv.Put(&kvpair, nil)
    if err != nil {
        log.Panic(err)
    }
    return 1, nil
}

func make_kv(consul_addr string) (kv *api.KV) {
    config := api.DefaultConfig()
    config.Address = consul_addr
    client, err := api.NewClient(config)
    if err != nil {
        log.Fatal("Consul is down")
    }
    return client.KV()
}

func gen(machine_id int, node_id int) (id int64, err error) {
    /*
    ID：64位整数
    组成（从高至低）
    - 标志位，1位
    - 时间戳，精确到秒，30位，可以用34年
    - 系统标识，5位，预留
    - 数据中心，5位，32个
    - 节点，5位，32个
    - 自增ID，18位，最大值262144-1
    */
    now := time.Now().Unix()

    if last_ts != -1 && now < last_ts {
        // 发生时钟回调时，直接返回错误
        return -1, fmt.Errorf("Please wait for %d s", last_ts - now)
    }

    if last_ts == now {
        seq += 1
        if (seq >= MAX_SEQ) {
            // 超过每秒最大限制（不太可能发生），等待1秒
            return -1, fmt.Errorf("Please wait for 1s")
        }
    } else {
        seq = 0
    }

    fmt.Println("last_ts %d", last_ts)
    fmt.Println("now %d", now)
    fmt.Println("epoch %d", epoch)

    ts := now - epoch
    last_ts = now

    fmt.Println("ts %d", ts)
    fmt.Println("ts %d", ts << TS_OFFSET)
    fmt.Println("machine_id %d", machine_id << MACHINE_OFFSET)
    fmt.Println("node_id %d", node_id << NODE_OFFSET)
    fmt.Println("seq %d", seq & SEQ_MASK)

    id = ((int64(ts) & TS_MASK) << TS_OFFSET) +
         ((int64(machine_id) & MACHINE_MASK) << MACHINE_OFFSET) +
         ((int64(node_id) & NODE_MASK) << NODE_OFFSET) +
         (seq & SEQ_MASK)

    log.Printf("id= %d", id)

    return id, nil
}


func main() {
    config = &Config{}
    config.parse()
    config.welcome()

    kv := make_kv(config.consul_addr)

    now := time.Now().Unix()
    result, _err := check_last_ts(now, config.machine_id, kv)
    if _err != nil {
        log.Panic(_err)
    }
    if result == 0 {
        log.Panic("error")
    }

    addr := ":" + config.port

    log.Printf("started server at %s", addr)
    err := redcon.ListenAndServe(addr,
        func(conn redcon.Conn, cmd redcon.Command) {
            switch strings.ToLower(string(cmd.Args[0])) {
            default:
                conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
            case "ping":
                conn.WriteString("PONG")
            case "quit":
                conn.WriteString("OK")
                conn.Close()
            case "select":
                conn.WriteString("OK")
            case "set":
                conn.WriteString("OK")
            case "info":
                s := fmt.Sprintf(
                    "machine_id: %d\r\n   node_id: %d\r\n      port: %s\r\n",
                    config.machine_id,
                    config.node_id,
                    config.port)
                conn.WriteBulk([]byte(s))
            case "incr":
                val, err := gen(config.machine_id, config.node_id)
                if err != nil {
                    conn.WriteError("ERR " + err.Error())
                } else {
                    conn.WriteInt64(val)
                    update_last_ts(last_ts, config.machine_id, kv)
                }
            case "del":
                conn.WriteInt(0)
            }
        },
        func(conn redcon.Conn) bool {
            // use this function to accept or deny the connection.
            log.Printf("accept: %s", conn.RemoteAddr())
            return true
        },
        func(conn redcon.Conn, err error) {
            // this is called when the connection has been closed
            log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
        },
    )
    if err != nil {
        log.Fatal(err)
    }
}
