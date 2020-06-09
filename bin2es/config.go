package bin2es

import (
	//系统
	"encoding/json"
	"io/ioutil"
	//第三方
	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

type Config struct {
	Bin2Es     Bin2Es     `toml:"bin2es"`
	MasterInfo MasterInfo `toml:"master_info"`
	Zk         Zk         `toml:"zk"`
	Etcd       Etcd       `toml:"etcd"`
	Es         Es         `toml:"es"`
	Mysql      Mysql      `toml:"mysql"`
	Sources    []Source   `toml:"source"`
}
type Bin2Es struct {
	SyncChLen int `toml:"sync_ch_len"`
}
type MasterInfo struct {
	Addr          string `toml:"addr"`
	Port          int    `toml:"port"`
	User          string `toml:"user"`
	Pwd           string `toml:"pwd"`
	Charset       string `toml:"charset"`
	Schema        string `toml:"schema"`
	Table         string `toml:"table"`
	FlushDuration int    `toml:"flush_duration"`
}
type Zk struct {
	Enable         bool     `toml:"enable"`
	LockPath       string   `toml:"lock_path"`
	SessionTimeout int      `toml:"session_timeout"`
	Hosts          []string `toml:"hosts"`
}
type Etcd struct {
	Enable      bool     `toml:"enable"`
	EnableTLS   bool     `toml:"enable_tls"`
	LockPath    string   `toml:"lock_path"`
	DialTimeout int      `toml:"dial_timeout"`
	CertPath    string   `toml:"cert_path"`
	KeyPath     string   `toml:"key_path"`
	CaPath      string   `toml:"ca_path"`
	Endpoints   []string `toml:"endpoints"`
}
type Es struct {
	Nodes                []string `toml:"nodes"`
	User                 string   `toml:"user"`
	Passwd               string   `toml:"passwd"`
	EnableAuthentication bool     `toml:"enable_authentication"`
	InsecureSkipVerify   bool     `toml:"insecure_skip_verify"`
	BulkSize             int      `toml:"bulk_size"`
	FlushDuration        int      `toml:"flush_duration"`
}
type Mysql struct {
	Addr     string `toml:"addr"`
	Port     uint64 `toml:"port"`
	User     string `toml:"user"`
	Pwd      string `toml:"pwd"`
	Charset  string `toml:"charset"`
	ServerID uint32 `toml:"server_id"`
}
type Source struct {
	Schema    string   `toml:"schema"`
	DumpTable string   `toml:"dump_table"`
	Tables    []string `toml:"tables"`
}

func NewConfigWithFile(path string) (*Config, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewConfig(string(data))
}

// NewConfig creates a Config from data.
func NewConfig(data string) (*Config, error) {
	var c Config

	_, err := toml.Decode(data, &c)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &c, nil
}

type Pos2Field map[string]int
type Field map[string]string
type Replace map[string]string
type Pipeline map[string]interface{}
type ROWS []map[string]interface{}

type Bin2esConfig []struct {
	Schema    string     `json:"schema"`
	Tables    []string   `json:"tables"`
	Actions   []string   `json:"actions"`
	Pipelines []Pipeline `json:"pipelines"`
	Dest      Dest       `json:"dest"`
}
type Dest struct {
	Index string `json:"index"`
}

func NewBin2esConfig(path string, config *Bin2esConfig) error {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	//读取的数据为json格式，需要进行解码
	err = json.Unmarshal(data, config)
	if err != nil {
		return err
	}

	return nil
}
