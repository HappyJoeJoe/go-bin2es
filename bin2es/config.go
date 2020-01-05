package bin2es

import (
	"io/ioutil"
	"encoding/json"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

type Config struct {
	DataDir  string   `toml:"data_dir"`
	Es       Es       `toml:"es"`
	Mysql    Mysql    `toml:"mysql"`
	Sources  []Source `toml:"source"`
}
type Es struct {
	Nodes         []string `toml:"nodes"`
	BulkSize      int      `toml:"bulk_size"`
	FlushDuration int      `toml:"flush_duration"`
}
type Mysql struct {
	Addr     string `toml:"addr"`
	User     string `toml:"user"`
	Pwd      string `toml:"pwd"`
	Charset  string `toml:"charset"`
	ServerID uint32 `toml:"server_id"`
}
type Source struct {
	Schema  string   `toml:"schema"`
	Tables  []string `toml:"tables"`
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

type Pos2Fields []map[string]int
type Fields     []map[string]string
type ROWS       []map[string]interface{}

type Bin2esConfig []struct {
	Schema   string   `json:"schema"`
	Tables   []string `json:"tables"`
	Actions  []string `json:"actions"`
	Pipeline Pipeline `json:"pipeline"`
	Dest     Dest     `json:"dest"`
}
type PkDoSQL struct {
	SQL     string `json:"sql"`
}
type NestedObj struct {
	Common string   `json:"common"`
	Fields Fields   `json:"fields"`
}
type NestedArray struct {
	SQLField   string       `json:"sql_field"`
	Common     string       `json:"common"`
	Pos2Fields Pos2Fields   `json:"pos2fields"`
}
type Pipeline struct {
	PkDoSQL     PkDoSQL     `json:"PkDoSQL"`
	NestedObj   NestedObj   `json:"NestedObj"`
	NestedArray NestedArray `json:"NestedArray"`
}
type Dest struct {
	Index  string `json:"index"`
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