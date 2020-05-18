package cosmosds

import (
	"fmt"
	"path/filepath"

	"github.com/ipfs/go-ipfs/plugin"
	"github.com/ipfs/go-ipfs/repo"
	"github.com/ipfs/go-ipfs/repo/fsrepo"

	ldbopts "github.com/syndtr/goleveldb/leveldb/opt"
)

// Plugins is exported list of plugins that will be loaded
var Plugins = []plugin.Plugin{
	&Plugin{},
}

// Plugin is the main structure.
type Plugin struct {
	ds *Datastore
}

// Static (compile time) check that Plugin satisfies the plugin.PluginDatastore interface.
var _ plugin.PluginDatastore = (*Plugin)(nil)

// Name returns the name of Plugin
func (*Plugin) Name() string {
	return "ds-cosmos"
}

// Version returns the version of Plugin
func (*Plugin) Version() string {
	return "0.1.0"
}

// Init Plugin
func (*Plugin) Init(_ *plugin.Environment) error {
	return nil
}

// DatastoreTypeName defines the type name of the datastore
func (*Plugin) DatastoreTypeName() string {
	return "cosmosds"
}

type datastoreConfig struct {
	pl          *Plugin
	path        string
	compression ldbopts.Compression
	tmEndpoint  string
}

// DatastoreConfigParser returns a configuration stub for a Cosmos datastore from the given parameters
func (p *Plugin) DatastoreConfigParser() fsrepo.ConfigFromMap {
	return func(params map[string]interface{}) (fsrepo.DatastoreConfig, error) {
		var c datastoreConfig
		var ok bool

		c.path, ok = params["path"].(string)
		if !ok {
			return nil, fmt.Errorf("'path' field is missing or not string")
		}

		c.tmEndpoint, ok = params["tendermint-endpoint"].(string)
		if !ok {
			return nil, fmt.Errorf("'tendermint-endpoint' field is missing or not string")
		}

		switch cm := params["compression"].(string); cm {
		case "none":
			c.compression = ldbopts.NoCompression
		case "snappy":
			c.compression = ldbopts.SnappyCompression
		case "":
			c.compression = ldbopts.DefaultCompression
		default:
			return nil, fmt.Errorf("unrecognized value for compression: %s", cm)
		}

		c.pl = p
		return &c, nil
	}
}

func (c *datastoreConfig) DiskSpec() fsrepo.DiskSpec {
	return map[string]interface{}{
		"type": "cosmosds",
		"path": c.path,
	}
}

func (c *datastoreConfig) Create(path string) (repo.Datastore, error) {
	p := c.path
	if !filepath.IsAbs(p) {
		p = filepath.Join(path, p)
	}

	ds, err := NewDatastore(p, c.tmEndpoint, &Options{
		Compression: c.compression,
	})
	if err != nil {
		return nil, err
	}

	c.pl.ds = ds
	return ds, nil
}
