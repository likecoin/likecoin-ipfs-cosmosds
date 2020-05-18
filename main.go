package main

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	gocid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/core/coreapi"
	"github.com/ipfs/go-ipfs/plugin/loader"
	"github.com/ipfs/go-ipfs/repo/fsrepo"

	config "github.com/ipfs/go-ipfs-config"
	libp2p "github.com/ipfs/go-ipfs/core/node/libp2p"
	icore "github.com/ipfs/interface-go-ipfs-core"

	iscnipldplugin "github.com/likecoin/iscn-ipld/plugin"
	iscn "github.com/likecoin/iscn-ipld/plugin/block"
	"github.com/likecoin/likecoin-ipfs-cosmosds/cosmosds"
)

const defaultTmEndpoint = "tcp://127.0.0.1:26657"

func setupDefaultDatastoreConfig(cfg *config.Config, tmEndpoint string) *config.Config {
	cfg.Datastore.Spec = map[string]interface{}{
		"mountpoint": "/",
		"type":       "measure",
		"prefix":     "cosmossdk.datastore",
		"child": map[string]interface{}{
			"type":                "cosmosds",
			"path":                "datastore",
			"compression":         "none",
			"tendermint-endpoint": tmEndpoint,
		},
	}
	return cfg
}

func setupNode(ctx context.Context, tmEndpoint string) (
	*loader.PluginLoader,
	icore.CoreAPI,
	error) {
	loader.Preload(cosmosds.Plugins...)
	loader.Preload(iscnipldplugin.Plugins...)

	rootPath, err := filepath.Abs("./ipfs")
	if err != nil {
		log.Printf("Cannot parse path: %s", err)
		return nil, nil, err
	}

	plugins, err := loader.NewPluginLoader(rootPath)
	if err != nil {
		log.Printf("error loading plugins: %s", err)
		return nil, nil, err
	}

	if err := plugins.Initialize(); err != nil {
		log.Printf("error initializing plugins: %s", err)
		return nil, nil, err
	}

	if err := plugins.Inject(); err != nil {
		log.Printf("error initializing plugins: %s", err)
		return nil, nil, err
	}

	rootPath, err = config.Path(rootPath, "")
	if err != nil {
		log.Printf("Cannot set config path: %s", err)
		return nil, nil, err
	}

	cfg, err := config.Init(ioutil.Discard, 2048)
	if err != nil {
		log.Printf("Cannot init config: %s", err)
		return nil, nil, err
	}
	cfg = setupDefaultDatastoreConfig(cfg, tmEndpoint)

	err = fsrepo.Init(rootPath, cfg)
	if err != nil {
		log.Printf("Cannot init repo: %s", "ab")
		return nil, nil, err
	}

	repo, err := fsrepo.Open(rootPath)
	if err != nil {
		log.Printf("Cannot open repo: %s", err)
		return nil, nil, err
	}

	nodeOptions := &core.BuildCfg{
		Online:  true,
		Routing: libp2p.DHTOption,
		Repo:    repo,
	}

	node, err := core.NewNode(ctx, nodeOptions)
	if err != nil {
		log.Printf("Cannot create node: %s", err)
		return nil, nil, err
	}
	node.IsDaemon = true

	ipfs, err := coreapi.NewCoreAPI(node)
	if err != nil {
		log.Println("No IPFS repo available on the default path")
		return nil, nil, err
	}

	log.Println("Start plugin")
	err = plugins.Start(node)
	if err != nil {
		log.Printf("Cannot start plugins: %s", err)
		return nil, nil, err
	}

	return plugins, ipfs, nil
}

func main() {
	tmEndpoint := defaultTmEndpoint
	if len(os.Args) > 2 {
		tmEndpoint = os.Args[2]
	}
	fmt.Printf("Using Tendermint endpoint %s\n", tmEndpoint)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Println("Setting up IPFS node ...")
	plugins, ipfs, err := setupNode(ctx, tmEndpoint)
	if err != nil {
		log.Panicf("Failed to set up IPFS node")
	}

	cleaned := false
	cleanup := func() {
		if cleaned {
			return
		}
		log.Println("Close plugin")
		err := plugins.Close()
		if err != nil {
			log.Panicf("Cannot close plugins: %s", err)
		}
		os.Exit(0)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		log.Printf("\nGet signal: \"%v\"", sig)
		cleanup()
	}()

	defer cleanup()
	log.Println("IPFS node is created")

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("Enter CID to get (Ctrl+C to stop service):")
		cidStr, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("Readline error: %s\n", err)
			break
		}
		cidStr = strings.TrimSpace(cidStr)
		if cidStr == "" {
			fmt.Print("Got empty string, closing")
			break
		}
		cid, err := gocid.Decode(cidStr)
		if err != nil {
			fmt.Printf("Error when decoding CID: %s\n", err)
			continue
		}
		node, err := ipfs.Dag().Get(ctx, cid)
		if err != nil {
			fmt.Printf("Can't get CID %s: %s\n", cidStr, err)
			continue
		}
		fmt.Printf("Got CID %s\n", cidStr)
		obj, ok := node.(iscn.IscnObject)
		if !ok {
			fmt.Printf("Not an ISCN object\n")
			continue
		}
		bz, err := obj.MarshalJSON()
		if err != nil {
			fmt.Printf("Can't marshal ISCN object to JSON: %s\n", err)
			continue
		}
		fmt.Println(string(bz))
	}

}
