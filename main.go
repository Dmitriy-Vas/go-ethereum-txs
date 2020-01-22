package main

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v4"
	"github.com/spf13/cobra"
	"log"
)

type Config struct {
	Database struct {
		Port uint16
		Host string
		User string
		Pass string
		Ref  string
	}
	Node struct {
		IPC      string
		KeyStore string
	}
}

var config *Config

func init() {
	config = new(Config)
	cmd := &cobra.Command{
		Short: "A program to save ethereum transactions to the database.",
		Long:  "The program watches all transactions from/to addresses in keystore and saves them to the postgresql database.",
	}
	flags := cmd.PersistentFlags()
	flags.Uint16Var(&config.Database.Port, "db_port", 5432, "")
	flags.StringVar(&config.Database.Host, "db_host", "127.0.0.1", "")
	flags.StringVar(&config.Database.User, "db_user", "root", "")
	flags.StringVar(&config.Database.Pass, "db_pass", "", "")
	flags.StringVar(&config.Database.Ref, "db_ref", "", "")
	flags.StringVar(&config.Node.IPC, "ipc", "~/.ethereum/geth.ipc", "")
	flags.StringVar(&config.Node.KeyStore, "ks", "~/.ethereum/keystore/", "")
	if err := cmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func isErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

type Client struct {
	RPC *rpc.Client
	ETH *ethclient.Client
	ctx context.Context
}

func NewClient() *Client {
	rpcClient, err := rpc.Dial(config.Node.IPC)
	isErr(err)
	ethClient := ethclient.NewClient(rpcClient)
	return &Client{
		RPC: rpcClient,
		ETH: ethClient,
		ctx: context.Background(),
	}
}

func (c *Client) Subscribe() (chan *types.Header, ethereum.Subscription) {
	headers := make(chan *types.Header)
	sub, err := c.ETH.SubscribeNewHead(c.ctx, headers)
	isErr(err)
	return headers, sub
}

func (c *Client) Signer() types.Signer {
	chainId, err := c.ETH.ChainID(c.ctx)
	isErr(err)
	return types.NewEIP155Signer(chainId)
}

type KeyStore struct {
	KS        *keystore.KeyStore
	addresses []common.Address
}

func NewKeyStore() *KeyStore {
	ks := &KeyStore{
		KS:        keystore.NewKeyStore(config.Node.KeyStore, keystore.StandardScryptN, keystore.StandardScryptP),
		addresses: make([]common.Address, 0),
	}
	go func() {
		subChan := make(chan accounts.WalletEvent)
		defer close(subChan)
		sub := ks.KS.Subscribe(subChan)
		for {
			select {
			case err := <-sub.Err():
				isErr(err)
			case event := <-subChan:
				switch event.Kind {
				case accounts.WalletArrived:
					ks.AddAccounts(event.Wallet.Accounts())
				case accounts.WalletDropped:
					ks.DelAccounts(event.Wallet.Accounts())
				}
			}
		}
	}()
	for _, acc := range ks.KS.Accounts() {
		ks.addresses = append(ks.addresses, acc.Address)
	}
	return ks
}

func (ks *KeyStore) AddAccounts(accounts []accounts.Account) {
	for _, acc := range accounts {
		ks.addresses = append(ks.addresses, acc.Address)
	}
}

func (ks *KeyStore) DelAccounts(accounts []accounts.Account) {
	for i, acc := range accounts {
		for _, adr := range ks.addresses {
			if acc.Address == adr {
				ks.addresses = append(ks.addresses[:i], ks.addresses[i+1:]...)
				break
			}
		}
	}
}

func (ks *KeyStore) AddressExists(message types.Message) bool {
	to := message.To()
	if to == nil {
		return false
	}
	from := message.From()

	check := func(adr common.Address) bool {
		for _, acc := range ks.addresses {
			if acc == adr {
				return true
			}
		}
		return false
	}
	return check(*to) || check(from)
}

type Database struct {
	Conn *pgx.Conn
	ctx  context.Context
}

func NewDatabase() *Database {
	db := &Database{
		ctx: context.Background(),
	}
	address := fmt.Sprintf("postgresql://%s:%d/%s?user=%s&password=%s",
		config.Database.Host,
		config.Database.Port,
		config.Database.Ref,
		config.Database.User,
		config.Database.Pass)
	conn, err := pgx.Connect(db.ctx, address)
	isErr(err)
	db.Conn = conn
	db.Init()
	return db
}

func (d *Database) Init() {
	_, err := d.Conn.Exec(d.ctx, `
	BEGIN;
	CREATE SCHEMA IF NOT EXISTS ethereum;
	CREATE TABLE IF NOT EXISTS ethereum.transactions
	(
    	tx_hash  VARCHAR(66) PRIMARY KEY,
    	tx_from  VARCHAR(44) NOT NULL,
    	tx_to    VARCHAR(44) NOT NULL,
    	tx_value VARCHAR     NOT NULL,
    	tx_time  TIMESTAMP   DEFAULT now()
	);
	COMMIT;
	`)
	isErr(err)
}

func (d *Database) AddTransaction(message types.Message, hash common.Hash) {
	_, err := d.Conn.Exec(d.ctx, `
	INSERT INTO ethereum.transactions (tx_hash, tx_from, tx_to, tx_value)
	VALUES ($1, $2, $3, $4)
	ON CONFLICT DO NOTHING;
	`,
		hash.Hex(),
		message.From().Hex(),
		message.To().Hex(),
		message.Value().String())
	isErr(err)
}

func main() {
	db := NewDatabase()
	defer db.Conn.Close(db.ctx)

	client := NewClient()
	defer client.RPC.Close()
	signer := client.Signer()

	headers, sub := client.Subscribe()
	defer sub.Unsubscribe()
	defer close(headers)
	ks := NewKeyStore()
	log.Printf("Ready to listen, loaded %d addresses...", len(ks.addresses))

	for {
		select {
		case err := <-sub.Err():
			isErr(err)
		case header := <-headers:
			go func(h *types.Header) {
				block, err := client.ETH.BlockByHash(client.ctx, header.Hash())
				isErr(err)
				for _, tx := range block.Transactions() {
					msg, err := tx.AsMessage(signer)
					isErr(err)
					if !ks.AddressExists(msg) {
						continue
					}
					db.AddTransaction(msg, tx.Hash())
				}
			}(header)
		}
	}
}
