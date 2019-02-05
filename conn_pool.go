package mongo_go_driver_connection_pool

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/mongodb/mongo-go-driver/mongo"
	"sync"
	"time"
)

type clientWithCreation struct {
	//mongo client
	db *mongo.Client
	//creation time of client in nano
	creationTime int64
}

type ConnPool struct {
	//used connection
	connectionsInUse map[[16]byte]clientWithCreation
	//unused connection
	freeConnections map[[16]byte]clientWithCreation
	//mutex loc
	lock sync.RWMutex
	//pool size
	poolSize int
	// conn string
	connStr string
	//Conn expiry in nano second, default is 5 minutes
	expiry int64
}

func CreateConnPool(poolSize int, connStr string) (*ConnPool, error) {
	var mu sync.RWMutex
	connPool := &ConnPool{
		connectionsInUse: make(map[[16]byte]clientWithCreation),
		freeConnections:  make(map[[16]byte]clientWithCreation),
		lock:             mu,
		poolSize:         poolSize,
		connStr:          connStr,
		expiry:           300000000000,
	}
	return connPool, nil
}

func (c *ConnPool) Checkout() (*mongo.Client, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	//Fill the connection pool for the first time
	if len(c.freeConnections) == 0 && len(c.connectionsInUse) == 0 {
		for i := 0; i < c.poolSize; i++ {
			err := c.createConnections()
			if err != nil {
				return nil, err
			}
		}
	}

	now := time.Now().UnixNano()

	//Fill the difference
	for i := len(c.freeConnections) + len(c.connectionsInUse); i <= c.poolSize; i++ {
		err := c.createConnections()
		if err != nil {
			return nil, err
		}
	}

	if len(c.freeConnections) > 0 {
		for k, v := range c.freeConnections {
			// Delete the expired connections
			if now-v.creationTime > c.expiry {
				delete(c.freeConnections, k)
				err := c.kill(v.db)
				if err != nil {
					return nil, fmt.Errorf("error while closing the connection :: %v", err)
				}
			} else {
				// Delete the non active connections
				if err := c.validate(v.db); err != nil {
					delete(c.freeConnections, k)
					err := c.kill(v.db)
					if err != nil {
						return nil, fmt.Errorf("error while closing the connection :: %v", err)
					}
				} else {
					delete(c.freeConnections, k)
					v.creationTime = now
					c.connectionsInUse[k] = v
					return v.db, nil
				}
			}
		}
	}

	//Need a new connection because all free connection is busy
	//Or number of connections crossing the limit
	client, e := c.create()
	if e != nil {
		return nil, fmt.Errorf("error while creating the connection :: %v", e)
	}
	hash := c.hash(*client)
	c.connectionsInUse[hash] = clientWithCreation{
		db:           client,
		creationTime: now,
	}
	return client, nil
}

func (c *ConnPool) CheckIn(client *mongo.Client) {
	c.lock.Lock()
	defer c.lock.Unlock()
	hash := c.hash(*client)
	delete(c.connectionsInUse, hash)
	//Add only if the size of active conn is less than the max
	if len(c.freeConnections)+len(c.connectionsInUse) < c.poolSize {
		c.freeConnections[hash] = clientWithCreation{
			db:           client,
			creationTime: time.Now().UnixNano(),
		}
	}
}

func (c *ConnPool) create() (*mongo.Client, error) {
	client, e := mongo.NewClient(c.connStr)
	if e != nil {
		return nil, fmt.Errorf("error at mongo db creation :: %v", e)
	}
	return client, nil
}

func (c *ConnPool) validate(client *mongo.Client) error {
	err := client.Connect(context.Background())
	if err != nil {
		return fmt.Errorf("error in connecting mongo :: %v", err)
	}
	return nil
}

func (c *ConnPool) kill(client *mongo.Client) error {
	return client.Disconnect(context.Background())
}

func (c *ConnPool) hash(arr mongo.Client) [16]byte {
	arrBytes := make([]byte, 0)
	jsonBytes, _ := json.Marshal(arr)
	arrBytes = append(arrBytes, jsonBytes...)
	return md5.Sum(arrBytes)
}

func (c *ConnPool) createConnections() error {
	client, e := c.create()
	if e != nil {
		return fmt.Errorf("error at creating the connection pool :: %v", e)
	}

	e = c.validate(client)
	if e != nil {
		return fmt.Errorf("error at creating the connection pool :: %v", e)
	}

	customClient := clientWithCreation{
		db:           client,
		creationTime: time.Now().UnixNano(),
	}
	hash := c.hash(*client)
	c.freeConnections[hash] = customClient
	return nil
}
