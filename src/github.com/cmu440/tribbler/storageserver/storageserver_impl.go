package storageserver

import (
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"strconv"
	"sync"
	"time"
)

type storageServer struct {
	dataLock *sync.Mutex

	// the hostport of the master server in the cluster
	masterHostport string

	// whether or not this server is the master server
	isMaster bool

	// whether or not all servers in the cluster are ready
	clusterIsReady bool

	// the number of nodes in the ring
	numNodes int

	// the Node representing the current server
	thisNode *storagerpc.Node

	// the Node representing the server preceding this one in the hash ring
	// (set to nil if none exists)
	prevNode *storagerpc.Node

	// the Node representing the server following this one in the hash ring
	// (set to nil if none exists)
	nextNode *storagerpc.Node

	// a list of storage servers, sorted in increasing order of nodeID
	hashRing []storagerpc.Node

	// TODO: datamap type
	// TODO: lease-tracking data type
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	thisNode := &storagerpc.Node{
		HostPort: net.JoinHostPort("localhost", strconv.Itoa(port)),
		NodeID:   nodeID,
	}

	rawServerData := &storageServer{
		dataLock:       new(sync.Mutex),
		masterHostport: masterServerHostPort,
		isMaster:       (len(masterServerHostPort) == 0),
		clusterIsReady: false,
		numNodes:       numNodes,
		thisNode:       thisNode,
		// remember to initialize prevNode, nextNode, and hashRing later!
		prevNode: nil,
		nextNode: nil,
		hashRing: nil,
	}

	// make server methods available for rpc
	rpc.RegisterName(RPC_NAME, storagerpc.Wrap(rawServerData))
	rpc.HandleHTTP()

	listenSocket, err := net.Listen("tcp", rawServerData.thisNode.HostPort)
	if err != nil {
		errMsg := fmt.Sprintf("storage server %s listen setup error: %s",
			nodeToStr(rawServerData.thisNode), err.Error())
		_DEBUGLOG.Println(errMsg)
		return nil, errors.New(errMsg)
	}
	go http.Serve(listenSocket, nil)

	// initialize hash ring information about other storage servers
	if rawServerData.isMaster {
		err = initMasterServerHashRing(rawServerData)
	} else {
		err = initSlaveServerHashRing(rawServerData)
	}

	if err != nil {
		_DEBUGLOG.Println(err.Error())
		return nil, err
	}

	return rawServerData, nil
}

// blocks until all slaves are registered and entire hashRing is created
func initMasterServerHashRing(masterData *storageServer) error {
	_DEBUGLOG.Printf("initializing master %s...\n",
		nodeToStr(masterData.thisNode))

	hashRing := make([]storagerpc.Node, masterData.numNodes)

	// remember to add the master server to the hash ring
	hashRing = append(hashRing, *masterData.thisNode)

	// TODO: wait until all slave servers registered
	serversSeen := 1

	for serversSeen < masterData.numNodes {
		select {} // TODO: implement!
	}

	// sort by ascending node id
	sort.Sort(sortByNodeID(hashRing))

	return errors.New("not yet implemented")
}

// blocks until the slave server is ready and has received data from the master
func initSlaveServerHashRing(slaveData *storageServer) error {
	_DEBUGLOG.Printf("initializing slave %s...\n",
		nodeToStr(slaveData.thisNode))

	var masterClient *rpc.Client
	var err error

	for {
		masterClient, err = rpc.DialHTTP("tcp", slaveData.masterHostport)
		if err != nil {
			errMsg := fmt.Sprintf("slave %s dial master error: %s",
				nodeToStr(slaveData.thisNode), err.Error())
			_DEBUGLOG.Println(errMsg)
			_DEBUGLOG.Printf("%s slave retrying dial...\n", nodeToStr(slaveData.thisNode))
			time.Sleep(_INIT_RETRY_INTERVAL)
		} else {
			break
		}
	}

	registerArgs := &storagerpc.RegisterArgs{
		ServerInfo: *(slaveData.thisNode),
	}
	var registerReply *storagerpc.RegisterReply

	for {
		err = masterClient.Call(fmt.Sprintf("%s.RegisterServer", RPC_NAME),
			registerArgs, &registerReply)
		// handle error in call to registration
		if err != nil {
			_DEBUGLOG.Printf("slave %s register error: %s\n",
				nodeToStr(slaveData.thisNode), err.Error())
		} else if registerReply.Status == storagerpc.OK {
			// handle call in which all servers are ready by saving the hash
			// ring and neighbor nodes of this slave server

			// wrapped in a function to ensure that lock is released
			return func() error {
				slaveData.dataLock.Lock()
				defer slaveData.dataLock.Unlock()

				hashRing := registerReply.Servers
				slaveData.hashRing = hashRing

				thisNodeIndex := getHashRingNodeIndex(hashRing,
					slaveData.thisNode.NodeID)

				if thisNodeIndex < 0 {
					panic(fmt.Sprintf("hash ring error, slave %s not found\n",
						nodeToStr(slaveData.thisNode)))
				}

				_DEBUGLOG.Println("received HASHRING:", hashRing)
				// get the prevNode and nextNode references, if available
				if slaveData.numNodes > 1 {
					prevIndex := posModulo(thisNodeIndex-1, len(hashRing))
					nextIndex := posModulo(thisNodeIndex+1, len(hashRing))

					slaveData.prevNode = &hashRing[prevIndex]
					slaveData.nextNode = &hashRing[nextIndex]
				}

				slaveData.clusterIsReady = true
				_DEBUGLOG.Printf("%s registered; master ready!\n",
					nodeToStr(slaveData.thisNode))
				return nil
			}()
		} else if registerReply.Status == storagerpc.NotReady {
			_DEBUGLOG.Printf("%s registered; master not yet ready\n",
				nodeToStr(slaveData.thisNode))
		}
		_DEBUGLOG.Printf("%s slave retrying register...\n", nodeToStr(slaveData.thisNode))
		time.Sleep(_INIT_RETRY_INTERVAL)
	}

	return errors.New("not yet implemented")
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	ss.dataLock.Lock()
	defer ss.dataLock.Unlock()

	if ss.clusterIsReady {
		reply.Status = storagerpc.OK
		reply.Servers = ss.hashRing
	} else {
		reply.Status = storagerpc.NotReady
		reply.Servers = nil
	}
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	return errors.New("not implemented")
}
