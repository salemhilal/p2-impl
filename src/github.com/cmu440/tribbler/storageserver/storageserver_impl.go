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

	// if the server is the master server, this will be the channel it will
	// receive new registrations on. If not, this will be nil
	registrationReceiveChan chan storagerpc.Node
	// if the server is the master server, this will be the channel it will
	// send its the final hashring back to the RegisterServer rpc call on
	registrationResponseChan chan storagerpc.Status

	// whether or not all servers in the cluster are ready
	clusterIsReady bool

	// the number of nodes in the ring
	numNodes int

	// the Node representing the current server
	thisNode *storagerpc.Node

	// the Node representing the server preceding this one in the hash ring,
	// with wraparound
	// (set to nil if none exists)
	prevNode *storagerpc.Node

	// the Node representing the server following this one in the hash ring,
	// with wraparound
	// (set to nil if none exists)
	nextNode *storagerpc.Node

	// a list of storage servers, sorted in increasing order of nodeID
	hashRing []storagerpc.Node

	// TODO: datamap type
	// TODO: lease-tracking data type
}

// returns the nodes preceding and succeeding the given server's node in the
// given hashRing. Returns an error if the given node is not in the hashRing.
// Returns nil if no preceding or succeeding nodes exist
// (due to having one node)
func (ss *storageServer) findPrevNextNodes(hashRing []storagerpc.Node) (*storagerpc.Node, *storagerpc.Node, error) {
	thisNodeIndex := getHashRingNodeIndex(hashRing,
		ss.thisNode.NodeID)

	if thisNodeIndex < 0 {
		return nil, nil, errors.New(
			fmt.Sprintf("hash ring error, node %s not found\n",
				nodeToStr(ss.thisNode)))
	}

	// get the prevNode and nextNode references, if available
	if ss.numNodes > 1 {
		prevIndex := posModulo(thisNodeIndex-1, len(hashRing))
		nextIndex := posModulo(thisNodeIndex+1, len(hashRing))

		prevNode := &hashRing[prevIndex]
		nextNode := &hashRing[nextIndex]
		return prevNode, nextNode, nil
	}
	return nil, nil, nil
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

		registrationReceiveChan:  nil,
		registrationResponseChan: nil,
	}

	if rawServerData.isMaster {
		rawServerData.registrationReceiveChan = make(chan storagerpc.Node)
		rawServerData.registrationResponseChan = make(chan storagerpc.Status, 1)
	}

	// make server methods available for rpc
	rpc.RegisterName("StorageServer", storagerpc.Wrap(rawServerData))
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
	_DEBUGLOG.Println("final hash ring", rawServerData.hashRing)

	return rawServerData, nil
}

// blocks until all slaves are registered and entire hashRing is created
func initMasterServerHashRing(masterData *storageServer) error {
	_DEBUGLOG.Printf("initializing master %s...\n",
		nodeToStr(masterData.thisNode))
	defer _DEBUGLOG.Println("finished initializing master",
		nodeToStr(masterData.thisNode))

	hashRing := make([]storagerpc.Node, 0)
	// remember to add the master server to the hash ring
	hashRing = append(hashRing, *masterData.thisNode)

	for len(hashRing) < masterData.numNodes {
		select {
		case newNode := <-masterData.registrationReceiveChan:
			// make sure we don't double add the same node
			foundNodeIndex := getHashRingNodeIndex(hashRing, newNode.NodeID)
			if foundNodeIndex < 0 {
				hashRing = append(hashRing, newNode)
				_DEBUGLOG.Printf("Master received %s; updated HashRing: %v\n",
					nodeToStr(&newNode), hashRing)
			} else {
				_DEBUGLOG.Printf("Master received duplicate %s; HashRing not changed: %v\n",
					nodeToStr(&newNode), hashRing)
			}

			if len(hashRing) < masterData.numNodes {
				masterData.registrationResponseChan <- storagerpc.NotReady
			}
		}
	}

	// sort by ascending node id
	sort.Sort(sortByNodeID(hashRing))

	masterData.dataLock.Lock()
	masterData.hashRing = hashRing
	masterData.clusterIsReady = true
	masterData.dataLock.Unlock()

	masterData.registrationResponseChan <- storagerpc.OK
	// close channel so that future registers don't block
	// (ie: only send an OK response once, and let RegisterServer treat the
	//  closed channel as a signal that the server ring is already OK)
	close(masterData.registrationResponseChan)

	return nil
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
		// call the Master server's registration rpc method
		err = masterClient.Call(fmt.Sprintf("StorageServer.RegisterServer"),
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
				_DEBUGLOG.Println("received HASHRING:", hashRing)

				prevNode, nextNode, err := slaveData.findPrevNextNodes(hashRing)
				if err != nil {
					return err
				}
				slaveData.prevNode = prevNode
				slaveData.nextNode = nextNode
				slaveData.hashRing = hashRing

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
	return nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	newNode := args.ServerInfo

	// wrapped into a closure to ensure lock is released after critical section
	err, shouldReturnEarly := func() (error, bool) {
		ss.dataLock.Lock()
		defer ss.dataLock.Unlock()

		if !ss.isMaster {
			return errors.New(fmt.Sprintf("cannot register to %s; "+
				"registration not allowed on slave servers",
				nodeToStr(ss.thisNode))), true
		} else if ss.clusterIsReady {
			newNodeIndex := getHashRingNodeIndex(ss.hashRing, newNode.NodeID)
			if newNodeIndex < 0 ||
				ss.hashRing[newNodeIndex].HostPort != newNode.HostPort {
				// if trying to register something not already in the hashRing,
				// return error
				return errors.New(fmt.Sprintf("cannot register new node %s to %s; "+
					"maximum number of servers have already been registered",
					nodeToStr(&newNode), nodeToStr(ss.thisNode))), true
			} else {
				// otherwise, simply respond with OK again using the master's
				// stored hashRing without reregistering
				// the slave server that's already in the ring
				reply.Status = storagerpc.OK
				reply.Servers = ss.hashRing

				_DEBUGLOG.Printf("master is already ready, sending OK to %s with %v\n",
					nodeToStr(&newNode), reply.Servers)
				return nil, true
			}
		}
		return nil, false
	}()

	if shouldReturnEarly {
		return err
	}

	// send the registration info to the master server and wait for a response
	ss.registrationReceiveChan <- newNode
	respStatus, isOpen := <-ss.registrationResponseChan

	// if channel is closed, then it has already sent an OK signal in the past
	if (!isOpen) || respStatus == storagerpc.OK {
		finalHashRing := ss.hashRing

		_DEBUGLOG.Printf("%s's registration complete; OK with %v\n", nodeToStr(&newNode), finalHashRing)
		reply.Status = storagerpc.OK
		reply.Servers = finalHashRing
	} else {
		_DEBUGLOG.Printf("%s's registration complete; not ready\n", nodeToStr(&newNode))
		reply.Status = storagerpc.NotReady
		reply.Servers = nil
	}
	return nil
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
