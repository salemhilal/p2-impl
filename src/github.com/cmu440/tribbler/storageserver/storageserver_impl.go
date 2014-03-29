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

type leaseHolderInfo struct {
	key          string
	hostport     string
	expireSignal chan struct{}
}

func newLeaseHolderInfo(key string, hostport string) *leaseHolderInfo {
	return &leaseHolderInfo{
		key:          key,
		hostport:     hostport,
		expireSignal: make(chan struct{}),
	}
}

type storageServer struct {
	// use this lock whenever any part of this struct needs to be read or modified
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

	// key/value storage datamap types
	singleValueMap map[string]string
	listValueMap   map[string]([]string)

	// lease-tracking data types

	// locks to use for blocking leasing on specific keys
	keyPermissionLocks map[string](*sync.Mutex)
	// a map of keys mapped to a map of all hostports
	// that have active leases on the key. hostports are mapped to info
	// regarding their holding status
	leaseHolders map[string](map[string](*leaseHolderInfo))
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

// checks if the given key falls within this node's hash range
// should only be called if the cluster is ready
func (ss *storageServer) isKeyInHashRange(key string) bool {
	if ss.numNodes <= 1 {
		return true
	}

	prevNodeID := ss.prevNode.NodeID
	thisNodeID := ss.thisNode.NodeID
	if prevNodeID == thisNodeID {
		_DEBUGLOG.Fatalln(
			"nodes should have unique IDs, but %v was duplicated! invalid hashRing %v\n",
			thisNodeID, ss.hashRing)
	}

	keyHashID := HashKeyPrefix(key)

	if prevNodeID < thisNodeID {
		return prevNodeID < keyHashID && keyHashID <= thisNodeID
	} else {
		// account for wraparound
		return keyHashID > prevNodeID || keyHashID <= thisNodeID
	}
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
		dataLock:           new(sync.Mutex),
		masterHostport:     masterServerHostPort,
		isMaster:           (len(masterServerHostPort) == 0),
		clusterIsReady:     false,
		numNodes:           numNodes,
		thisNode:           thisNode,
		singleValueMap:     make(map[string]string),
		listValueMap:       make(map[string]([]string)),
		keyPermissionLocks: make(map[string](*sync.Mutex)),
		leaseHolders:       make(map[string](map[string](*leaseHolderInfo))),
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
	prevNode, nextNode, _ := masterData.findPrevNextNodes(hashRing)
	masterData.prevNode = prevNode
	masterData.nextNode = nextNode
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
		masterClient, err = dialRpcHostport(slaveData.masterHostport)
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
		err = masterClient.Call("StorageServer.RegisterServer",
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
	_DEBUGLOG.Println("CALL RegisterServer")
	defer _DEBUGLOG.Println("EXIT RegisterServer")

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
	_DEBUGLOG.Println("CALL GetServers")
	defer _DEBUGLOG.Println("EXIT GetServers")

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

// checks that the server is both ready and correctly allows the given key
// in its range, calling the respective given callback functions if it isn't
// NOT THREAD SAFE; lock before calling
func (ss *storageServer) validateServerKey(key string,
	onNotReady func(), onWrongServer func()) bool {

	if !ss.clusterIsReady {
		onNotReady()
		return false
	} else if !ss.isKeyInHashRange(key) {
		_DEBUGLOG.Printf("%s is wrong server for hash %v\n",
			nodeToStr(ss.thisNode), HashKeyPrefix(key))
		onWrongServer()
		return false
	}
	return true
}

// NOT THREAD SAFE, make sure to lock dataLock before calling
func (ss *storageServer) deleteCachedLease(key string, hostport string) {
	_DEBUGLOG.Println("DELETING cached lease for", key, hostport)
	defer _DEBUGLOG.Println("DELETED cached lease for", key, hostport)

	_, exists := ss.leaseHolders[key][hostport]
	if exists {
		delete(ss.leaseHolders[key], hostport)
	}
}

// does not remove the lease from the cache, make sure to call deleteCachedLease afterwards
func revokeLease(key string, hostport string, revokeSuccessSignal chan<- struct{}) (*storagerpc.RevokeLeaseReply, error) {
	_DEBUGLOG.Println("REVOKING lease for", key, hostport)
	defer _DEBUGLOG.Println("REVOKED lease for", key, hostport)

	defer func() {
		revokeSuccessSignal <- struct{}{}
	}()

	args := &storagerpc.RevokeLeaseArgs{
		Key: key,
	}
	var reply storagerpc.RevokeLeaseReply

	libStoreClient, err := dialRpcHostport(hostport)
	if err == nil {
		err = libStoreClient.Call("LeaseCallbacks.RevokeLease",
			args, &reply)
	}

	return &reply, err
}

func (ss *storageServer) setupExpireTimeout(leaseHolder *leaseHolderInfo) {
	startTime := time.Now().Unix()
	_DEBUGLOG.Println("timer started on", leaseHolder, startTime)

	select {
	// if the lease expires, mark as dead, but dont revoke it
	case <-time.After(_LEASE_EXPIRE_DURATION):
		_DEBUGLOG.Println("EXPIRE on", leaseHolder, startTime)

		// on expiration, simply expire the lease rather than revoking it
		// explicitly
		ss.dataLock.Lock()
		close(leaseHolder.expireSignal)
		ss.dataLock.Unlock()
	}
}

// Not thread safe; call this after locking the key's lock and ss.dataLock
func (ss *storageServer) cacheNewLease(key string, hostport string) {
	leaseHolder, alreadyHeld := ss.leaseHolders[key][hostport]
	if alreadyHeld {
		_DEBUGLOG.Printf("EXISTS lease for '%s'@%s; do not refresh\n", key, hostport)
	} else {
		_DEBUGLOG.Printf("INIT lease for '%s'@%s\n", key, hostport)
		leaseHolder = newLeaseHolderInfo(key, hostport)
		// add the hostport along with info regarding its holding status
		ss.leaseHolders[key][hostport] = leaseHolder

		// set up timeout check
		go ss.setupExpireTimeout(leaseHolder)
	}
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	_DEBUGLOG.Println("CALL Get", args)
	defer _DEBUGLOG.Println("EXIT Get", args)

	key, wantLease, hostport := args.Key, args.WantLease, args.HostPort

	emptyLease := storagerpc.Lease{}
	emptyValue := ""

	onNotReady := func() {
		reply.Status = storagerpc.NotReady
		reply.Value = emptyValue
		reply.Lease = emptyLease
	}
	onWrongServer := func() {
		reply.Status = storagerpc.WrongServer
		reply.Value = emptyValue
		reply.Lease = emptyLease
	}

	ss.dataLock.Lock()
	keyValidates := ss.validateServerKey(key, onNotReady, onWrongServer)
	if !keyValidates {
		ss.dataLock.Unlock()
		return nil
	}

	keyLock, lockExists := ss.keyPermissionLocks[key]
	ss.dataLock.Unlock()

	// lock the key outside of the data lock so that other modifications
	// to the key's lease status do not occur concurrently, while still allowing
	// other keys to be modified
	if lockExists {
		keyLock.Lock()
	}
	ss.dataLock.Lock()
	// actually access the key's value data
	value, keyFound := ss.singleValueMap[key]

	// initialize/refresh lease
	replyLease := emptyLease
	// only allow leases if not about to return a KeyNotFound status
	if wantLease && keyFound {
		replyLease = *makeLease()
		ss.cacheNewLease(key, hostport)
	}

	// set up the final reply
	if !keyFound {
		reply.Status = storagerpc.KeyNotFound
		reply.Value = emptyValue
		reply.Lease = emptyLease
	} else {
		reply.Status = storagerpc.OK
		reply.Value = value
		reply.Lease = replyLease
	}

	ss.dataLock.Unlock()
	if lockExists {
		keyLock.Unlock()
	}
	return nil
}

// call this AFTER locking the key's permission lock,
// but BEFORE locking ss.dataLock
func (ss *storageServer) blockUntilLeasesCleared(key string) {
	ss.dataLock.Lock()
	// store leases in this slice so we still have access after unlocking
	leasesToRevoke := make([](*leaseHolderInfo), 0)
	for hostport, leaseInfo := range ss.leaseHolders[key] {
		select {
		// don't add to revoke list if already expired
		case <-leaseInfo.expireSignal:
			// pass
		default:
			leasesToRevoke = append(leasesToRevoke, leaseInfo)
		}

		// remove the lease from the cache
		ss.deleteCachedLease(key, hostport)
	}
	// release the data lock so we are not blocking other keys
	ss.dataLock.Unlock()

	// revoke all leases in parallel
	totalToRevoke := len(leasesToRevoke)
	leaseDoneChan := make(chan struct{})

	_DEBUGLOG.Println("leases to revoke", leasesToRevoke)

	for _, lease := range leasesToRevoke {
		go func(lease *leaseHolderInfo) {
			revokeSuccessSignal := make(chan struct{}, 1)

			go revokeLease(lease.key, lease.hostport, revokeSuccessSignal)
			// wait until either the lease has been explicitly
			// revoked or the lease has expired
			select {
			case <-revokeSuccessSignal:
				// pass
			case <-lease.expireSignal:
				// pass
			}
			leaseDoneChan <- struct{}{}
		}(lease)
	}

	// block until all revocations are complete
	for step := 0; step < totalToRevoke; step++ {
		<-leaseDoneChan
	}
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	_DEBUGLOG.Println("CALL Put", args)
	defer _DEBUGLOG.Println("EXIT Put", args)

	key, value := args.Key, args.Value

	// do some validation sanity checks and return early if any fail
	err, shouldReturnEarly := func() (error, bool) {
		ss.dataLock.Lock()
		defer ss.dataLock.Unlock()

		onNotReady := func() {
			reply.Status = storagerpc.NotReady
		}
		onWrongServer := func() {
			reply.Status = storagerpc.WrongServer
		}
		if !ss.validateServerKey(key, onNotReady, onWrongServer) {
			return nil, true
		}
		_, alreadyHasList := ss.listValueMap[key]
		if alreadyHasList {
			return errors.New(fmt.Sprintf("key %s already has list, cannot be Put", key)), true
		}
		return nil, false
	}()
	if shouldReturnEarly {
		return err
	}

	// grab the permission lock on the key, if one exists
	ss.dataLock.Lock()
	keyLock, lockExists := ss.keyPermissionLocks[key]
	// initialize key permission lock info without releasing the datalock so
	// that if this block runs concurrently with itself, only one actually
	// initializes the new key lock
	if !lockExists {
		keyLock = new(sync.Mutex)
		ss.keyPermissionLocks[key] = keyLock
		ss.leaseHolders[key] = make(map[string](*leaseHolderInfo))
	}
	ss.dataLock.Unlock()

	keyLock.Lock()
	// revoke all existing leases
	if lockExists {
		_DEBUGLOG.Println("blocking in Put", args, "until leases revoked")
		ss.blockUntilLeasesCleared(key)
		_DEBUGLOG.Println("unblocking in Put", args, " from leases revoked")
	}

	ss.dataLock.Lock()
	// actually put the value in the map
	ss.singleValueMap[key] = value
	reply.Status = storagerpc.OK

	ss.dataLock.Unlock()
	keyLock.Unlock()
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	_DEBUGLOG.Println("CALL GetList", args)
	defer _DEBUGLOG.Println("EXIT GetList", args)

	key, wantLease, hostport := args.Key, args.WantLease, args.HostPort
	// default empty lease
	emptyLease := storagerpc.Lease{}
	var emptyValue []string = nil

	onNotReady := func() {
		reply.Status = storagerpc.NotReady
		reply.Value = emptyValue
		reply.Lease = emptyLease
	}
	onWrongServer := func() {
		reply.Status = storagerpc.WrongServer
		reply.Value = emptyValue
		reply.Lease = emptyLease
	}

	ss.dataLock.Lock()
	if !ss.validateServerKey(key, onNotReady, onWrongServer) {
		ss.dataLock.Unlock()
		return nil
	}
	keyLock, lockExists := ss.keyPermissionLocks[key]
	ss.dataLock.Unlock()

	// lock the key outside of the data lock
	if lockExists {
		keyLock.Lock()
	}
	ss.dataLock.Lock()
	// actually access the key's value data
	value, keyFound := ss.listValueMap[key]

	// initialize/refresh lease
	replyLease := emptyLease
	// only allow leases if not about to return a KeyNotFound status
	if wantLease && keyFound {
		replyLease = *makeLease()
		ss.cacheNewLease(key, hostport)
	}

	if keyFound {
		reply.Status = storagerpc.OK
		reply.Value = value
		reply.Lease = replyLease
	} else {
		reply.Status = storagerpc.KeyNotFound
		reply.Value = emptyValue
		reply.Lease = emptyLease
	}

	ss.dataLock.Unlock()
	if lockExists {
		keyLock.Unlock()
	}
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	_DEBUGLOG.Println("CALL AppendToList", args)
	defer _DEBUGLOG.Println("EXIT AppendToList", args)

	key, value := args.Key, args.Value

	onNotReady := func() {
		reply.Status = storagerpc.NotReady
	}
	onWrongServer := func() {
		reply.Status = storagerpc.WrongServer
	}

	err, shouldReturnEarly := func() (error, bool) {
		ss.dataLock.Lock()
		defer ss.dataLock.Unlock()

		if !ss.validateServerKey(key, onNotReady, onWrongServer) {
			return nil, true
		}

		_, alreadyHasSingleton := ss.singleValueMap[key]
		if alreadyHasSingleton {
			return errors.New(
				fmt.Sprintf("key %s already has single value, "+
					"cannot be AppendToList'd", key)), true
		}
		return nil, false
	}()
	if shouldReturnEarly {
		return err
	}

	ss.dataLock.Lock()
	keyLock, lockExists := ss.keyPermissionLocks[key]
	// initialize key permission lock info without releasing the datalock so
	// that if this block runs concurrently with itself, only one actually
	// initializes the new key lock
	if !lockExists {
		keyLock = new(sync.Mutex)
		ss.keyPermissionLocks[key] = keyLock
		ss.leaseHolders[key] = make(map[string](*leaseHolderInfo))
	}
	ss.dataLock.Unlock()

	keyLock.Lock()
	// revoke all existing leases
	if lockExists {
		_DEBUGLOG.Println("blocking in AppendToList", args, "until leases revoked")
		ss.blockUntilLeasesCleared(key)
		_DEBUGLOG.Println("unblocking in AppendToList", args, " from leases revoked")
	}

	ss.dataLock.Lock()
	oldList, hasList := ss.listValueMap[key]
	// initialize empty list if no list has been mapped yet
	if !hasList {
		oldList = make([]string, 0)
	}

	itemFound := false
	// check that the value is not already in the list
	for i := 0; i < len(oldList); i++ {
		if oldList[i] == value {
			itemFound = true
			break
		}
	}

	if itemFound {
		reply.Status = storagerpc.ItemExists
	} else {
		// update the list
		ss.listValueMap[key] = append(oldList, value)
		reply.Status = storagerpc.OK
	}
	ss.dataLock.Unlock()
	keyLock.Unlock()
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	_DEBUGLOG.Println("CALL RemoveFromList", args)
	defer _DEBUGLOG.Println("EXIT RemoveFromList", args)

	key, value := args.Key, args.Value

	onNotReady := func() {
		reply.Status = storagerpc.NotReady
	}
	onWrongServer := func() {
		reply.Status = storagerpc.WrongServer
	}

	err, shouldReturnEarly := func() (error, bool) {
		ss.dataLock.Lock()
		defer ss.dataLock.Unlock()

		if !ss.validateServerKey(key, onNotReady, onWrongServer) {
			return nil, true
		}

		_, alreadyHasSingleton := ss.singleValueMap[key]
		if alreadyHasSingleton {
			return errors.New(
				fmt.Sprintf("key %s already has single value,"+
					" cannot be RemoveFromList'd", key)), true
		}

		return nil, false
	}()
	if shouldReturnEarly {
		return err
	}

	ss.dataLock.Lock()
	keyLock, lockExists := ss.keyPermissionLocks[key]
	// initialize key permission lock info without releasing the datalock so
	// that if this block runs concurrently with itself, only one actually
	// initializes the new key lock
	if !lockExists {
		keyLock = new(sync.Mutex)
		ss.keyPermissionLocks[key] = keyLock
		ss.leaseHolders[key] = make(map[string](*leaseHolderInfo))
	}
	ss.dataLock.Unlock()

	keyLock.Lock()
	// revoke all existing leases
	if lockExists {
		_DEBUGLOG.Println("blocking in RemoveFromList", args, "until leases revoked")
		ss.blockUntilLeasesCleared(key)
		_DEBUGLOG.Println("unblocking in RemoveFromList", args, " from leases revoked")
	}

	ss.dataLock.Lock()
	oldList, hasList := ss.listValueMap[key]
	// if no list has been mapped yet, return ItemNotFound status
	if !hasList {
		reply.Status = storagerpc.ItemNotFound
	} else {
		foundIndex := -1
		// find index of item to remove
		for i := 0; i < len(oldList); i++ {
			if oldList[i] == value {
				foundIndex = i
				break
			}
		}

		if foundIndex < 0 {
			// if item was never found, return ItemNotFound
			reply.Status = storagerpc.ItemNotFound
		} else {
			// otherwise, actually remove the item and return OK status
			prefix := oldList[:foundIndex]
			suffix := oldList[foundIndex+1:]
			ss.listValueMap[key] = append(prefix, suffix...)
			reply.Status = storagerpc.OK
		}
	}

	ss.dataLock.Unlock()
	keyLock.Unlock()
	return nil
}
