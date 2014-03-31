package libstore

import (
	"errors"
	"net/rpc"
	"time"

	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type libstore struct {
	// List of storage servers
	servers []storagerpc.Node

	// host:port of the master storage server
	masterServerHostPort string

	// host:port of this instance of the libstore
	hostPort string

	// Maps storage nodeID's to their respective clients.
	serverClients map[uint32]*rpc.Client
}

const (
	RETRY_LIMIT = 5
)

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	// Get to leases later.
	if mode != Never {
		return nil, errors.New("Mode not implemented")
	}

	// Connect to server
	masterServerClient, err := dialRpcHostport(masterServerHostPort)
	if err != nil {
		_DEBUGLOG.Println("Failed to dial storage server", err)
	}

	// Get server list. If not ready, sleep for a second, retry 5 times
	args := &storagerpc.GetServersArgs{}
	var reply storagerpc.GetServersReply
	err = masterServerClient.Call("GetServers", args, &reply)

	// Should we try and timeout (i.e. is err == nil)?
	for attempts := 1; err == nil && reply.Status == storagerpc.NotReady; attempts++ {
		// Have we timed out?
		if attempts >= RETRY_LIMIT {
			return nil, errors.New("Connection timeout calling GetServers")
		}
		time.Sleep(1 * time.Second)                              // Sleep a second
		err = masterServerClient.Call("GetServers", args, reply) // Try to call again
	}
	// At this point, we should have a list of servers.
	// Pack up the libstore
	lib := &libstore{
		servers:              reply.Servers,
		masterServerHostPort: masterServerHostPort,
		hostPort:             myHostPort,
		serverClients:        make(map[uint32]*rpc.Client),
	}

	// Register the lease callback rpc
	rpc.RegisterName("LeaseCallbacks", librpc.Wrap(lib))

	// Send 'er off.
	return lib, nil
}

//
// API IMPLEMENTATION
//

// Gets a key's singleton value from the data store, cacheing if necessary
func (ls *libstore) Get(key string) (string, error) {
	// Get the server the key belongs on.
	client, err := ls.getClientForKey(key)
	if err != nil {
		return "", err
	}

	// response and args
	var reply storagerpc.GetReply // Create reply
	args := &storagerpc.GetArgs{
		Key:       key,               // Specificy key
		WantLease: ls.wantLease(key), // Do we want to cache this key?
		HostPort:  ls.hostPort,       // Our hostport
	}

	// make the call
	if err := client.Call("Get", args, &reply); err != nil {
		return "", err
	}

	// Handle the response
	switch reply.Status {
	case storagerpc.WrongServer: // Wrong server
		_ERRORLOG.Println("tried to get data from wrong server. key: ", key)
		return "", errors.New("tried to get data from wrong server")
	case storagerpc.KeyNotFound: // Can't find the key
		return "", errors.New("key not found")
	case storagerpc.OK: // All is well
		return reply.Value, nil
	default: // Not sure what happened
		_ERRORLOG.Println("unhandled status: ", reply.Status)
		return "", errors.New("unhandled status")
	}

}

func (ls *libstore) Put(key, value string) error {
	// Get the server the key belongs on.
	client, err := ls.getClientForKey(key)
	if err != nil {
		return err
	}

	// response and args
	var reply storagerpc.PutReply
	args := &storagerpc.PutArgs{
		Key:   key,
		Value: value,
	}

	// Make the call
	if err := client.Call("Put", args, &reply); err != nil {
		return err
	}

	// Handle the response
	switch reply.Status {
	case storagerpc.WrongServer: // Wrong server
		_ERRORLOG.Println("tried to get data from wrong server. key: ", key)
		return errors.New("tried to get data from wrong server")
	case storagerpc.OK: // All is well
		return nil
	default: // Not sure what happened
		_ERRORLOG.Println("unhandled status: ", reply.Status)
		return errors.New("unhandled status")
	}
}

func (ls *libstore) GetList(key string) ([]string, error) {
	// Get the server the key belongs on.
	client, err := ls.getClientForKey(key)
	if err != nil {
		return nil, err
	}

	// response and args
	var reply storagerpc.GetListReply
	args := &storagerpc.GetArgs{
		Key:       key,
		WantLease: ls.wantLease(key),
		HostPort:  ls.hostPort,
	}

	// make the call
	if err := client.Call("GetList", args, &reply); err != nil {
		return nil, err
	}

	// handle the response
	switch reply.Status {
	case storagerpc.WrongServer:
		_ERRORLOG.Println("tried to get data from wrong server. key: ", key)
		return nil, errors.New("tried to get data from wrong server")
	case storagerpc.KeyNotFound:
		return nil, errors.New("key not found")
	case storagerpc.OK:
		return reply.Value, nil
	default:
		_ERRORLOG.Println("unhandled status: ", reply.Status)
		return nil, errors.New("unhandled status")
	}
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	// Get the server the key belongs on.
	client, err := ls.getClientForKey(key)
	if err != nil {
		return err
	}

	// response and args
	var reply storagerpc.PutReply
	args := &storagerpc.PutArgs{
		Key:   key,
		Value: removeItem,
	}

	// Make the call
	if err := client.Call("RemoveFromList", args, &reply); err != nil {
		return err
	}

	// handle the response
	switch reply.Status {
	case storagerpc.WrongServer:
		_ERRORLOG.Println("tried to remove data from wrong server. key: ", key)
		return errors.New("tried to remove data from wrong server")
	case storagerpc.ItemNotFound:
		return errors.New("key not found")
	case storagerpc.OK:
		return nil
	default:
		_ERRORLOG.Println("unhandled status: ", reply.Status)
		return errors.New("unhandled status")
	}
}

func (ls *libstore) AppendToList(key, newItem string) error {
	// Get the server the key belongs on
	client, err := ls.getClientForKey(key)
	if err != nil {
		return err
	}

	// response and args
	var reply storagerpc.PutReply
	args := storagerpc.PutArgs{
		Key:   key,
		Value: newItem,
	}

	// Make the call
	if err := client.Call("AppendToList", args, &reply); err != nil {
		return err
	}

	switch reply.Status {
	case storagerpc.WrongServer:
		_ERRORLOG.Println("tried to append data from wrong server. key: ", key)
		return errors.New("tried to appen data from wrong server")
	case storagerpc.ItemExists:
		return errors.New("item exists")
	case storagerpc.OK:
		return nil
	default:
		_ERRORLOG.Println("unhandled status: ", reply.Status)
		return errors.New("unhandled status")
	}
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	return errors.New("not implemented")
}

//
// NON-API / HELPER METHODS
//

// Given a node, get its client. Create it if it isn't already created.
// Should be called synchronously.
func (ls *libstore) getClientForNode(node *storagerpc.Node) (*rpc.Client, error) {

	if client, ok := ls.serverClients[node.NodeID]; ok == true {
		// Already have a clilent, return it
		return client, nil
	} else if client, err := dialRpcHostport(node.HostPort); err == nil {
		// Lazily created new client, save it for later.
		ls.serverClients[node.NodeID] = client
		return client, nil
	} else {
		// Something threw an error, spit that out.
		return nil, err
	}
}

// Given a key, spit out a client to the appropriate node
func (ls *libstore) getClientForKey(key string) (*rpc.Client, error) {
	node := getNodeForHashKey(ls.servers, key)
	return ls.getClientForNode(node)
}

// Determine whether or not we want a lease on a specified key
func (ls *libstore) wantLease(key string) bool {
	// TODO: Implement this
	return false
}
