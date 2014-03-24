package storageserver

// several utility functions and constants for use in storageserver

import (
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"log"
	"os"
	"sort"
	"strings"
	"time"
)

var RPC_NAME string = "StorageServer"

var _DEBUGLOG = log.New(os.Stdout, "STORESRV: ", log.Lmicroseconds|log.Lshortfile)
var _INIT_RETRY_INTERVAL = 1 * time.Second

// Implements a sort.Interface for []storagerpc.Node based on the NodeID field, ascending.
// Modified from http://golang.org/pkg/sort/#Sort example.
// To sort a list of nodes, call sort.Sort(sortByNodeID(nodeList))
type sortByNodeID []storagerpc.Node

func (a sortByNodeID) Len() int           { return len(a) }
func (a sortByNodeID) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a sortByNodeID) Less(i, j int) bool { return a[i].NodeID < a[j].NodeID }

// converts a Node into a human readable string for debugging
func nodeToStr(node *storagerpc.Node) string {
	if node == nil {
		return "null"
	}
	return fmt.Sprintf("Node<%d@%s>", node.NodeID, node.HostPort)
}

// performs a modulo similar to Python such that negative mods still result in
// positive numbers
func posModulo(x, y int) int {
	return ((x % y) + y) % y
}

// finds the index of the given nodeID in the given hash ring
// returns -1 if the given nodeID is not one of the hash ring's points
func getHashRingNodeIndex(hashRing []storagerpc.Node, nodeID uint32) int {
	searchFn := func(i int) bool { return hashRing[i].NodeID >= nodeID }

	// use builtin binary search to search for index
	foundIndex := sort.Search(len(hashRing), searchFn)
	// if we found the node
	if foundIndex < len(hashRing) && hashRing[foundIndex].NodeID == nodeID {
		return foundIndex
	}
	return -1
}

// gets the hash value of the key, based on the substring before the first colon
// in the key, if available
func hashKeyPrefix(key string) uint32 {
	sep := ":"
	numSubstrs := 2

	prefix := strings.SplitN(key, sep, numSubstrs)[0]
	return libstore.StoreHash(prefix)
}
