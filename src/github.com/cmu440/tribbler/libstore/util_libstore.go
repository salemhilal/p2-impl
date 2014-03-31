package libstore

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

var _DEBUGLOG = log.New(os.Stdout, "LBS-DEBUG: ", log.Lmicroseconds|log.Lshortfile)
var _ERRORLOG = log.New(os.Stdout, "LBS-ERROR: ", log.Lmicroseconds|log.Lshortfile)

// Convenience wrapper for DialHTTP
func dialRpcHostport(hostport string) (*rpc.Client, error) {
	return rpc.DialHTTP("tcp", hostport)
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

// Gets the hash value of the key, based on the substring before the first colon
// in the key, if available
func HashKeyPrefix(key string) uint32 {
	sep := ":"
	numSubstrs := 2

	prefix := strings.SplitN(key, sep, numSubstrs)[0]
	return StoreHash(prefix)
}

// converts a Node into a human readable string for debugging
func nodeToStr(node *storagerpc.Node) string {
	if node == nil {
		return "null"
	}
	return fmt.Sprintf("Node<%d@%s>", node.NodeID, node.HostPort)
}

// Given a hash ring and a key, returns the Node that the key hashes to.
// Assumes hashRing sorted in increasing order by NodeID
func getNodeForHashKey(hashRing []storagerpc.Node, key string) *storagerpc.Node {
	_DEBUGLOG.Println("Hash ring", hashRing)
	// Get hash of key prefix
	hash := HashKeyPrefix(key)
	_DEBUGLOG.Println("Hash key prefix:", hash)

	// Go over each hash element
	for _, node := range hashRing {
		_DEBUGLOG.Println("Checking node:", nodeToStr(&node))
		if node.NodeID >= hash {
			return &node
		}
	}

	// If we're here, hash is greater than all node ID's, so it
	// must belong to the first node, as it's the one "above."
	return &hashRing[0]
}
