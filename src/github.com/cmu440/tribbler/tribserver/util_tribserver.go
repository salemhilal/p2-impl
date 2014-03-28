package tribserver

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"log"
	"net/rpc"
	"os"
	"time"
)

var _DEBUGLOG = log.New(os.Stdout, "TRB: ", log.Lmicroseconds|log.Lshortfile)
var _MAX_RECENT_TRIBS = 100

// hashing utility function
func sha256Hash(data []byte) string {
	hash := sha256.New()
	hash.Write(data)
	return fmt.Sprintf("%x", hash.Sum(nil))
}

func md5Hash(data []byte) string {
	hash := md5.New()
	hash.Write(data)
	return fmt.Sprintf("%x", hash.Sum(nil))
}

// JSON marshaling utility function
func tribbleToJson(trib *tribrpc.Tribble) ([]byte, error) {
	tribJson, err := json.Marshal(trib)
	return tribJson, err
}

// JSON unmarshaling utility function
func jsonToTribble(jsonData []byte) (*tribrpc.Tribble, error) {
	var parsedTrib *tribrpc.Tribble
	err := json.Unmarshal(jsonData, &parsedTrib)
	return parsedTrib, err
}

// Implements a sort.Interface for []tribrpc.Tribble in reverse
// chronological order (newest first)
// Modified from http://golang.org/pkg/sort/#Sort example.
// To sort a list of nodes, call sort.Sort(sortTribNewestFirst(tribList))
type sortTribNewestFirst []tribrpc.Tribble

func (a sortTribNewestFirst) Len() int {
	return len(a)
}
func (a sortTribNewestFirst) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a sortTribNewestFirst) Less(i, j int) bool {
	return a[i].Posted.After(a[j].Posted)
}

func createNewTribbleNow(user string, contents string) *tribrpc.Tribble {
	return &tribrpc.Tribble{
		UserID:   user,
		Posted:   time.Now(),
		Contents: contents,
	}
}

func generateUserExistsKey(user string) string {
	return fmt.Sprintf("%s:exists", user)
}

// creates the key to use when retrieving a given user's subscription list
func generateUserSubsKey(user string) string {
	return fmt.Sprintf("%s:subs", user)
}

// creates the key to use when retrieving a given user's list of all tribkeys
func generateUserAllTribKeysListKey(user string) string {
	return fmt.Sprintf("%s:posts_all", user)
}

// creates the key to use when retrieving a given user's list of
// recent tribkeys
func generateUserRecentTribKeysListKey(user string) string {
	return fmt.Sprintf("%s:posts_recent", user)
}

// creates the key to use when retrieving the json of a single post from the
// user
func generateSingleTribKey(trib *tribrpc.Tribble) string {
	tribbleJson, _ := tribbleToJson(trib)
	tribbleHash := md5Hash(tribbleJson)
	return fmt.Sprintf("%s:post-%s", trib.UserID, tribbleHash)
}

func dialRpcHostport(hostport string) (*rpc.Client, error) {
	return rpc.DialHTTP("tcp", hostport)
}
