package tribserver

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"log"
	"net/rpc"
	//"os"
	"io/ioutil"
	"strings"
	"time"
)

var _DEBUGLOG = log.New(ioutil.Discard, "TRB: ", log.Lmicroseconds|log.Lshortfile)
var _MAX_RECENT_TRIBS = 100

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

// Implements a sort.Interface for []string, which represent lists of
// user:posttimestamp keys for specific tribbles, in reverse
// chronological order (newest first)
// Modified from http://golang.org/pkg/sort/#Sort example.
// To sort a list of nodes, call sort.Sort(sortTribKeyNewestFirst(tribList))
type sortTribKeyNewestFirst []string

func (a sortTribKeyNewestFirst) Len() int {
	return len(a)
}
func (a sortTribKeyNewestFirst) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a sortTribKeyNewestFirst) Less(i, j int) bool {
	nanos1 := parseKeyNanoTimestamp(a[i])
	nanos2 := parseKeyNanoTimestamp(a[j])

	time1 := time.Unix(0, nanos1)
	time2 := time.Unix(0, nanos2)
	return time1.After(time2)
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

// given a string in format of user:hexnanostamp, return the number of nano
// seconds represented by the timestamp
func parseKeyNanoTimestamp(key string) int64 {
	sep := ":"
	numSegs := 2
	timestampStr := strings.SplitN(key, sep, numSegs)[1]

	var nanoSecs int64
	fmt.Sscanf(timestampStr, "%x", &nanoSecs)
	return nanoSecs
}

// creates the key to use when retrieving the json of a single post from the
// user
// uses the format of "user:post_time_in_nanoseconds"
func generateSingleTribKey(trib *tribrpc.Tribble) string {
	return fmt.Sprintf("%s:%x", trib.UserID, trib.Posted.UnixNano())
}

func dialRpcHostport(hostport string) (*rpc.Client, error) {
	return rpc.DialHTTP("tcp", hostport)
}
