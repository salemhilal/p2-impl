package tribserver

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/rpc/tribrpc"
)

type tribServer struct {
	// TODO: implement this!
}

// hashing utility function
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

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	return nil, errors.New("not implemented")
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	return errors.New("not implemented")
}
