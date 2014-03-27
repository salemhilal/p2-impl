package tribserver

import (
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"net"
	"net/http"
	"net/rpc"
	"sync"
)

type tribServer struct {
	hostport   string
	myLibstore libstore.Libstore
	serverLock *sync.Mutex
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	rawServerData := &tribServer{
		hostport:   myHostPort,
		myLibstore: nil,
		serverLock: new(sync.Mutex),
	}

	// make server methods available for rpc
	rpc.RegisterName("TribServer", tribrpc.Wrap(rawServerData))
	rpc.HandleHTTP()
	listenSocket, err := net.Listen("tcp", myHostPort)
	if err != nil {
		errMsg := fmt.Sprintf("tribserver %s listen setup error: %s",
			rawServerData.hostport, err.Error())
		_DEBUGLOG.Println(errMsg)
		return nil, errors.New(errMsg)
	}

	// initialize a libstore on the same tribserver hostport
	newLibstore, err := libstore.NewLibstore(masterServerHostPort,
		rawServerData.hostport, libstore.Normal)
	if err != nil {
		_DEBUGLOG.Println("error while initializing tribserver's libstore", err)
		return nil, err
	}
	rawServerData.myLibstore = newLibstore

	// actually begin serving the server
	go http.Serve(listenSocket, nil)

	return rawServerData, nil
}

func (ts *tribServer) doesUserExist(userId string) bool {
	_, existsErr := ts.myLibstore.Get(generateUserExistsKey(userId))
	return existsErr == nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	_DEBUGLOG.Println("CALL CreateUser", args)
	defer _DEBUGLOG.Println("EXIT CreateUser", args, reply)

	ts.serverLock.Lock()
	defer ts.serverLock.Unlock()

	userId := args.UserID

	userExistsKey := generateUserExistsKey(userId)
	userAllTribsListKey := generateUserAllTribsListKey(userId)
	userRecentTribsListKey := generateUserRecentTribsListKey(userId)
	userSubsListKey := generateUserSubsKey(userId)

	// check to see if the user exists
	if !ts.doesUserExist(userId) {
		reply.Status = tribrpc.Exists
	} else {
		// initialize user data with empty string to mark as existing
		ts.myLibstore.Put(userExistsKey, "")
		// use empty list to initialize keys that require lists
		// REMEMBER TO FILTER OUT THE SINGLETON EMPTY STRINGS LATER
		ts.myLibstore.AppendToList(userAllTribsListKey, "")
		ts.myLibstore.AppendToList(userRecentTribsListKey, "")
		ts.myLibstore.AppendToList(userSubsListKey, "")
		reply.Status = tribrpc.OK
	}
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	_DEBUGLOG.Println("CALL AddSubscription", args)
	defer _DEBUGLOG.Println("EXIT AddSubscription", args, reply)

	ts.serverLock.Lock()
	defer ts.serverLock.Unlock()

	userId := args.UserID
	targetId := args.TargetUserID

	// check that user and target both exist
	if !ts.doesUserExist(userId) {
		reply.Status = tribrpc.NoSuchUser
	} else if !ts.doesUserExist(targetId) {
		reply.Status = tribrpc.NoSuchTargetUser
	} else {
		// actually add the target to the user's subs list
		userSubsListKey := generateUserSubsKey(userId)
		ts.myLibstore.AppendToList(userSubsListKey, targetId)
		reply.Status = tribrpc.OK
	}
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	_DEBUGLOG.Println("CALL RemoveSubscription", args)
	defer _DEBUGLOG.Println("EXIT RemoveSubscription", args, reply)

	ts.serverLock.Lock()
	defer ts.serverLock.Unlock()

	userId := args.UserID
	targetId := args.TargetUserID

	// check that user and target both exist
	if !ts.doesUserExist(userId) {
		reply.Status = tribrpc.NoSuchUser
	} else if !ts.doesUserExist(targetId) {
		reply.Status = tribrpc.NoSuchTargetUser
	} else {
		// actually remove the target from the user's subs list
		userSubsListKey := generateUserSubsKey(userId)
		ts.myLibstore.RemoveFromList(userSubsListKey, targetId)
		reply.Status = tribrpc.OK
	}
	return nil
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	_DEBUGLOG.Println("CALL GetSubscriptions", args)
	defer _DEBUGLOG.Println("EXIT GetSubscriptions", args, reply)

	ts.serverLock.Lock()
	defer ts.serverLock.Unlock()

	userId := args.UserID

	// check that user exists
	if !ts.doesUserExist(userId) {
		reply.Status = tribrpc.NoSuchUser
		reply.UserIDs = nil
	} else {
		userSubsListKey := generateUserSubsKey(userId)
		subsList, err := ts.myLibstore.GetList(userSubsListKey)
		if err != nil {
			reply.Status = tribrpc.NoSuchUser
			reply.UserIDs = nil
			return err
		} else {
			reply.Status = tribrpc.OK
			reply.UserIDs = filterOutEmptyStrs(subsList)
		}
	}
	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	_DEBUGLOG.Println("CALL PostTribble", args)
	defer _DEBUGLOG.Println("EXIT PostTribble", args, reply)

	ts.serverLock.Lock()
	defer ts.serverLock.Unlock()

	return errors.New("not implemented")
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	_DEBUGLOG.Println("CALL GetTribbles", args)
	defer _DEBUGLOG.Println("EXIT GetTribbles", args, reply)

	ts.serverLock.Lock()
	defer ts.serverLock.Unlock()

	return errors.New("not implemented")
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	_DEBUGLOG.Println("CALL GetTribblesBySubscription", args)
	defer _DEBUGLOG.Println("EXIT GetTribblesBySubscription", args, reply)

	ts.serverLock.Lock()
	defer ts.serverLock.Unlock()

	return errors.New("not implemented")
}
