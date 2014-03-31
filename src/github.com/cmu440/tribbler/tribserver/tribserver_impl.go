package tribserver

import (
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"net"
	"net/http"
	"net/rpc"
	"sort"
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
		errMsg := fmt.Sprintf("tribserver %s listen setup error: %s\n",
			rawServerData.hostport, err.Error())
		_DEBUGLOG.Println(errMsg)
		return nil, errors.New(errMsg)
	}

	// initialize a libstore on the same tribserver hostport
	newLibstore, err := libstore.NewLibstore(masterServerHostPort,
		rawServerData.hostport, libstore.Normal)
	if err != nil {
		_DEBUGLOG.Println("error while initializing tribserver's libstore:", err)
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

func (ts *tribServer) initUser(userId string) {
	userExistsKey := generateUserExistsKey(userId)
	ts.myLibstore.Put(userExistsKey, "")
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	_DEBUGLOG.Println("CALL CreateUser", args)
	defer _DEBUGLOG.Println("EXIT CreateUser", args, reply)

	ts.serverLock.Lock()
	defer ts.serverLock.Unlock()

	userId := args.UserID
	// check to see if the user exists
	if ts.doesUserExist(userId) {
		reply.Status = tribrpc.Exists
	} else {
		// initialize user data with empty string to mark as existing
		ts.initUser(userId)

		// note that we don't initialize the various lists for user data, since
		// we don't have any initial data to put in them. Make sure to
		// account for possibly missing lists when writing the other tribserver
		// methods!

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
		appendError := ts.myLibstore.AppendToList(userSubsListKey, targetId)
		if appendError != nil {
			// don't allow duplicate subscriptions
			reply.Status = tribrpc.Exists
		} else {
			reply.Status = tribrpc.OK
		}
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
		removeErr := ts.myLibstore.RemoveFromList(userSubsListKey, targetId)
		// don't allow removal of a user that is not actually already in the
		// subs list (also catches case when user has no subs)
		if removeErr != nil {
			reply.Status = tribrpc.NoSuchTargetUser
		} else {
			reply.Status = tribrpc.OK
		}
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
		// if no subs list exists, but the actual user exists, this just means
		// that we haven't initialized the actual subs list for the user, so
		// return an empty list
		if err != nil {
			reply.Status = tribrpc.OK
			reply.UserIDs = make([]string, 0)
		} else {
			reply.Status = tribrpc.OK
			reply.UserIDs = subsList
		}
	}
	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	_DEBUGLOG.Println("CALL PostTribble", args)
	defer _DEBUGLOG.Println("EXIT PostTribble", args, reply)

	ts.serverLock.Lock()
	defer ts.serverLock.Unlock()

	userId, contents := args.UserID, args.Contents
	if !ts.doesUserExist(userId) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// setup keys and values to be used in storage server updates
	allTribKeylistKey := generateUserAllTribKeysListKey(userId)
	recentTribKeylistKey := generateUserRecentTribKeysListKey(userId)

	newTribble := createNewTribbleNow(userId, contents)
	newTribJsonBytes, jsonErr := tribbleToJson(newTribble)
	if jsonErr != nil {
		return jsonErr
	}
	newTribJson := string(newTribJsonBytes)
	newTribKey := generateSingleTribKey(newTribble)

	// add the new tribble post's json data as an entry on the storage server
	ts.myLibstore.Put(newTribKey, newTribJson)
	// add the new tribble post's key to the overall trib list for the user
	ts.myLibstore.AppendToList(allTribKeylistKey, newTribKey)

	// also add to the recent tribbles list for the user
	ts.myLibstore.AppendToList(recentTribKeylistKey, newTribKey)

	// remove the oldest tribbles in the recent tribs list to bring it back
	// under the maximum tribs limit
	recentTribKeyList, recentTribsErr := ts.myLibstore.GetList(recentTribKeylistKey)
	if recentTribsErr == nil {
		// since we are limited in how many rpc calls we can make here, we
		// assume that since we always append newest tribs to the end of the
		// recent list, we'll remove the front few tribbles, which will be the
		// oldest until we are back under the _MAX_RECENT_TRIBS limit
		for i := 0; i < len(recentTribKeyList)-_MAX_RECENT_TRIBS; i++ {
			// drop the tribble from the recent triblist (but not globally!)
			tribKeyToDrop := recentTribKeyList[i]
			ts.myLibstore.RemoveFromList(recentTribKeylistKey, tribKeyToDrop)
		}
	}

	// finally, remember to setup the reply
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	_DEBUGLOG.Println("CALL GetTribbles", args)
	defer _DEBUGLOG.Println("EXIT GetTribbles", args, reply)

	ts.serverLock.Lock()
	defer ts.serverLock.Unlock()

	userId := args.UserID
	if !ts.doesUserExist(userId) {
		reply.Status = tribrpc.NoSuchUser
		reply.Tribbles = nil
		return nil
	}

	recentTribKeylistKey := generateUserRecentTribKeysListKey(userId)
	recentTribKeyList, err := ts.myLibstore.GetList(recentTribKeylistKey)
	if err != nil {
		recentTribKeyList = make([]string, 0)
	}

	// retrieve the actual trib data corresponding to each key in the recent
	// key list
	recentTribList := make([]tribrpc.Tribble, 0)
	for _, tribKey := range recentTribKeyList {
		tribJson, err := ts.myLibstore.Get(tribKey)
		if err != nil {
			errMsg := fmt.Sprintf("GetTribbles error while retrieving %s: %s", tribKey, err.Error())
			_DEBUGLOG.Println(errMsg)
			return errors.New(errMsg)
		}

		rawTrib, err := jsonToTribble(([]byte)(tribJson))
		if err != nil || rawTrib == nil {
			errMsg := fmt.Sprintf("GetTribbles JSON error on key %s: %s", tribKey, err.Error())
			_DEBUGLOG.Println(errMsg)
			return errors.New(errMsg)
		}
		recentTribList = append(recentTribList, *rawTrib)
	}
	// sort the tribbles list by newest first
	sort.Sort(sortTribNewestFirst(recentTribList))

	// remove tribbles that go beyond the max limit
	if len(recentTribList) > _MAX_RECENT_TRIBS {
		for i := _MAX_RECENT_TRIBS; i < len(recentTribList); i++ {
			rawTrib := recentTribList[i]
			tribKey := generateSingleTribKey(&rawTrib)
			ts.myLibstore.RemoveFromList(recentTribKeylistKey, tribKey)
		}
		recentTribList = recentTribList[:_MAX_RECENT_TRIBS]
	}

	// finally, set up the reply params
	reply.Status = tribrpc.OK
	reply.Tribbles = recentTribList

	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	_DEBUGLOG.Println("CALL GetTribblesBySubscription", args)
	defer _DEBUGLOG.Println("EXIT GetTribblesBySubscription", args, reply)

	ts.serverLock.Lock()

	userId := args.UserID
	if !ts.doesUserExist(userId) {
		reply.Status = tribrpc.NoSuchUser
		ts.serverLock.Unlock()
		return nil
	}

	userSubsListKey := generateUserSubsKey(userId)
	userSubs, err := ts.myLibstore.GetList(userSubsListKey)
	// if the user has no subscriptions, simply return an empty list
	if err != nil {
		reply.Status = tribrpc.OK
		reply.Tribbles = make([]tribrpc.Tribble, 0)
		ts.serverLock.Unlock()
		return nil
	}

	subbedRecentTribs := make([]tribrpc.Tribble, 0)
	// unlock to allow us to use the lock-protected GetTribbles on
	//individual subbed users
	ts.serverLock.Unlock()

	// retrieve the most recent posts for each user
	for _, userSubName := range userSubs {
		args := &tribrpc.GetTribblesArgs{
			UserID: userSubName,
		}
		var reply tribrpc.GetTribblesReply
		err = ts.GetTribbles(args, &reply)
		if err != nil || reply.Status != tribrpc.OK {
			continue
		}
		// merge with posts list
		subbedRecentTribs = append(subbedRecentTribs, reply.Tribbles...)
	}

	// sort by newest first and cap to maximum
	sort.Sort(sortTribNewestFirst(subbedRecentTribs))
	if len(subbedRecentTribs) > _MAX_RECENT_TRIBS {
		subbedRecentTribs = subbedRecentTribs[:_MAX_RECENT_TRIBS]
	}

	reply.Status = tribrpc.OK
	reply.Tribbles = subbedRecentTribs
	return nil
}
