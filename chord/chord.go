package chord

import (
	"dht/naive"
	"fmt"
	"hash/fnv"
	"math/rand"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	FingerTableLength   = 32 // Length of the finger table
	BackUpLength        = 12 // Number of backup nodes to maintain
	ReplicaCount        = 4  // num of backup datas
	ExpireDuration      = 3 * time.Second
	StabilizeInterval   = 200 * time.Millisecond
	FixFingerInterval   = 200 * time.Millisecond
	CheckPredInterval   = 500 * time.Millisecond
	ReplicaInterval     = 1 * time.Second
	ClearExpireInterval = 1 * time.Second
)

func toID(addr string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(addr))
	return h.Sum32()
}

type DataWithTime struct {
	Value     string
	Timestamp time.Time
}

type ChordNode struct {
	*naive.MiniNode
	ID            uint32                    // Unique identifier for the node in the Chord ring
	fingerTable   [FingerTableLength]string // Routing table to maintain connections to other nodes
	successorList [BackUpLength]string      // Backup nodes for redundancy
	predecessor   string                    // Address of the predecessor node in the Chord ring

	mu sync.Mutex // protect the finger table and successor list

	data     map[string]DataWithTime
	datalock sync.RWMutex // protect data consistency

	shutdown chan struct{} // channel for stopping background task when shutting down

	randGen  *rand.Rand
	isActive bool
}

func (node *ChordNode) Init(addr string) {
	node.MiniNode.Init(addr)
	node.ID = toID(addr) // Simple hash function to generate a unique ID based on the address
	node.data = make(map[string]DataWithTime)
	node.shutdown = make(chan struct{})

	node.randGen = rand.New(rand.NewSource(time.Now().UnixNano()))
}

func (node *ChordNode) Run(wg *sync.WaitGroup) {
	node.isActive = true
	// Use a ticker for periodic tasks.
	stabilizeTicker := time.NewTicker(StabilizeInterval)
	fixFingerTicker := time.NewTicker(FixFingerInterval)
	checkPredTicker := time.NewTicker(CheckPredInterval)
	replicaTicker := time.NewTicker(ReplicaInterval)
	clearExpireTicker := time.NewTicker(ClearExpireInterval)
	// Add your data replication tickers here too.

	go func() {
		for {
			select {
			case <-stabilizeTicker.C:
				node.Stabilize()
			case <-node.shutdown:
				stabilizeTicker.Stop()
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case <-fixFingerTicker.C:
				node.FixFinger()
			case <-node.shutdown:
				fixFingerTicker.Stop()
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case <-checkPredTicker.C:
				node.CheckPredecessor()
			case <-node.shutdown:
				checkPredTicker.Stop()
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case <-replicaTicker.C:
				node.NotifyReplica()
			case <-node.shutdown:
				replicaTicker.Stop()
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case <-clearExpireTicker.C:
				node.ClearExpired()
			case <-node.shutdown:
				clearExpireTicker.Stop()
				return
			}
		}
	}()

	node.MiniNode.Run(node, wg)
}

func (node *ChordNode) GetID(_ string, reply *uint32) error {
	*reply = node.ID
	return nil
}

func (node *ChordNode) GetSuccessorList(_ string, reply *[BackUpLength]string) error {
	node.mu.Lock()
	defer node.mu.Unlock()
	*reply = node.successorList
	return nil
}

func (node *ChordNode) GetPredecessor(_ string, reply *string) error {
	node.mu.Lock()
	defer node.mu.Unlock()
	*reply = node.predecessor
	return nil
}

// findPrimarySuccessor iterates through the successor list and returns the first
// one that is reachable. It is responsible for the core ring integrity.
func (node *ChordNode) findPrimarySuccessor() (string, error) {
	node.mu.Lock()                 // Lock to safely read the successor list
	listCopy := node.successorList // Make a copy to iterate over
	node.mu.Unlock()               // Unlock before making network calls

	for i, addr := range listCopy {
		if addr != "" {
			err := node.RemoteCall(addr, "ChordNode.Ping", "", nil)
			if err == nil {
				// Found a live successor
				return addr, nil
			} else {
				// Failed to ping, lock to safely modify the list
				node.mu.Lock()
				// Check if the entry is still the same one we failed to ping
				if node.successorList[i] == addr {
					node.successorList[i] = ""
				}
				node.mu.Unlock()
			}
		}
	}
	return "", fmt.Errorf("no live successors found for node %d", node.ID)
}

// Check if the given ID is after id1 and before id2 in the Chord ring
func isBetween(id1, id2, id uint32) bool {
	if id1 < id2 {
		return id1 < id && id <= id2
	}
	return id1 < id || id <= id2
}

// FindSuccessor finds the first node that succeeds the target_id.
func (node *ChordNode) FindSuccessor(target_id uint32, reply *string) error {
	successorAddr, err := node.findPrimarySuccessor()
	if err != nil {
		// If we have no successor, we might be the only node.
		// In a single-node ring, we are our own successor.
		*reply = node.Addr
		return nil
	}

	successorID := toID(successorAddr)
	// BASE CASE: If the target is between this node and its successor,
	// then the successor is the answer.
	if isBetween(node.ID, successorID, target_id) {
		*reply = successorAddr
		return nil
	}

	// RECURSIVE STEP: Search the finger table for the best node to forward to.
	nextHop, err := node.closestFinger(target_id)
	if err != nil {
		return fmt.Errorf("fail to find closestPrecedingFinger")
	}

	// Forward the request to the chosen next hop.
	return node.RemoteCall(nextHop, "ChordNode.FindSuccessor", target_id, reply)
}

func (node *ChordNode) closestFinger(target_id uint32) (string, error) {
	// We don't lock at the top level. We'll lock only when needed.
	node.mu.Lock()
	fingerTableCopy := node.fingerTable // Make a copy to iterate safely
	node.mu.Unlock()

	for i := FingerTableLength - 1; i >= 0; i-- {
		fingerAddr := fingerTableCopy[i]
		if fingerAddr == "" {
			continue
		}
		fingerID := toID(fingerAddr)

		if isBetween(node.ID, target_id, fingerID) {
			// Ping to make sure it's alive before using it.
			err := node.RemoteCall(fingerAddr, "ChordNode.Ping", "", nil)
			if err == nil {
				return fingerAddr, nil // Found a live, valid finger.
			} else {
				// Optional: Mark as dead. This requires a lock.
				node.mu.Lock()
				if node.fingerTable[i] == fingerAddr { // Check before overwriting
					node.fingerTable[i] = ""
				}
				node.mu.Unlock()
			}
		}
	}

	// If no finger was a better shortcut, the next hop is our immediate successor.
	// We are NOT holding a lock here, so this call is safe.
	return node.findPrimarySuccessor()
}

func (node *ChordNode) Stabilize() error {
	// This part is already good
	primarySuccessor, err := node.findPrimarySuccessor()
	if err != nil {
		return err
	}

	var x_addr string
	err = node.RemoteCall(primarySuccessor, "ChordNode.GetPredecessor", "", &x_addr)
	if err == nil && x_addr != "" {
		x_id := toID(x_addr)
		successorID := toID(primarySuccessor)
		if isBetween(node.ID, successorID, x_id) {
			primarySuccessor = x_addr
		}
	}

	// Notify our (potentially new) successor that we might be its predecessor.
	node.RemoteCall(primarySuccessor, "ChordNode.Notify", node.Addr, new(string))

	// --- NEW FEATURE: Update the rest of the successor list ---
	var remoteSuccessorList [BackUpLength]string
	err = node.RemoteCall(primarySuccessor, "ChordNode.GetSuccessorList", "", &remoteSuccessorList)
	if err == nil {
		node.mu.Lock()
		node.successorList[0] = primarySuccessor
		// Copy the successor's list into our backup slots
		copy(node.successorList[1:], remoteSuccessorList[:BackUpLength-1])
		node.mu.Unlock()
	}

	return nil
}

func (node *ChordNode) Notify(potentialPredAddr string, reply *string) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	if potentialPredAddr == "" {
		return fmt.Errorf("empty predecessor address")
	}

	potentialPredID := toID(potentialPredAddr)
	if node.predecessor == "" || isBetween(toID(node.predecessor), node.ID, potentialPredID) {
		node.predecessor = potentialPredAddr
	}

	*reply = node.Addr
	return nil
}

// FixFinger randomly picks a finger table entry to refresh.
func (node *ChordNode) FixFinger() {

	i := node.randGen.Intn(FingerTableLength)
	start := (node.ID + (1 << uint(i))) // The start of the ith finger interval
	var successorAddr string
	err := node.FindSuccessor(start, &successorAddr)
	node.mu.Lock()
	if err == nil && successorAddr != "" {
		node.fingerTable[i] = successorAddr
	}
	node.mu.Unlock()
}

func (node *ChordNode) CheckPredecessor() {
	node.mu.Lock()
	pred := node.predecessor
	node.mu.Unlock()

	if pred == "" {
		return
	}

	err := node.RemoteCall(pred, "ChordNode.Ping", "", nil)
	if err != nil {
		node.mu.Lock()
		if node.predecessor == pred {
			node.predecessor = ""
		}
		node.mu.Unlock()
	}
}

func (node *ChordNode) Create() {
	node.mu.Lock()
	defer node.mu.Unlock()
	// In Create, the node starts a new ring: its successor is itself.
	node.successorList[0] = node.Addr
	node.predecessor = ""
	// Optionally clear the rest of the successor list and finger table
	for i := 1; i < BackUpLength; i++ {
		node.successorList[i] = ""
	}
	for i := 0; i < FingerTableLength; i++ {
		node.fingerTable[i] = ""
	}
}

func (node *ChordNode) Join(existingAddr string) bool {
	if existingAddr == "" || existingAddr == node.Addr {
		return false
	}
	node.mu.Lock()
	node.predecessor = ""
	node.mu.Unlock()

	var successorAddr string
	err := node.RemoteCall(existingAddr, "ChordNode.FindSuccessor", node.ID, &successorAddr)
	if err != nil || successorAddr == "" {
		return false
	}
	var dataToStore map[string]DataWithTime
	err = node.RemoteCall(successorAddr, "ChordNode.RequestDataTransfer", node.ID, &dataToStore)
	if err != nil {
		return false
	}

	node.mu.Lock()
	node.successorList[0] = successorAddr
	// Optionally clear the rest of the successor list and finger table
	for i := 1; i < BackUpLength; i++ {
		node.successorList[i] = ""
	}
	for i := 0; i < FingerTableLength; i++ {
		node.fingerTable[i] = ""
	}
	node.mu.Unlock()

	//Ask for data transfer
	if len(dataToStore) > 0 {
		node.datalock.Lock()
		for k, v := range dataToStore {
			node.data[k] = v
		}
		node.datalock.Unlock()
	}

	return true
}

func (node *ChordNode) LocalGet(key string, reply *struct {
	Found bool
	Value string
}) error {
	node.datalock.RLock()
	defer node.datalock.RUnlock()
	data, ok := node.data[key]
	if !ok {
		reply.Found = false
		reply.Value = ""
		return nil
	}
	reply.Found = true
	reply.Value = data.Value
	return nil
}

func (node *ChordNode) LocalPut(args []string, reply *bool) error {
	if len(args) != 2 {
		*reply = false
		return fmt.Errorf("invalid arguments")
	}
	key, value := args[0], args[1]
	node.datalock.Lock()
	defer node.datalock.Unlock()
	node.data[key] = DataWithTime{
		Value:     value,
		Timestamp: time.Now(),
	}
	*reply = true
	return nil
}

func (node *ChordNode) LocalDelete(key string, reply *bool) error {
	node.datalock.Lock()
	defer node.datalock.Unlock()
	_, exists := node.data[key]
	if exists {
		delete(node.data, key)
		*reply = true
	} else {
		*reply = false
	}
	return nil
}

// getSingleShot attempts to get a value once.
// Returns (found, value, nil) on a definitive outcome.
// Returns (false, "", error) on a transient/retryable failure.
func (node *ChordNode) getSingleShot(key string) (bool, string, error) {
	keyID := toID(key)
	var responsibleAddr string
	err := node.FindSuccessor(keyID, &responsibleAddr)
	if err != nil {
		return false, "", fmt.Errorf("lookup failed: %w", err) // Return an error
	}
	if responsibleAddr == node.Addr {
		var reply struct {
			Found bool
			Value string
		}
		node.LocalGet(key, &reply)
		return reply.Found, reply.Value, nil // No error
	}
	var reply struct {
		Found bool
		Value string
	}
	err = node.RemoteCall(responsibleAddr, "ChordNode.LocalGet", key, &reply)
	if err != nil {
		return false, "", fmt.Errorf("remote call failed: %w", err) // Return an error
	}
	return reply.Found, reply.Value, nil // No error
}

// putSingleShot attempts to put a key-value pair once.
// Returns (true/false for success, nil) on a definitive outcome.
// Returns (false, error) on a transient/retryable failure.
func (node *ChordNode) putSingleShot(key, value string) (bool, error) {
	keyID := toID(key)
	var responsibleAddr string
	err := node.FindSuccessor(keyID, &responsibleAddr)
	if err != nil {
		return false, fmt.Errorf("lookup failed: %w", err)
	}
	if responsibleAddr == node.Addr {
		var reply bool
		node.LocalPut([]string{key, value}, &reply)
		return reply, nil
	}
	var success bool
	err = node.RemoteCall(responsibleAddr, "ChordNode.LocalPut", []string{key, value}, &success)
	if err != nil {
		return false, fmt.Errorf("remote call failed: %w", err)
	}
	return success, nil
}

// deleteSingleShot - same pattern
func (node *ChordNode) deleteSingleShot(key string) (bool, error) {
	keyID := toID(key)
	var responsibleAddr string
	err := node.FindSuccessor(keyID, &responsibleAddr)
	if err != nil {
		return false, fmt.Errorf("lookup failed: %w", err)
	}
	if responsibleAddr == node.Addr {
		var reply bool
		node.LocalDelete(key, &reply)
		return reply, nil
	}
	var success bool
	err = node.RemoteCall(responsibleAddr, "ChordNode.LocalDelete", key, &success)
	if err != nil {
		return false, fmt.Errorf("remote call failed: %w", err)
	}
	return success, nil
}

// Policy
const (
	maxRetryCount    = 5
	initialRetryWait = 100 * time.Millisecond
	maxRetryWait     = 2 * time.Second
)

// exponentialBackoff waits for an exponentially increasing duration, capped at maxRetryWait.
func exponentialBackoff(attempt int) time.Duration {
	wait := initialRetryWait * (1 << attempt)
	if wait > maxRetryWait {
		return maxRetryWait
	}
	return wait
}
func (node *ChordNode) Put(key, value string) bool {
	for attempt := 0; attempt < maxRetryCount; attempt++ {
		success, err := node.putSingleShot(key, value)
		if err == nil { // If there was no error, the operation is complete.
			return success
		}
		// Log the retryable error and wait.
		logrus.Warnf("[%s] Put attempt %d failed, retrying: %v", node.Addr, attempt+1, err)
		time.Sleep(exponentialBackoff(attempt))
	}
	return false
}

func (node *ChordNode) Get(key string) (bool, string) {
	for attempt := 0; attempt < maxRetryCount; attempt++ {
		found, val, err := node.getSingleShot(key)
		if err == nil { // If there was no error, the operation is complete.
			return found, val
		}
		logrus.Warnf("[%s] Get attempt %d failed, retrying: %v", node.Addr, attempt+1, err)
		time.Sleep(exponentialBackoff(attempt))
	}
	return false, ""
}

// Delete follows the same pattern as Put.
func (node *ChordNode) Delete(key string) bool {
	for attempt := 0; attempt < maxRetryCount; attempt++ {
		success, err := node.deleteSingleShot(key)
		if err == nil {
			return success
		}
		logrus.Warnf("[%s] Delete attempt %d failed, retrying: %v", node.Addr, attempt+1, err)
		time.Sleep(exponentialBackoff(attempt))
	}
	return false
}

// send a copy of all source data to the successors
func (node *ChordNode) NotifyReplica() {
	node.mu.Lock()
	preID := toID(node.predecessor)
	myID := node.ID
	succs := make([]string, 0, ReplicaCount)
	for _, addr := range node.successorList {
		if addr != "" && addr != node.Addr {
			succs = append(succs, addr)
			if len(succs) >= ReplicaCount {
				break
			}
		}
	}
	node.mu.Unlock()

	primaryData := make(map[string]string)
	node.datalock.RLock()
	// First, collect only the data for which we are the primary owner.
	for key, val := range node.data {
		keyID := toID(key)
		if isBetween(preID, myID, keyID) || keyID == myID {
			primaryData[key] = val.Value
		}
	}
	node.datalock.RUnlock()

	// Now, replicate this smaller set of data.
	for _, succ := range succs {
		for key, value := range primaryData {
			// FIX: Call the specific RPC endpoint.
			go node.RemoteCall(succ, "ChordNode.LocalPut", []string{key, value}, new(bool))
		}
	}
}

// clear datas that is expired and is not sourced
func (node *ChordNode) ClearExpired() {
	now := time.Now()
	node.mu.Lock()
	preID := toID(node.predecessor)
	myID := node.ID
	node.mu.Unlock()
	node.datalock.Lock()
	for key, val := range node.data {
		keyID := toID(key)
		// If this node is not responsible for the key (not sourced)
		if !(isBetween(preID, myID, keyID) || keyID == myID) {
			if now.Sub(val.Timestamp) > ExpireDuration {
				delete(node.data, key)
			}
		}
	}
	node.datalock.Unlock()
}

func (node *ChordNode) RequestDataTransfer(newPredID uint32, reply *map[string]DataWithTime) error {
	dataToTransfer := make(map[string]DataWithTime)
	node.datalock.Lock()
	defer node.datalock.Unlock()

	for key, val := range node.data {
		keyID := toID(key)
		// If this key now belongs to the new predecessor...
		if isBetween(newPredID, node.ID, keyID) {
			// ...it doesn't belong to us anymore.
		} else if isBetween(toID(node.predecessor), newPredID, keyID) {
			// This key now belongs to the new node.
			dataToTransfer[key] = val
			// IMPORTANT: We do NOT delete it. We now hold it as a replica.
		}
	}
	*reply = dataToTransfer
	return nil
}

func (node *ChordNode) Quit() {
	if !node.isActive {
		return
	}
	node.isActive = false
	// 1. Signal all background goroutines to stop.
	// This prevents the node's state (successor, predecessor, etc.) from changing
	// while we are trying to leave.
	close(node.shutdown)
	// Give them a moment to stop
	time.Sleep(500 * time.Millisecond)

	node.mu.Lock()
	myPred := node.predecessor
	mySucc := node.successorList[0]
	myID := node.ID
	node.mu.Unlock()

	// Edge Case: If we are the only node in the ring, just shut down.
	if mySucc == node.Addr || mySucc == "" {
		logrus.Infof("Node %d is the only one in the ring, shutting down.\n", myID)
		node.MiniNode.Quit()
		return
	}

	// 2. Transfer our primary data to our successor.
	primaryData := make(map[string]DataWithTime)
	node.datalock.RLock()
	for key, val := range node.data {
		keyID := toID(key)
		// Identify data for which we are the primary owner.
		if isBetween(toID(myPred), myID, keyID) || keyID == myID {
			primaryData[key] = val
		}
	}
	node.datalock.RUnlock()

	if len(primaryData) > 0 {
		logrus.Infof("Node %d transferring %d keys to successor %s\n", myID, len(primaryData), mySucc)
		// This is a new RPC we need to create.
		err := node.RemoteCall(mySucc, "ChordNode.TakeOverData", primaryData, new(bool))
		if err != nil {
			logrus.Errorf("Error transferring data to successor: %v. Aborting quit.\n", err)
			// Re-open the channel to resume background tasks if quit fails.
			node.shutdown = make(chan struct{})
			go node.Run(new(sync.WaitGroup)) // Restart the background tasks
			return
		}
	}

	// 3. Rewire the ring.
	// Tell our successor that its new predecessor is our predecessor.
	node.RemoteCall(mySucc, "ChordNode.UpdatePredecessor", myPred, new(bool))
	// Tell our predecessor that its new successor is our successor.
	node.RemoteCall(myPred, "ChordNode.UpdateSuccessor", mySucc, new(bool))

	logrus.Infof("Node %d has gracefully left the ring.\n", myID)
	node.MiniNode.Quit()
}

// TakeOverData is called by a leaving node on its successor.
// The successor takes this data and stores it as its own.
func (node *ChordNode) TakeOverData(data map[string]DataWithTime, reply *bool) error {
	node.datalock.Lock()
	defer node.datalock.Unlock()
	for key, val := range data {
		// Store the data, updating the timestamp.
		node.data[key] = DataWithTime{Value: val.Value, Timestamp: time.Now()}
	}
	*reply = true
	return nil
}

// UpdatePredecessor is called by a leaving node on its successor.
func (node *ChordNode) UpdatePredecessor(newPredAddr string, reply *bool) error {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.predecessor = newPredAddr
	*reply = true
	return nil
}

// UpdateSuccessor is called by a leaving node on its predecessor.
func (node *ChordNode) UpdateSuccessor(newSuccAddr string, reply *bool) error {
	node.mu.Lock()
	defer node.mu.Unlock()
	// This is a simplified update. A more robust version might also
	// trigger a stabilize call to update the full successor list.
	node.successorList[0] = newSuccAddr
	*reply = true
	return nil
}

func (node *ChordNode) ForceQuit() {
	if !node.isActive {
		return
	}
	node.isActive = false
	// Stop background goroutines and shut down immediately, no remote cleanup.
	close(node.shutdown)
	node.MiniNode.ForceQuit()
}

//Debug only part

type DebugState struct {
	ID            uint32
	Addr          string
	Predecessor   string
	SuccessorList [BackUpLength]string
	// We can add the finger table later if needed.
}

// Add this new RPC method to your ChordNode.
func (node *ChordNode) GetState(_ string, reply *DebugState) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	reply.ID = node.ID
	reply.Addr = node.Addr
	reply.Predecessor = node.predecessor
	reply.SuccessorList = node.successorList

	return nil
}

// In file: chord/chord.go

// GetDebugState returns a formatted string of the node's essential state for debugging.
// This method satisfies the dhtNode interface.
func (node *ChordNode) GetDebugState() string {
	// Lock the mutex to safely read the predecessor and successor list.
	node.mu.Lock()
	defer node.mu.Unlock()

	// --- Safely get the predecessor's ID ---
	var predIDStr string
	if node.predecessor == "" {
		predIDStr = "nil"
	} else {
		predIDStr = fmt.Sprintf("%d", toID(node.predecessor))
	}

	// --- Safely get the primary successor's ID ---
	var succIDStr string
	// Find the first non-empty successor in the list for robustness.
	primarySuccessor := ""
	for _, s := range node.successorList {
		if s != "" {
			primarySuccessor = s
			break
		}
	}

	if primarySuccessor == "" {
		succIDStr = "nil"
	} else {
		succIDStr = fmt.Sprintf("%d", toID(primarySuccessor))
	}

	// --- Format the final output string ---
	// Using padding like %-10s makes the output align nicely in columns.
	return fmt.Sprintf(
		"Node %-10d | Addr: %-21s | Pred: %-10s | Succ: %-10s",
		node.ID,
		node.Addr,
		predIDStr,
		succIDStr,
	)
}
