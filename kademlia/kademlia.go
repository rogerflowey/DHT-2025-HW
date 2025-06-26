package kademlia

import (
	"dht/naive"
	"errors"
	"fmt"
	"hash/fnv"
	"math/bits"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	digitLength          = 32
	k_Size               = 8
	trySize              = 4
	republishInterval    = 4 * time.Second
	bucketUpdateInterval = 4 * time.Second
	dataStaleAfter       = 20 * time.Second
)

func toID(addr string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(addr))
	return h.Sum32()
}

type KadeNode struct {
	*naive.MiniNode
	ID               uint32
	k_Bucket         [digitLength][k_Size]string
	BucketUpdateTime [digitLength]time.Time

	blacklist   map[string]time.Time
	blacklistMu sync.RWMutex

	mu sync.Mutex // protect internal structure

	// The data map now stores StoredData, which separates core data from local metadata.
	data     map[string]StoredData
	datalock sync.RWMutex // protect data consistency

	shutdown chan struct{} // channel for stopping background task when shutting down

	randGen  *rand.Rand
	isActive bool
}

// CoreData represents the core key-value data that is replicated across the network.
// It is immutable from the perspective of a single node.
type CoreData struct {
	Value     string
	IsTomb    bool
	Timestamp time.Time
}

// StoredData wraps the KVPair with metadata local to the node storing it.
// This metadata is not part of the replicated data.
type StoredData struct {
	CoreData
	// LastRefreshed tracks when this data was last received via a Put or Publish.
	// Used by the background task to determine when to republish.
	LastRefreshed time.Time
}

func (node *KadeNode) Init(addr string) {
	node.MiniNode.Init(addr)
	node.ID = toID(addr)
	node.data = make(map[string]StoredData)
	node.blacklist = make(map[string]time.Time)
	node.shutdown = make(chan struct{})
	node.randGen = rand.New(rand.NewSource(time.Now().UnixNano()))

}

func (node *KadeNode) GetID(_ string, reply *uint32) error {
	*reply = node.ID
	return nil
}

// called every time successfully connect/accept a connect from addr
func (node *KadeNode) updateContact(addr string) {
	if addr == node.Addr {
		return
	}
	contactID := toID(addr)
	dis := contactID ^ node.ID
	if dis == 0 {
		return
	}
	bucketIndex := bits.Len32(dis) - 1

	node.mu.Lock()
	defer node.mu.Unlock()

	barrel := node.k_Bucket[bucketIndex][:]
	var idx = -1
	for i, v := range barrel {
		if v == addr {
			idx = i
			break
		}
	}
	if idx != -1 {
		if idx != 0 {
			addrVal := barrel[idx]
			copy(barrel[1:idx+1], barrel[0:idx])
			barrel[0] = addrVal
		}
	} else {
		copy(barrel[1:], barrel[:len(barrel)-1])
		barrel[0] = addr
	}
	node.BucketUpdateTime[bucketIndex] = time.Now()
}

// removeContact removes a dead node from the appropriate k-bucket.
func (node *KadeNode) removeContact(addr string) {
	if addr == "" {
		return
	}
	contactID := toID(addr)
	dis := contactID ^ node.ID
	if dis == 0 {
		return
	}
	bucketIndex := bits.Len32(dis) - 1

	node.mu.Lock()
	defer node.mu.Unlock()

	barrel := node.k_Bucket[bucketIndex][:]
	var idx = -1
	for i, v := range barrel {
		if v == addr {
			idx = i
			break
		}
	}

	if idx != -1 {
		// Found the dead contact. Remove it by shifting all subsequent
		// elements to the left and placing an empty string at the end.
		copy(barrel[idx:], barrel[idx+1:])
		barrel[len(barrel)-1] = ""
		logrus.Infof("[%s] Pruned dead contact %s from bucket %d\n", node.Addr, addr, bucketIndex)
	}
}

func (node *KadeNode) NotifyUpdate(addr string, reply *struct{}) error {
	if !node.isActive {
		return naive.ErrNetwork
	}
	node.updateContact(addr)
	return nil
}
func (node *KadeNode) NotifyLeave(addr string, reply *struct{}) error {
	if !node.isActive {
		return naive.ErrNetwork
	}
	logrus.Infof("[%s] Received leave notification from %s. Pruning contact.", node.Addr, addr)
	node.removeContact(addr)
	return nil
}

// wrap the original call to automatically update
func (node *KadeNode) RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	err := node.MiniNode.RemoteCall(addr, method, args, reply)

	//try update bothside
	//the re-call is only to ensure strict correctness(and not changing every RPC function)
	if err != nil {
		node.removeContact(addr)
	} else {
		go func() {
			err := node.MiniNode.RemoteCall(addr, "KadeNode.NotifyUpdate", node.Addr, nil)
			if err == nil {
				node.updateContact(addr)
			}
		}()
	}

	return err
}

// askForIDInternal returns up to k_Size closest known contacts to the target_id.
// This is the internal logic, not exposed as an RPC.
func (node *KadeNode) askForIDInternal(target_id uint32, reply *[k_Size]string) {
	dis := target_id ^ node.ID
	bucketIndex := 0
	if dis > 0 {
		bucketIndex = bits.Len32(dis) - 1
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	// --- Fast Path (sorted) ---
	// Collect non-empty contacts from the ideal bucket.
	type pair struct {
		addr string
		dis  uint32
	}
	var bucketContacts []pair
	for _, addr := range node.k_Bucket[bucketIndex] {
		if addr != "" {
			id := toID(addr)
			distance := id ^ target_id
			bucketContacts = append(bucketContacts, pair{addr, distance})
		}
	}
	// Sort the bucket contacts by distance to the target.
	sort.Slice(bucketContacts, func(i, j int) bool {
		return bucketContacts[i].dis < bucketContacts[j].dis
	})

	count := 0
	for _, p := range bucketContacts {
		reply[count] = p.addr
		count++
	}
	// If we found k contacts from the ideal bucket, we are done.
	if count == k_Size {
		return
	}

	// else, we collect all known contacts and sort
	var allContacts []pair
	// Add self as a potential candidate
	allContacts = append(allContacts, pair{node.Addr, toID(node.Addr) ^ target_id})

	// Gather all known contacts
	for i := 0; i < digitLength; i++ {
		for _, addr := range node.k_Bucket[i] {
			if addr != "" {
				id := toID(addr)
				distance := id ^ target_id
				allContacts = append(allContacts, pair{addr, distance})
			}
		}
	}

	// Sort them by distance to the target
	sort.Slice(allContacts, func(i, j int) bool {
		return allContacts[i].dis < allContacts[j].dis
	})

	// Clear the reply array before filling it again
	for i := range reply {
		reply[i] = ""
	}

	// Fill the reply with the top k
	for i := 0; i < k_Size && i < len(allContacts); i++ {
		reply[i] = allContacts[i].addr
	}
}

// AskForID is the RPC interface for returning up to k_Size closest known contacts to the target_id.
func (node *KadeNode) AskForID(target_id uint32, reply *[k_Size]string) error {
	if !node.isActive {
		return naive.ErrNetwork
	}
	node.askForIDInternal(target_id, reply)
	return nil
}

func (node *KadeNode) addToBlacklist(addr string) {
	node.blacklistMu.Lock()
	defer node.blacklistMu.Unlock()
	node.blacklist[addr] = time.Now().Add(2 * time.Second)
}

func (node *KadeNode) isBlacklisted(addr string) bool {
	node.blacklistMu.RLock()
	defer node.blacklistMu.RUnlock()
	expiryTime, ok := node.blacklist[addr]
	return ok && time.Now().Before(expiryTime)
}

func (node *KadeNode) NodeLookup(target_id uint32) [k_Size]string {
	type candidate struct {
		addr    string
		queried bool
	}

	var known [k_Size]candidate

	// first add result of local ask
	var reply [k_Size]string
	node.askForIDInternal(target_id, &reply)
	for i, addrResult := range reply {
		if addrResult != "" {
			known[i] = candidate{addrResult, false}
		}
	}

	updateKnown := func(maxConcurrentQueries int) bool {
		var wg sync.WaitGroup
		resultMap := make(map[string]struct{})
		var resultMapMu sync.Mutex

		queryCount := 0
		for i := range known {
			if queryCount >= maxConcurrentQueries || known[i].addr == "" {
				break
			}
			if known[i].queried {
				continue
			}
			if node.isBlacklisted(known[i].addr) {
				continue
			}
			known[i].queried = true
			queryCount++

			wg.Add(1)
			go func(addr string) {
				defer wg.Done()
				var remoteResult [k_Size]string
				err := node.RemoteCall(addr, "KadeNode.AskForID", target_id, &remoteResult)
				if err == nil {
					resultMapMu.Lock()
					for _, resultAddr := range remoteResult {
						if resultAddr != "" && !node.isBlacklisted(resultAddr) {
							resultMap[resultAddr] = struct{}{}
						}
					}
					resultMapMu.Unlock()
				} else {
					// Blacklist this node for this lookup
					node.addToBlacklist(addr)
				}
			}(known[i].addr)
		}
		wg.Wait()

		for _, knownNode := range known {
			if knownNode.addr != "" {
				delete(resultMap, knownNode.addr)
			}
		}

		changed := false
		for addr := range resultMap {
			if node.isBlacklisted(addr) {
				continue
			}
			id := toID(addr)
			dis := id ^ target_id

			for i := 0; i < k_Size; i++ {
				if known[i].addr == "" {
					known[i] = candidate{addr, false}
					changed = true
					break
				}
				kid := toID(known[i].addr)
				kdis := kid ^ target_id
				if dis < kdis {
					// Insert here, shift right
					copy(known[i+1:], known[i:]) // Corrected line
					known[i] = candidate{addr, false}
					changed = true
					break
				}
			}
		}
		return changed
	}

	for {
		if !updateKnown(trySize) {
			if !updateKnown(k_Size) {
				break
			}
		}
	}

	var result [k_Size]string
	for i, c := range known {
		if !node.isBlacklisted(c.addr) {
			result[i] = c.addr
		}
	}
	return result
}

func (node *KadeNode) Create() {
	//No need for operation
}

func (node *KadeNode) Join(addr string) bool {
	err := node.RemoteCall(addr, "KadeNode.Ping", "", nil)
	if err != nil {
		return false
	}
	node.updateContact(addr)
	foundNodes := node.NodeLookup(node.ID)
	for _, foundAddr := range foundNodes {
		if foundAddr != "" && foundAddr != node.Addr {
			return true
		}
	}
	return false
}

// -------------------data part------------------

// LocalGet retrieves the core KVPair from the local store.
func (node *KadeNode) LocalGet(key string, reply *struct {
	Found bool
	Value CoreData
}) error {
	node.datalock.RLock()
	defer node.datalock.RUnlock()
	data, ok := node.data[key]
	if !ok {
		reply.Found = false
		return nil
	}
	reply.Found = true
	reply.Value = data.CoreData // Return only the core data, not the local metadata
	return nil
}

// LocalPut stores a KVPair and updates its local metadata.
func (node *KadeNode) LocalPut(entry struct {
	Key   string
	Value CoreData
}, reply *bool) error {
	node.datalock.Lock()
	defer node.datalock.Unlock()
	existing, exists := node.data[entry.Key]

	// Accept the change as long as the incoming timestamp is newer or the same.
	if !exists || !entry.Value.Timestamp.Before(existing.Timestamp) {
		node.data[entry.Key] = StoredData{
			CoreData:      entry.Value,
			LastRefreshed: time.Now(),
		}
		logrus.Infof("[%s] Accepted value for key %s (tombstone=%v, ts=%v)", node.Addr, entry.Key, entry.Value.IsTomb, entry.Value.Timestamp)
		*reply = true
		return nil
	}
	logrus.Infof("[%s] Ignored value for key %s (incoming ts=%v, existing ts=%v)", node.Addr, entry.Key, entry.Value.Timestamp, existing.Timestamp)
	*reply = false
	return nil
}

//Old methods, use the new Retry version instead

/*
func (node *KadeNode) Get(key string) (bool, string) {
	candidates := node.NodeLookup(toID(key))

	var newest CoreData
	var found bool
	var resultMu sync.Mutex
	var wg sync.WaitGroup

	for _, addr := range candidates {
		if addr == "" {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			var reply struct {
				Found bool
				Value CoreData
			}
			err := node.RemoteCall(addr, "KadeNode.LocalGet", key, &reply)
			if err == nil && reply.Found {
				resultMu.Lock()
				defer resultMu.Unlock()
				if !found || reply.Value.Timestamp.After(newest.Timestamp) {
					newest = reply.Value
					found = true
				}
			}
		}(addr)
	}
	wg.Wait()

	if found && !newest.IsTomb {
		return true, newest.Value
	}
	return false, ""
}

func (node *KadeNode) Put(key string, value string) bool {
	return node.Publish(key, CoreData{
		Value:     value,
		IsTomb:    false,
		Timestamp: time.Now(),
	})
}

func (node *KadeNode) Delete(key string) bool {
	return node.Publish(key, CoreData{
		IsTomb:    true,
		Timestamp: time.Now(),
	})
}

func (node *KadeNode) Publish(key string, value CoreData) bool {
	candidates := node.NodeLookup(toID(key))

	var succCnt atomic.Int32
	var wg sync.WaitGroup
	for _, addr := range candidates {
		if addr == "" {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			var reply bool
			err := node.RemoteCall(addr, "KadeNode.LocalPut", struct {
				Key   string
				Value CoreData
			}{Key: key, Value: value}, &reply)
			if err == nil {
				if reply {
					succCnt.Add(1)
				}
			}
		}(addr)
	}
	wg.Wait()

	return succCnt.Load() >= k_Size/4
}
*/

// policy, same as chord
const (
	// --- New constants for retry logic ---
	maxRetryCount    = 2
	initialRetryWait = 100 * time.Millisecond
	maxRetryWait     = 1 * time.Second
)

// exponentialBackoff waits for an exponentially increasing duration, capped at maxRetryWait.
func exponentialBackoff(attempt int) time.Duration {
	wait := initialRetryWait * (1 << attempt)
	if wait > maxRetryWait {
		return maxRetryWait
	}
	return wait
}

func (node *KadeNode) getSingleShot(key string) (found bool, value string, isTombstone bool, err error) {
	candidates := node.NodeLookup(toID(key))

	var newest CoreData
	var hasResult bool
	var resultMu sync.Mutex
	var wg sync.WaitGroup

	for _, addr := range candidates {
		if addr == "" {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			var reply struct {
				Found bool
				Value CoreData
			}
			rpcErr := node.RemoteCall(addr, "KadeNode.LocalGet", key, &reply)
			if rpcErr == nil && reply.Found {
				resultMu.Lock()
				defer resultMu.Unlock()
				if !hasResult || reply.Value.Timestamp.After(newest.Timestamp) {
					newest = reply.Value
					hasResult = true
				}
			}
		}(addr)
	}
	wg.Wait()

	if !hasResult {
		// No node returned a value. This is a transient failure.
		return false, "", false, errors.New("no responsive nodes returned a value")
	}

	return true, newest.Value, newest.IsTomb, nil
}

// publishSingleShot attempts to publish data to the k-closest nodes once.
// It returns true with a nil error if the quorum was met.
// It returns false with a non-nil error if the quorum was not met and should be retried.
func (node *KadeNode) publishSingleShot(key string, value CoreData) (bool, error) {
	candidates := node.NodeLookup(toID(key))

	var succCnt atomic.Int32
	var totalCnt int32
	var wg sync.WaitGroup
	for _, addr := range candidates {
		if addr == "" {
			continue
		}
		totalCnt++
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			var reply bool
			err := node.RemoteCall(addr, "KadeNode.LocalPut", struct {
				Key   string
				Value CoreData
			}{Key: key, Value: value}, &reply)
			if err == nil && reply {
				succCnt.Add(1)
			}
		}(addr)
	}
	wg.Wait()

	successes := succCnt.Load()
	if successes >= k_Size/2 || (totalCnt > 0 && successes == totalCnt) {
		return true, nil
	}
	var failedCandidates []string
	for _, addr := range candidates {
		if addr != "" {
			failedCandidates = append(failedCandidates, addr)
		}
	}
	return false, errors.New(
		"publish quorum not met: total candidates=" + strconv.Itoa(int(totalCnt)) +
			", successes=" + strconv.Itoa(int(successes)) +
			", candidates=" + fmt.Sprintf("%v", failedCandidates),
	)
}

func (node *KadeNode) Get(key string) (bool, string) {
	for attempt := 0; attempt < maxRetryCount; attempt++ {
		found, val, isTomb, err := node.getSingleShot(key)
		if err == nil {
			// Definitive result: if it's a tombstone, it's not "found".
			if isTomb {
				return false, ""
			}
			return found, val
		}
		// Transient error, wait and retry.
		logrus.Warnf("[%s] Get attempt %d for key '%s' failed, retrying: %v", node.Addr, attempt+1, key, err)
		time.Sleep(exponentialBackoff(attempt))
	}
	return false, ""
}

func (node *KadeNode) Put(key string, value string) bool {
	return node.Publish(key, CoreData{
		Value:     value,
		IsTomb:    false,
		Timestamp: time.Now(),
	})
}

func (node *KadeNode) Delete(key string) bool {
	// 1. Check if the key exists and is not already a tombstone.
	// The Get method correctly handles retries and checks for tombstones.
	found, _ := node.Get(key)
	if !found {
		// Nothing to delete, or it's already a tombstone.
		// Return false to indicate no meaningful state change occurred.
		logrus.Infof("[%s] Delete for key '%s' found no existing value. Operation aborted.", node.Addr, key)
		return false
	}

	// 2. The key exists, so now we publish a tombstone.
	// The timestamp must be fresh to ensure it overwrites the existing value.
	logrus.Infof("[%s] Delete for key '%s' found existing value. Publishing tombstone.", node.Addr, key)
	return node.Publish(key, CoreData{
		IsTomb:    true,
		Timestamp: time.Now(),
	})
}

func (node *KadeNode) Publish(key string, value CoreData) bool {
	for attempt := 0; attempt < maxRetryCount; attempt++ {
		success, err := node.publishSingleShot(key, value)
		if err == nil {
			// Definitive result from the single shot.
			return success
		}
		// Transient error, wait and retry.
		logrus.Warnf("[%s] Publish attempt %d for key '%s' failed, retrying: %v", node.Addr, attempt+1, key, err)
		time.Sleep(exponentialBackoff(attempt))
	}
	logrus.Errorf("[%s] Publish for key '%s' failed after %d attempts.", node.Addr, key, maxRetryCount)
	return false
}

//--------------Background Task----------------

func (node *KadeNode) MaintainData() {
	// Copy keys and data under read lock
	node.datalock.RLock()
	dataCopy := make(map[string]StoredData, len(node.data))
	for k, v := range node.data {
		dataCopy[k] = v
	}
	node.datalock.RUnlock()

	var wg sync.WaitGroup

	// Republish in parallel
	for key, storeData := range dataCopy {
		if time.Since(storeData.LastRefreshed) > republishInterval {
			wg.Add(1)
			go func(key string, core CoreData) {
				defer wg.Done()
				node.Publish(key, core)
			}(key, storeData.CoreData)
		}
	}
	wg.Wait()

	// Remove stale data under write lock
	node.datalock.Lock()
	for key, storeData := range node.data {
		if time.Since(storeData.LastRefreshed) > dataStaleAfter {
			delete(node.data, key)
			logrus.Infof("Removed stale data for key '%s'", key)
		}
	}
	node.datalock.Unlock()
}

func (node *KadeNode) MaintainBucket() {
	now := time.Now()
	for i := 0; i < digitLength; i++ {
		if now.Sub(node.BucketUpdateTime[i]) > bucketUpdateInterval {
			// Refresh the bucket by performing a lookup for a random ID in the bucket's range
			randomID := node.ID ^ (1 << i) ^ uint32(node.randGen.Intn(1<<i))
			node.NodeLookup(randomID)
		}
	}
}

func (node *KadeNode) Run(wg *sync.WaitGroup) {
	node.isActive = true
	go func() {
		// Add a random ~500ms difference between the tickers to avoid overlap across nodes
		randDiff := time.Duration(node.randGen.Intn(500)) * time.Millisecond
		tickerData := time.NewTicker(republishInterval + randDiff)
		tickerBucket := time.NewTicker(bucketUpdateInterval + randDiff)
		defer tickerData.Stop()
		defer tickerBucket.Stop()
		for {
			select {
			case <-node.shutdown:
				node.isActive = false
				return
			case <-tickerData.C:
				node.MaintainData()
			case <-tickerBucket.C:
				node.MaintainBucket()
			}
		}
	}()

	node.MiniNode.Run(node, wg)
}

func (node *KadeNode) Quit() {
	if !node.isActive {
		return
	}

	node.isActive = false

	close(node.shutdown)

	//Maintain connectivity & strengthen connection
	logrus.Infof("[%s] introduce neighbors to each other", node.Addr)
	var neighbors [k_Size]string

	node.askForIDInternal(node.ID, &neighbors) // get close nodes
	cleanNeighbor := make([]string, 0, k_Size)
	for _, addr := range neighbors {
		if addr != "" && addr != node.Addr {
			cleanNeighbor = append(cleanNeighbor, addr)
		}
	}
	node.randGen.Shuffle(len(cleanNeighbor), func(i, j int) {
		cleanNeighbor[i], cleanNeighbor[j] = cleanNeighbor[j], cleanNeighbor[i]
	})
	if len(cleanNeighbor) > 1 {
		var wg sync.WaitGroup
		numHeirs := len(cleanNeighbor)
		for i := 0; i < numHeirs; i++ {
			//introduce one to the next to form a ring
			first := cleanNeighbor[i]
			second := cleanNeighbor[(i+1)%numHeirs]
			wg.Add(1)
			go func(target string, introducee string) {
				defer wg.Done()
				node.MiniNode.RemoteCall(target, "KadeNode.NotifyUpdate", introducee, nil)
				node.MiniNode.RemoteCall(introducee, "KadeNode.NotifyUpdate", target, nil)
			}(first, second)
		}
		wg.Wait()
	}
	logrus.Infof("[%s] Finished pairwise introductions.", node.Addr)

	// --- Start of New "Goodbye" Logic ---
	logrus.Infof("[%s] Broadcasting leave notification to all known contacts...", node.Addr)

	// Collect all unique contacts from k-buckets
	allContacts := make(map[string]struct{})
	node.mu.Lock()
	for i := 0; i < digitLength; i++ {
		for j := 0; j < k_Size; j++ {
			addr := node.k_Bucket[i][j]
			if addr != "" {
				allContacts[addr] = struct{}{}
			}
		}
	}
	node.mu.Unlock()

	// Send notifications in parallel
	var wg sync.WaitGroup
	for addr := range allContacts {
		wg.Add(1)
		go func(peerAddr string) {
			defer wg.Done()
			node.RemoteCall(peerAddr, "KadeNode.NotifyLeave", node.Addr, nil)
		}(addr)
	}
	wg.Wait() // Wait for all notifications to be sent.
	logrus.Infof("[%s] Finished broadcasting leave notifications.", node.Addr)

	// Republish all data in parallel before leaving
	var wgRepublish sync.WaitGroup
	for key, storeData := range node.data {
		wgRepublish.Add(1)
		go func(key string, core CoreData) {
			defer wgRepublish.Done()
			node.Publish(key, core)
		}(key, storeData.CoreData)
	}
	wgRepublish.Wait()

	node.MiniNode.Shutdown()
	node.MiniNode.Quit()
	logrus.Infof("Quit Finished")
}

func (node *KadeNode) ForceQuit() {
	if !node.isActive {
		return
	}
	node.isActive = false
	close(node.shutdown)
	node.MiniNode.ForceQuit()
}

func (node *KadeNode) GetDebugState() string {
	node.mu.Lock()
	defer node.mu.Unlock()
	state := fmt.Sprintf("Node: %s (ID: %d)\nK-Buckets:\n", node.Addr, node.ID)
	for i := 0; i < digitLength; i++ {
		// Check if the bucket has any non-empty entries
		nonEmpty := false
		for j := 0; j < k_Size; j++ {
			if node.k_Bucket[i][j] != "" {
				nonEmpty = true
				break
			}
		}
		if !nonEmpty {
			continue
		}
		state += "[" + strconv.Itoa(i) + "] "
		for j := 0; j < k_Size; j++ {
			if node.k_Bucket[i][j] != "" {
				state += node.k_Bucket[i][j] + " "
			}
		}
		state += "\n"
	}
	return state
}
