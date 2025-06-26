package kademlia

import (
	"dht/naive"
	"hash/fnv"
	"math/bits"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	digitLength          = 32
	k_Size               = 4
	trySize              = 2
	republishInterval    = 3 * time.Second
	bucketUpdateInterval = 7 * time.Second
	dataStaleAfter       = 10 * time.Second
)

func toID(addr string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(addr))
	return h.Sum32()
}

type KadeNode struct {
	*naive.MiniNode
	ID              uint32
	k_Bucket        [digitLength][k_Size]string
	BucktUpdateTime [digitLength]time.Time

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
	node.shutdown = make(chan struct{})
	node.randGen = rand.New(rand.NewSource(time.Now().UnixNano()))

	// Example of how you would start the background task
	// go node.backgroundRepublisher()
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
		// Not found, insert at front, evict if full
		if barrel[len(barrel)-1] != "" {
			//Todo: Try to ping
			//Not done since this requires release lock and recheck, which significantly increase complexity
		}
		copy(barrel[1:], barrel[:len(barrel)-1])
		barrel[0] = addr
	}
	node.BucktUpdateTime[bucketIndex] = time.Now()
}

func (node *KadeNode) NotifyUpdate(addr string, reply *struct{}) error {
	node.updateContact(addr)
	return nil
}

// wrap the original call to automatically update
func (node *KadeNode) RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	err := node.MiniNode.RemoteCall(addr, method, args, reply)

	//try update bothside
	//the re-call is only to ensure strict correctness(and not changing every RPC function)
	if err == nil {
		update_err := node.MiniNode.RemoteCall(addr, "KadeNode.NotifyUpdate", node.Addr, nil)
		if update_err == nil {
			node.updateContact(addr)
		}
	}

	return err
}

// revised by AI for comment, and fix the len(fixed_array) bug
func (node *KadeNode) AskForID(target_id uint32, reply *[k_Size]string) error {
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
		return nil
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

	return nil
}
func (node *KadeNode) NodeLookup(target_id uint32) [k_Size]string {
	type candidate struct {
		addr    string
		queried bool
	}

	var known [k_Size]candidate

	// first add result of local ask
	var reply [k_Size]string
	node.AskForID(target_id, &reply)
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
						if resultAddr != "" {
							resultMap[resultAddr] = struct{}{}
						}
					}
					resultMapMu.Unlock()
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
		result[i] = c.addr
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

	// Only update if the new entry is newer.
	if !exists || entry.Value.Timestamp.After(existing.Timestamp) {
		// Do not store tombstone if there is no existing value.
		if entry.Value.IsTomb && !exists {
			*reply = false
			return nil
		}
		node.data[entry.Key] = StoredData{
			CoreData:      entry.Value,
			LastRefreshed: time.Now(), // Mark as refreshed now.
		}
		*reply = true
	} else {
		*reply = false
	}
	return nil
}

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
	var failCnt atomic.Int32
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

			if err == nil && reply {
				succCnt.Add(1)
			} else {
				failCnt.Add(1)
			}
		}(addr)
	}
	wg.Wait()

	return succCnt.Load() > failCnt.Load()
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
		}
	}
	node.datalock.Unlock()
}

func (node *KadeNode) MaintainBucket() {
	now := time.Now()
	for i := 0; i < digitLength; i++ {
		if now.Sub(node.BucktUpdateTime[i]) > 2*bucketUpdateInterval {
			// Refresh the bucket by performing a lookup for a random ID in the bucket's range
			randomID := node.ID ^ (1 << i) ^ uint32(node.randGen.Intn(1<<i))
			node.NodeLookup(randomID)
			return
		}
	}
}

func (node *KadeNode) Run(wg *sync.WaitGroup) {
	node.isActive = true
	go func() {
		tickerData := time.NewTicker(republishInterval)
		tickerBucket := time.NewTicker(bucketUpdateInterval)
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
	node.MiniNode.Quit()
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
	state := "K-Buckets:\n"
	for i := 0; i < digitLength; i++ {
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
