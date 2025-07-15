package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	extraTestNodeSize               = 100
	extraTestRoundNum               = 10
	extraTestRoundJoinNodeSize      = 5
	extraTestRoundQuitNodeSize      = 5
	extraTestAfterRunSleepTime      = 1 * time.Second
	extraTestJoinQuitSleepTime      = 200 * time.Millisecond
	extraTestAfterJoinQuitSleepTime = 2 * time.Second
	extraTestPutSize                = 200
	extraTestGetSize                = 50
)

func extraTest() (bool, int, int) {
	yellow.Println("Start Extra Test")

	extraFailedCnt, extraTotalCnt, panicked := 0, 0, false

	defer func() {
		if r := recover(); r != nil {
			red.Println("Program panicked with", r)
		}
		panicked = true
	}()

	nodes := new([extraTestNodeSize + 1]dhtNode)
	nodeAddresses := new([extraTestNodeSize + 1]string)
	nodesInNetwork := make([]int, 0, extraTestNodeSize+1)
	kvMap := make(map[string]string)

	/* Run all nodes. */
	wg = new(sync.WaitGroup)
	for i := 0; i <= extraTestNodeSize; i++ {
		nodes[i] = NewNode(firstPort + i)
		nodeAddresses[i] = portToAddr(localAddress, firstPort+i)

		wg.Add(1)
		go nodes[i].Run(wg)
	}

	wg.Wait()
	time.Sleep(extraTestAfterRunSleepTime)

	/* Node 0 creates a new network. */
	nodes[0].Create()
	nodesInNetwork = append(nodesInNetwork, 0)

	/* Put some data. */
	putInfo := testInfo{
		msg:       "Extra test put",
		failedCnt: 0,
		totalCnt:  0,
	}
	cyan.Printf("Start putting\n")
	for i := 0; i < extraTestPutSize; i++ {
		key := randString(lengthOfKeyValue)
		value := randString(lengthOfKeyValue)
		kvMap[key] = value

		if !nodes[nodesInNetwork[rand.Intn(len(nodesInNetwork))]].Put(key, value) {
			putInfo.fail()
		} else {
			putInfo.success()
		}
	}
	putInfo.finish(&extraFailedCnt, &extraTotalCnt)

	nextJoinNode := 1
	for t := 1; t <= extraTestRoundNum; t++ {
		cyan.Printf("Extra Test Round %d\n", t)

		/* Join. */
		cyan.Printf("Start joining (round %d)\n", t)
		for j := 1; j <= extraTestRoundJoinNodeSize; j++ {
			if nextJoinNode > extraTestNodeSize {
				break
			}
			addr := nodeAddresses[nodesInNetwork[rand.Intn(len(nodesInNetwork))]]
			nodes[nextJoinNode].Join(addr)
			nodesInNetwork = append(nodesInNetwork, nextJoinNode)

			time.Sleep(extraTestJoinQuitSleepTime)
			nextJoinNode++
		}

		time.Sleep(extraTestAfterJoinQuitSleepTime)

		runCornerCaseTests(nodes, nodesInNetwork, &extraFailedCnt, &extraTotalCnt)
		runRegularGetTests(nodes, nodesInNetwork, kvMap, &extraFailedCnt, &extraTotalCnt)

		/* Quit. */
		if len(nodesInNetwork) > 1 {
			cyan.Printf("Start quitting (round %d)\n", t)
			for i := 1; i <= extraTestRoundQuitNodeSize; i++ {
				if len(nodesInNetwork) <= 1 {
					break
				}
				idxInArray := rand.Intn(len(nodesInNetwork))
				// Ensure we don't quit node 0, which is the entry point
				if nodesInNetwork[idxInArray] == 0 && len(nodesInNetwork) > 1 {
					idxInArray = (idxInArray + 1) % len(nodesInNetwork)
				}
				if nodesInNetwork[idxInArray] == 0 {
					continue
				}

				nodes[nodesInNetwork[idxInArray]].Quit()
				nodesInNetwork = removeFromArray(nodesInNetwork, idxInArray)

				time.Sleep(extraTestJoinQuitSleepTime)
			}
		}

		time.Sleep(extraTestAfterJoinQuitSleepTime)

		runCornerCaseTests(nodes, nodesInNetwork, &extraFailedCnt, &extraTotalCnt)
		runRegularGetTests(nodes, nodesInNetwork, kvMap, &extraFailedCnt, &extraTotalCnt)
	}

	/* All nodes quit. */
	for _, nodeIdx := range nodesInNetwork {
		nodes[nodeIdx].Quit()
	}

	return panicked, extraFailedCnt, extraTotalCnt
}

func runRegularGetTests(nodes *[extraTestNodeSize + 1]dhtNode, nodesInNetwork []int, kvMap map[string]string, extraFailedCnt *int, extraTotalCnt *int) {
	if len(nodesInNetwork) == 0 {
		return
	}
	getInfo := testInfo{
		msg:       "Regular Get",
		failedCnt: 0,
		totalCnt:  0,
	}
	cyan.Println("Start testing regular get")

	keys := make([]string, 0, len(kvMap))
	for k := range kvMap {
		keys = append(keys, k)
	}

	for i := 0; i < extraTestGetSize && len(keys) > 0; i++ {
		key := keys[rand.Intn(len(keys))]
		value := kvMap[key]
		ok, res := nodes[nodesInNetwork[rand.Intn(len(nodesInNetwork))]].Get(key)
		if !ok || res != value {
			getInfo.fail()
			fmt.Printf("[Get Fail] key=%q expected=%q got=%q ok=%v\n", key, value, res, ok)
		} else {
			getInfo.success()
		}
	}
	getInfo.finish(extraFailedCnt, extraTotalCnt)
}

func runCornerCaseTests(nodes *[extraTestNodeSize + 1]dhtNode, nodesInNetwork []int, extraFailedCnt *int, extraTotalCnt *int) {
	if len(nodesInNetwork) == 0 {
		return
	}
	// Test 1: Deleting a non-existent key
	deleteNonExistentInfo := testInfo{
		msg:       "Delete non-existent key",
		failedCnt: 0,
		totalCnt:  0,
	}
	cyan.Println("Start testing deleting a non-existent key")
	randomKey := randString(lengthOfKeyValue)
	if nodes[nodesInNetwork[rand.Intn(len(nodesInNetwork))]].Delete(randomKey) {
		deleteNonExistentInfo.fail()
		fmt.Printf("[Delete Non-Existent Fail] Succeeded on a key that should not exist: key=%q\n", randomKey)
	} else {
		deleteNonExistentInfo.success()
	}
	deleteNonExistentInfo.finish(extraFailedCnt, extraTotalCnt)

	// Test 2: Putting on a key with different value multiple times
	updateKeyInfo := testInfo{
		msg:       "Update existing key",
		failedCnt: 0,
		totalCnt:  0,
	}
	cyan.Println("Start testing updating a key")
	updateKey := randString(lengthOfKeyValue)
	value1 := randString(lengthOfKeyValue)
	value2 := randString(lengthOfKeyValue)

	// Put first value
	if !nodes[nodesInNetwork[rand.Intn(len(nodesInNetwork))]].Put(updateKey, value1) {
		updateKeyInfo.fail()
		fmt.Printf("[Update Fail] Put failed for key=%q, value=%q\n", updateKey, value1)
	} else {
		ok, res := nodes[nodesInNetwork[rand.Intn(len(nodesInNetwork))]].Get(updateKey)
		if !ok || res != value1 {
			updateKeyInfo.fail()
			fmt.Printf("[Update Fail] Get failed for key=%q, expected=%q, got=%q, ok=%v\n", updateKey, value1, res, ok)
		} else {
			// Put second value
			if !nodes[nodesInNetwork[rand.Intn(len(nodesInNetwork))]].Put(updateKey, value2) {
				updateKeyInfo.fail()
				fmt.Printf("[Update Fail] Put failed for key=%q, value=%q\n", updateKey, value2)
			} else {
				ok, res := nodes[nodesInNetwork[rand.Intn(len(nodesInNetwork))]].Get(updateKey)
				if !ok || res != value2 {
					updateKeyInfo.fail()
					fmt.Printf("[Update Fail] Get failed for key=%q, expected=%q, got=%q, ok=%v\n", updateKey, value2, res, ok)
				} else {
					updateKeyInfo.success()
				}
			}
		}
	}
	updateKeyInfo.finish(extraFailedCnt, extraTotalCnt)

	// Test 3: Corner cases for data
	cornerCasesInfo := testInfo{
		msg:       "Corner cases for data",
		failedCnt: 0,
		totalCnt:  0,
	}
	cyan.Println("Start testing corner cases for data")
	// Empty Key
	emptyKey := ""
	emptyKeyValue := randString(lengthOfKeyValue)
	if !nodes[nodesInNetwork[rand.Intn(len(nodesInNetwork))]].Put(emptyKey, emptyKeyValue) {
		cornerCasesInfo.fail()
	} else {
		ok, res := nodes[nodesInNetwork[rand.Intn(len(nodesInNetwork))]].Get(emptyKey)
		if !ok || res != emptyKeyValue {
			cornerCasesInfo.fail()
		} else {
			cornerCasesInfo.success()
		}
	}
	// Empty Value
	keyForEmptyValue := randString(lengthOfKeyValue)
	emptyValue := ""
	if !nodes[nodesInNetwork[rand.Intn(len(nodesInNetwork))]].Put(keyForEmptyValue, emptyValue) {
		cornerCasesInfo.fail()
	} else {
		ok, res := nodes[nodesInNetwork[rand.Intn(len(nodesInNetwork))]].Get(keyForEmptyValue)
		if !ok || res != emptyValue {
			cornerCasesInfo.fail()
		} else {
			cornerCasesInfo.success()
		}
	}
	// Empty Key and Value
	if !nodes[nodesInNetwork[rand.Intn(len(nodesInNetwork))]].Put("", "") {
		cornerCasesInfo.fail()
	} else {
		ok, res := nodes[nodesInNetwork[rand.Intn(len(nodesInNetwork))]].Get("")
		if !ok || res != "" {
			cornerCasesInfo.fail()
		} else {
			cornerCasesInfo.success()
		}
	}
	cornerCasesInfo.finish(extraFailedCnt, extraTotalCnt)
}
