package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func basicTest() (bool, int, int) {
	basicFailedCnt, basicTotalCnt, panicked := 0, 0, false

	defer func() {
		if r := recover(); r != nil {
			red.Println("Program panicked with", r)
		}
		panicked = true
	}()

	nodes := new([basicTestNodeSize + 1]dhtNode)
	nodeAddresses := new([basicTestNodeSize + 1]string)
	kvMap := make(map[string]string)

	/* "Run" all nodes. */
	wg = new(sync.WaitGroup)
	for i := 0; i <= basicTestNodeSize; i++ {
		nodes[i] = NewNode(firstPort + i)
		nodeAddresses[i] = portToAddr(localAddress, firstPort+i)

		wg.Add(1)
		go nodes[i].Run(wg)
	}

	wg.Wait()

	nodesInNetwork := make([]int, 0, basicTestNodeSize+1)

	time.Sleep(basicTestAfterRunSleepTime)

	/* Node 0 now creates a new network. */
	nodes[0].Create()
	nodesInNetwork = append(nodesInNetwork, 0)

	/* 5 rounds in total. */
	nextJoinNode := 1
	for t := 1; t <= basicTestRoundNum; t++ {
		cyan.Printf("Basic Test Round %d\n", t)

		/* Join. */
		joinInfo := testInfo{
			msg:       fmt.Sprintf("Join (round %d)", t),
			failedCnt: 0,
			totalCnt:  0,
		}
		cyan.Printf("Start joining (round %d)\n", t)
		for j := 1; j <= basicTestRoundJoinNodeSize; j++ {
			addr := nodeAddresses[nodesInNetwork[rand.Intn(len(nodesInNetwork))]]
			if !nodes[nextJoinNode].Join(addr) {
				joinInfo.fail()
			} else {
				joinInfo.success()
			}
			nodesInNetwork = append(nodesInNetwork, nextJoinNode)

			time.Sleep(basicTestJoinQuitSleepTime)
			nextJoinNode++
		}
		joinInfo.finish(&basicFailedCnt, &basicTotalCnt)

		printRingState(nodes[:], nodesInNetwork) // <-- ADD THIS

		time.Sleep(basicTestAfterJoinQuitSleepTime)

		/* Put, part 1. */
		put1Info := testInfo{
			msg:       fmt.Sprintf("Put (round %d, part 1)", t),
			failedCnt: 0,
			totalCnt:  0,
		}
		cyan.Printf("Start putting (round %d, part 1)\n", t)
		for i := 1; i <= basicTestRoundPutSize; i++ {
			key := randString(lengthOfKeyValue)
			value := randString(lengthOfKeyValue)
			kvMap[key] = value

			if !nodes[nodesInNetwork[rand.Intn(len(nodesInNetwork))]].Put(key, value) {
				put1Info.fail()
			} else {
				put1Info.success()
			}
		}
		put1Info.finish(&basicFailedCnt, &basicTotalCnt)
		//printRingState(nodes[:], nodesInNetwork)

		/* Get, part 1. */
		get1Info := testInfo{
			msg:       fmt.Sprintf("Get (round %d, part 1)", t),
			failedCnt: 0,
			totalCnt:  0,
		}
		cyan.Printf("Start getting (round %d, part 1)\n", t)
		get1Cnt := 0
		for key, value := range kvMap {
			ok, res := nodes[nodesInNetwork[rand.Intn(len(nodesInNetwork))]].Get(key)
			if !ok || res != value {
				get1Info.fail()
			} else {
				get1Info.success()
			}

			get1Cnt++
			if get1Cnt == basicTestRoundGetSize {
				break
			}
		}
		get1Info.finish(&basicFailedCnt, &basicTotalCnt)
		//printRingState(nodes[:], nodesInNetwork)

		/* Delete, part 1. */
		delete1Info := testInfo{
			msg:       fmt.Sprintf("Delete (round %d, part 1)", t),
			failedCnt: 0,
			totalCnt:  0,
		}
		cyan.Printf("Start deleting (round %d, part 1)\n", t)
		for i := 1; i <= basicTestRoundDeleteSize; i++ {
			for key := range kvMap {
				delete(kvMap, key)
				success := nodes[nodesInNetwork[rand.Intn(len(nodesInNetwork))]].Delete(key)
				if !success {
					delete1Info.fail()
				} else {
					delete1Info.success()
				}

				break
			}
		}
		delete1Info.finish(&basicFailedCnt, &basicTotalCnt)
		//printRingState(nodes[:], nodesInNetwork)

		/* Quit. */
		cyan.Printf("Start quitting (round %d)\n", t)
		for i := 1; i <= basicTestRoundQuitNodeSize; i++ {
			idxInArray := rand.Intn(len(nodesInNetwork))

			nodes[nodesInNetwork[idxInArray]].Quit()
			nodesInNetwork = removeFromArray(nodesInNetwork, idxInArray)

			time.Sleep(basicTestJoinQuitSleepTime)
		}
		green.Printf("Quit (round %d) passed.\n", t)
		time.Sleep(basicTestAfterJoinQuitSleepTime)

		/* Put, part 2. */
		put2Info := testInfo{
			msg:       fmt.Sprintf("Put (round %d, part 2)", t),
			failedCnt: 0,
			totalCnt:  0,
		}
		cyan.Printf("Start putting (round %d, part 2)\n", t)
		for i := 1; i <= basicTestRoundPutSize; i++ {
			key := randString(lengthOfKeyValue)
			value := randString(lengthOfKeyValue)
			kvMap[key] = value

			if !nodes[nodesInNetwork[rand.Intn(len(nodesInNetwork))]].Put(key, value) {
				put2Info.fail()
			} else {
				put2Info.success()
			}
		}
		put2Info.finish(&basicFailedCnt, &basicTotalCnt)

		/* Get, part 2. */
		get2Info := testInfo{
			msg:       fmt.Sprintf("Get (round %d, part 2)", t),
			failedCnt: 0,
			totalCnt:  0,
		}
		cyan.Printf("Start getting (round %d, part 2)\n", t)
		get2Cnt := 0
		for key, value := range kvMap {
			ok, res := nodes[nodesInNetwork[rand.Intn(len(nodesInNetwork))]].Get(key)
			if !ok || res != value {
				get2Info.fail()
			} else {
				get2Info.success()
			}

			get2Cnt++
			if get2Cnt == basicTestRoundGetSize {
				break
			}
		}
		get2Info.finish(&basicFailedCnt, &basicTotalCnt)

		/* Delete, part 2. */
		delete2Info := testInfo{
			msg:       fmt.Sprintf("Delete (round %d, part 2)", t),
			failedCnt: 0,
			totalCnt:  0,
		}
		cyan.Printf("Start deleting (round %d, part 2)\n", t)
		for i := 1; i <= basicTestRoundDeleteSize; i++ {
			for key := range kvMap {
				delete(kvMap, key)
				success := nodes[nodesInNetwork[rand.Intn(len(nodesInNetwork))]].Delete(key)
				if !success {
					delete2Info.fail()
				} else {
					delete2Info.success()
				}

				break
			}
		}
		delete2Info.finish(&basicFailedCnt, &basicTotalCnt)
	}

	/* All nodes quit. */
	for i := 0; i <= basicTestNodeSize; i++ {
		nodes[i].Quit()
	}

	return panicked, basicFailedCnt, basicTotalCnt
}

// printRingState iterates through the live nodes and prints their debug state.
func printRingState(nodes []dhtNode, nodesInNetwork []int) {
	cyan.Println("====================================== Current Ring State ======================================")
	// Sort the nodes by their index for consistent output, if desired.
	// sort.Ints(nodesInNetwork) // Optional but nice for readability

	for _, nodeIdx := range nodesInNetwork {
		// Get the node from the interface slice.
		dhtNode := nodes[nodeIdx]

		// Call the method directly on the interface!
		stateString := dhtNode.GetDebugState()

		// Print the formatted string returned by the node.
		fmt.Println(stateString)
	}
	cyan.Println("==============================================================================================")
}
