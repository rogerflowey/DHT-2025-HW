package main

import (
	"flag"
	"math/rand"
	"os"
	"time"
)

var (
	help     bool
	testName string
)

func init() {
	flag.BoolVar(&help, "help", false, "help")
	flag.StringVar(&testName, "test", "", "which test(s) do you want to run: basic/advance/extra/consistency/all")

	flag.Usage = usage
	flag.Parse()

	if help || (testName != "basic" && testName != "advance" && testName != "extra" && testName != "consistency" && testName != "all") {
		flag.Usage()
		os.Exit(0)
	}

	rand.Seed(time.Now().UnixNano())
}

func main() {
	yellow.Printf("Welcome to DHT-2025 Test Program!\n\n")

	var basicFailRate float64
	var forceQuitFailRate float64
	var QASFailRate float64
	var extraFailRate float64
	var consistencyFailRate float64

	switch testName {
	case "all":
		fallthrough
	case "basic":
		yellow.Println("Basic Test Begins:")
		basicPanicked, basicFailedCnt, basicTotalCnt := basicTest()
		if basicPanicked {
			red.Printf("Basic Test Panicked.")
			os.Exit(0)
		}

		basicFailRate = float64(basicFailedCnt) / float64(basicTotalCnt)
		if basicFailRate > basicTestMaxFailRate {
			red.Printf("Basic test failed with fail rate %.4f\n\n", basicFailRate)
		} else {
			green.Printf("Basic test passed with fail rate %.4f\n\n", basicFailRate)
		}

		if testName == "basic" {
			break
		}
		time.Sleep(afterTestSleepTime)
		fallthrough
	case "advance":
		yellow.Println("Advance Test Begins:")

		/* ------ Force Quit Test Begins ------ */
		forceQuitPanicked, forceQuitFailedCnt, forceQuitTotalCnt := forceQuitTest()
		if forceQuitPanicked {
			red.Printf("Force Quit Test Panicked.")
			os.Exit(0)
		}

		forceQuitFailRate = float64(forceQuitFailedCnt) / float64(forceQuitTotalCnt)
		if forceQuitFailRate > forceQuitMaxFailRate {
			red.Printf("Force quit test failed with fail rate %.4f\n\n", forceQuitFailRate)
		} else {
			green.Printf("Force quit test passed with fail rate %.4f\n\n", forceQuitFailRate)
		}
		time.Sleep(afterTestSleepTime)
		/* ------ Force Quit Test Ends ------ */

		/* ------ Quit & Stabilize Test Begins ------ */
		QASPanicked, QASFailedCnt, QASTotalCnt := quitAndStabilizeTest()
		if QASPanicked {
			red.Printf("Quit & Stabilize Test Panicked.")
			os.Exit(0)
		}

		QASFailRate = float64(QASFailedCnt) / float64(QASTotalCnt)
		if QASFailRate > QASMaxFailRate {
			red.Printf("Quit & Stabilize test failed with fail rate %.4f\n\n", QASFailRate)
		} else {
			green.Printf("Quit & Stabilize test passed with fail rate %.4f\n\n", QASFailRate)
		}
		/* ------ Quit & Stabilize Test Ends ------ */
		if testName == "advance" {
			break
		}
		time.Sleep(afterTestSleepTime)
		fallthrough
	case "extra":
		yellow.Println("Extra Test Begins:")
		extraPanicked, extraFailedCnt, extraTotalCnt := extraTest()
		if extraPanicked {
			red.Printf("Extra Test Panicked.")
			os.Exit(0)
		}

		extraFailRate = float64(extraFailedCnt) / float64(extraTotalCnt)
		if extraFailRate > 0 {
			red.Printf("Extra test failed with fail rate %.4f\n\n", extraFailRate)
		} else {
			green.Printf("Extra test passed with fail rate %.4f\n\n", extraFailRate)
		}

		if testName == "extra" {
			break
		}
		time.Sleep(afterTestSleepTime)
		fallthrough
	case "consistency":
		yellow.Println("Consistency Test Begins:")
		consistencyPanicked, consistencyFailedCnt, consistencyTotalCnt := ConsistencyTest()
		if consistencyPanicked {
			red.Printf("Consistency Test Panicked.")
			os.Exit(0)
		}

		consistencyFailRate = float64(consistencyFailedCnt) / float64(consistencyTotalCnt)
		if consistencyFailedCnt > 0 {
			red.Printf("Consistency test failed with fail rate %.4f\n\n", consistencyFailRate)
		} else {
			green.Printf("Consistency test passed with fail rate %.4f\n\n", consistencyFailRate)
		}
		if testName == "consistency" {
			break
		}
	}

	cyan.Println("\nFinal print:")
	if basicFailRate > basicTestMaxFailRate {
		red.Printf("Basic test failed with fail rate %.4f\n", basicFailRate)
	} else {
		green.Printf("Basic test passed with fail rate %.4f\n", basicFailRate)
	}

	if testName == "advance" || testName == "all" {
		if forceQuitFailRate > forceQuitMaxFailRate {
			red.Printf("Force quit test failed with fail rate %.4f\n", forceQuitFailRate)
		} else {
			green.Printf("Force quit test passed with fail rate %.4f\n", forceQuitFailRate)
		}
		if QASFailRate > QASMaxFailRate {
			red.Printf("Quit & Stabilize test failed with fail rate %.4f\n", QASFailRate)
		} else {
			green.Printf("Quit & Stabilize test passed with fail rate %.4f\n", QASFailRate)
		}
	}

	if testName == "extra" || testName == "all" {
		if extraFailRate > 0 {
			red.Printf("Extra test failed with fail rate %.4f\n", extraFailRate)
		} else {
			green.Printf("Extra test passed with fail rate %.4f\n", extraFailRate)
		}
	}

	if testName == "consistency" || testName == "all" {
		if consistencyFailRate > 0 {
			red.Printf("Consistency test failed with fail rate %.4f\n", consistencyFailRate)
		} else {
			green.Printf("Consistency test passed with fail rate %.4f\n", consistencyFailRate)
		}
	}
}

func usage() {
	flag.PrintDefaults()
}
