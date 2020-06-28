package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
)

func get(count int) {
	response, err := http.Get(fmt.Sprintf("http://localhost:3001/Patient?_count=%d", count))
	if err != nil {
		fmt.Printf("error: %s\n", err)
	} else {
		defer response.Body.Close()
		contents, err := ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Printf("error reading: %s\n", err)
		}
		// fmt.Printf("received %d bytes: %s\n", len(contents), string(contents))
		// fmt.Printf("received %d bytes\n", len(contents))
		if len(contents) != 295 {
			fmt.Printf("received %d bytes: %s\n", len(contents), string(contents))
		}
	}
}

func post(inputFilePath string) {

	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		panic(err)
	}

	response, err := http.Post("http://localhost:3001/", "application/fhir+json", inputFile)
	if err != nil {
		fmt.Printf("error: %s\n", err)
	} else {
		defer response.Body.Close()
		contents, err := ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Printf("error reading: %s\n", err)
		}
		// fmt.Printf("received %d bytes: %s\n", len(contents), string(contents))
		// fmt.Printf("received %d bytes\n", len(contents))
		// if len(contents) != 295 {
		// fmt.Printf("%d: received %d bytes: %s\n", response.StatusCode, len(contents), string(contents))
		fmt.Printf("%d: received %d bytes\n", response.StatusCode, len(contents))
		// }
	}
}

func main() {
	fmt.Println("concurrent test")

	var wg sync.WaitGroup

	for round := 0; round < 10; round++ {
		for i := 100; i < 115; i++ {
			wg.Add(1)

			go func(count int) {
				defer wg.Done()
				// get(count)
				post("../../fixtures/synthea_bundle.json")
			}(i)
		}
		wg.Wait()
	}

}
