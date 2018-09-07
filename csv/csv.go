package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

func main() {

	numRecords, _ := strconv.Atoi(os.Args[1])
	numFiles, _ := strconv.Atoi(os.Args[2])

	var wg sync.WaitGroup

	for i := 0; i < numFiles; i++ {
		fmt.Println("Spinning off new goroutine...")
		wg.Add(1)
		go csvWriter(strconv.Itoa(i), numRecords, &wg)
	}

	wg.Wait()
	fmt.Println("All goroutines are done!")
}

func csvWriter(fileNum string, numRecords int, wg *sync.WaitGroup) {
	newFile, _ := os.Create(fileNum + "_out.csv")
	writer := csv.NewWriter(newFile)
	colPrefixeDef := "abcdefghijklmnopqrstuvwxyzABCDEFG"
	colPrefixes := strings.Split(colPrefixeDef, "")
	numCols := len(colPrefixes)

	writer.Write(colPrefixes)

	content := make([]string, 0)

	for j := 0; j < numCols; j++ {
		content = append(content, strconv.Itoa(j))
	}

	//fmt.Println(content)

	for i := 0; i < numRecords; i++ {
		writer.Write(content)
	}

	writer.Flush()
	wg.Done()
}
