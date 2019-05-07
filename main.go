// https://blog.golang.org/pipelines

package main

import (
	"bufio"
	"fmt"
	"os"
	"io"
	//"sync"
	"time"
	"encoding/csv"
)

func main() {
    csvFile, _ := os.Open("the.csv")
    reader := csv.NewReader(bufio.NewReader(csvFile))

    for {
        line, error := reader.Read()
        if error == io.EOF {
            break
        } else if error != nil {
			fmt.Println("lol")
        }
		c := function(line)
		fmt.Println(<-c)
    }

	//c := fanIn(function("foo"), function("bar"))

	//for i := 0; i < 10; i++ {
	//	fmt.Println(<-c)
	//}
	//fmt.Println("End of the road.")
}

func gen(lines ...[]string) <-chan []string {
    out := make(chan []string)
    go func() {
        for _, n := range lines {
            out <- n
        }
        close(out)
    }()
    return out
}

func function(msg []string) <-chan string { // Returns receive only channel of strings
	c := make(chan string)
	go func() { // closure
		for i := 0; ; i++ {
			c <- fmt.Sprintf("%s %d", msg, i)
			time.Sleep(2 * time.Second)
		}
	}()
	return c // return channel to caller
}

func fanIn(input1, input2 <-chan string) <-chan string {
	c := make(chan string)
	go func() {
		for {
			select {
			case s := <-input1:
				c <- s
			case s := <-input2:
				c <- s
			}
		}
	}()
	return c
}
