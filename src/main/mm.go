package main

import (
	"fmt"
	"time"
)

func main() {
	fmt.Println(time.Now())
	overtime := 10 * time.Millisecond
	electionTimer := time.NewTimer(overtime)
	time.Sleep(1000)
	x := <-electionTimer.C
	fmt.Println(time.Now())
	fmt.Println(x)
}
