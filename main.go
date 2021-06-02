package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/spf13/viper"
)

type System struct {
	Name string
	Host string
}

type Config struct {
	Source System
	Target System
	Worker int
}

func readConfiguration(config *Config) error {
	log.Println("Start reading configuration file")

	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	viper.SetConfigType("yml")

	if err := viper.ReadInConfig(); err != nil {
		return err
	}

	err := viper.Unmarshal(config)
	if err != nil {
		return err
	}

	log.Println("Finished reading configuraiton file")
	return nil
}

func worker(workerid int, work <-chan string, res chan string, wg *sync.WaitGroup) {
	// call done on your work group, this will decrement the counter by 1
	defer wg.Done()

	// keep fetching work from your work channel
	// this loop will terminate if the producer calls close on this channel
	for task := range work {
		log.Printf("Worker[%d] started work: %s\n", workerid, task)

		// do some expensive work, simulated with random sleep time
		rand.Seed(time.Now().UnixNano())
		n := rand.Intn(5)
		log.Printf("Work will take %d seconds...\n", n)
		time.Sleep(time.Duration(n) * time.Second)
		// expensive work done

		res <- fmt.Sprintf("Result%s", task)
		log.Printf("Worker[%d] finished work: %s\n", workerid, task)
	}
}

func publisher(work chan string) {
	// close the work channel after feeding stuff into it
	defer close(work)

	// please use whatever methode you want here
	// for demonstration purpose, we use a simple for loop
	for i := 0; i < 50; i++ {
		// publish work into work channel
		// if the capacity, in your case 10 is reached
		// this routine will wait until the workers have
		// processed some of them -> nice to keep memory relaxed
		var task string = fmt.Sprintf("Task-%d", i)
		work <- task
		log.Println("Published: ", task)

		// maybe the publisher is also slow, this will force
		// the workers into sleep mode
		// please uncomment the line below to simulate it
		// time.Sleep(time.Duration(1) * time.Second)
	}
}

func main() {
	fmt.Println("GoForrestGo is starting")

	// start reading the configuration file into config variable
	var config Config
	// check if any error occured during configuration parsing
	if err := readConfiguration(&config); err != nil {
		log.Println(err)
		log.Fatal("Failed to read configuration file properly, please check error above")
	}

	// GoForrestGo tries to finish an expensive task fast by utilizing multiple workers
	// ----------------------------------------------------------------------------------
	// Publisher: fetch work and publishes to your workers
	// Worker: waits for new work and does the expensive task
	// golang allows us to implement this nicely with channels https://tour.golang.org/concurrency/2

	// define a work channel with capacity of 10 (depends on the amount of work)
	// cosumer will write to this channel, consumer will read from it
	work := make(chan string, 10)

	// define result channel with same capacity as work
	// worker will write to it, main will read from it
	// res channel holds the result from the heavy computation done by multiple workers
	res := make(chan string, 10)

	// we will use a waitgroup to wait for all wokers finishing there work
	var wg sync.WaitGroup

	// ----------------------------------------------------------------
	// first let's fire up all the workers which will to the hard work
	// the worker will read from the work and write to res channel
	for workerId := 1; workerId <= config.Worker; workerId++ {
		// increment the work group counter by 1
		wg.Add(1)
		// start worker routine, please add additional params as needed
		go worker(workerId, work, res, &wg)
	}

	// ----------------------------------------------------------------
	// Next we will start a small function which deals with the result
	// because your res channel is buffered, the application will halt
	// after the workers have written 10 res to this channel
	// hence we need to read from it
	// in a real wold example, we could stream the result back to the
	// http client
	// we have not written a separate function, just use the short version
	go func(res chan string) {
		var idx int = 0
		for found := range res {
			log.Printf("Result[%d] -> %s\n", idx, found)
			idx++
		}
	}(res)

	// ----------------------------------------------------------------
	// all your workers listen on the work channel for new tasks
	// lets publish some
	publisher(work)

	// ----------------------------------------------------------------
	// now let's wait for all workers before hitting further in the main
	wg.Wait()
	// if all the workers are done, we can close the res channel
	// this will signal the go routine in line 129 to stop and release memory
	close(res)

	// finally we can go ahead with your logic as we like
	// for example if this code was triggered by a http request
	// the function in 129 could stream the result and now we close the request

	fmt.Println("GoForrestGo finished processing hard work via multiple wokers and single publisher")
}
