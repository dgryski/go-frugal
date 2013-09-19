package main

import (
	"bufio"
	"flag"
	"github.com/dgryski/go-frugal"
	"io"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"
)

func main() {

	q := flag.Float64("q", 0.5, "the quantile to estimate")
	n := flag.Int("n", 5, "number of concurrent estimators")
	m := flag.Int("m", 0, "initial estimate")
	f := flag.String("f", "", "file to read")

	rand.Seed(time.Now().UnixNano())

	flag.Parse()

	var fs []*frugal.Frugal2U

	for i := 0; i < *n; i++ {
		fs = append(fs, frugal.New(*m, float32(*q)))
	}

	ch := make(chan int)

	var r io.Reader

	if *f == "" {
		r = os.Stdin
	} else {
		var err error
		r, err = os.Open(*f)
		if err != nil {
			log.Fatal(err)
		}

	}

	go sendInts(r, ch)

	for v := range ch {
		for i := 0; i < *n; i++ {
			fs[i].Insert(int(v))
		}
	}

	// find the median of our estimates
	ints := make([]int, *n)

	for i := 0; i < *n; i++ {
		ints[i] = fs[i].Estimate()
	}

	sort.Ints(ints)

	for i := 0; i < *n; i++ {
		var s string
		if i == *n/2 {
			s = " <-- "
		}
		log.Println(ints[i], s)
	}

}

func sendInts(r io.Reader, ch chan<- int) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		b := sc.Bytes()
		v, err := strconv.ParseInt(string(b), 10, 64)
		if err != nil {
			log.Fatal(err)
		}
		ch <- int(v)
	}
	if sc.Err() != nil {
		log.Fatal(sc.Err())
	}
	close(ch)
}
