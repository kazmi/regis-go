package main

import (
	"flag"
	"log"
)

func main() {

	dirPtr := flag.String("dir", "", "the path to the directory where the RDB file is stored")
	dbfilenamePtr := flag.String("dbfilename", "", "the name of the RDB file")
	flag.Parse()

	config := Configuration{Directory: *dirPtr, DbFileName: *dbfilenamePtr}

	server := NewRedis("0.0.0.0", "6379", config)
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
