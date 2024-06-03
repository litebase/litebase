package main

import (
	"litebasedb/server"
	"log"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	server.NewServer().Start(func(s *server.ServerInstance) {
		server.NewApp(s).Run()
	})
	// 	f, err := os.Create("cpu.prof")
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	pprof.StartCPUProfile(f)
	// 	defer pprof.StopCPUProfile()

	// 	wg := sync.WaitGroup{}

	// 	wg.Add(1)
	// 	go func() {
	// 		db, err := database.ConnectionManager().Get(
	// 			"eb9674f2-c64a-40c2-8889-13ac13401ccd",
	// 			"547f5365-f06e-4996-9c39-194769869d1b",
	// 		)

	// 		if err != nil {
	// 			log.Println(err)
	// 			return
	// 		}
	// 		run(db)
	// 		wg.Done()
	// 	}()

	// 	wg.Add(1)
	// 	go func() {
	// 		db, err := database.ConnectionManager().Get(
	// 			"eb9674f2-c64a-40c2-8889-13ac13401ccd",
	// 			"547f5365-f06e-4996-9c39-194769869d1b",
	// 		)

	// 		if err != nil {
	// 			log.Println(err)
	// 			return
	// 		}
	// 		run(db)
	// 		wg.Done()
	// 	}()

	// 	wg.Wait()

	// }

	// func run(db *database.ClientConnection) {
	// 	start := time.Now()

	// 	for i := 0; i < 500000; i++ {
	// 		requestQuery, _ := query.NewQuery(
	// 			db,
	// 			"accessKey.AccessKeyId",
	// 			map[string]interface{}{
	// 				"statement": "SELECT * FROM names LIMIT ?",
	// 				"parameters": []interface{}{
	// 					15,
	// 				},
	// 			},
	// 			"",
	// 		)

	// 		// results, err := requestQuery.Resolve()
	// 		_, err := requestQuery.Resolve()

	// 		if err != nil {
	// 			log.Println(err)
	// 			return
	// 		}

	// 		// log.Println(results)

	// 	}

	// 	elapsed := time.Since(start)

	// 	log.Println("Elapsed time:", elapsed)
	// 	log.Println("Queries per second:", float64(1000000)/elapsed.Seconds())

}
