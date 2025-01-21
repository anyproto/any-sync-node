package main

import (
	"context"
	"flag"
	"fmt"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-store/query"

	"github.com/anyproto/any-sync-node/nodespace/migrator"
)

var ctx = context.Background()

var flagStore = flag.String("s", "db/node0/.index/store.db", "path to storage")

func main() {
	store, err := anystore.Open(ctx, *flagStore, nil)
	if err != nil {
		panic(err)
	}
	defer store.Close()
	fmt.Println("Migration status:")
	migrated := migrator.CheckMigrated(ctx, store)
	if migrated {
		fmt.Println("- store migrated")
	} else {
		fmt.Println("- store not migrated")
	}
	fmt.Println("Space migration status count:")
	migrationColl, err := store.Collection(ctx, migrator.SpaceMigrationColl)
	if err != nil {
		panic(err)
	}
	filter := query.All{}
	iter, err := migrationColl.Find(filter).Iter(ctx)
	if err != nil {
		panic(err)
	}
	results := map[string]int{}
	for iter.Next() {
		doc, err := iter.Doc()
		if err != nil {
			panic(err)
		}
		results[doc.Value().GetString("status")]++
	}
	for status, count := range results {
		fmt.Printf("- %s: %d\n", status, count)
	}
}
