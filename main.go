package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	configPath := flag.String("config", "penpals.yaml", "path to config file")
	flag.Parse()

	cfg, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	accounts := make(map[string]*Account)
	for name, acctCfg := range cfg.Accounts {
		log.Printf("initializing account: %s", name)
		acct, err := NewAccount(name, acctCfg)
		if err != nil {
			log.Fatalf("account %s: %v", name, err)
		}
		accounts[name] = acct
		log.Printf("account %s: %d documents, gen=%d", name, len(acct.Tree.Docs), acct.Tree.Generation)
	}

	engine := NewSyncEngine(accounts, cfg.Shares)

	for i, share := range cfg.Shares {
		for acctName, path := range share.Paths {
			acct := accounts[acctName]
			doc, err := acct.FindDocByPath(path)
			if err != nil {
				log.Printf("share %q: %s path %q not found (will sync on creation)", share.Name, acctName, path)
				continue
			}
			cfg.Shares[i].DocIDs[acctName] = doc.DocumentID
			log.Printf("share %q: %s → %s (id=%s)", share.Name, acctName, path, doc.DocumentID)
		}
	}

	var wg sync.WaitGroup
	notifiers := make([]*Notifier, 0, len(accounts))

	for name, acct := range accounts {
		acctName := name
		n := NewNotifier(acct, cfg.PollSecs, func() {
			engine.HandleChange(acctName)
		})
		notifiers = append(notifiers, n)
		wg.Add(1)
		go func() {
			defer wg.Done()
			n.Run()
		}()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("received %v, shutting down", sig)

	for _, n := range notifiers {
		n.Stop()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("clean shutdown")
	}
}
