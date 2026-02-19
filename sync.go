package main

import (
	"log"

	"github.com/juruen/rmapi/api/sync15"
	"github.com/juruen/rmapi/archive"
	"github.com/juruen/rmapi/transport"
)

type SyncEngine struct {
	accounts map[string]*Account
	shares   []ShareConfig
}

func NewSyncEngine(accounts map[string]*Account, shares []ShareConfig) *SyncEngine {
	return &SyncEngine{
		accounts: accounts,
		shares:   shares,
	}
}

func (e *SyncEngine) HandleChange(changedAccount string) {
	src, ok := e.accounts[changedAccount]
	if !ok {
		log.Printf("unknown account: %s", changedAccount)
		return
	}

	if err := src.RefreshTree(); err != nil {
		if err == transport.ErrUnauthorized {
			log.Printf("[%s] token expired, renewing", src.Name)
			if rerr := src.RenewToken(); rerr != nil {
				log.Printf("[%s] token renewal failed: %v", src.Name, rerr)
				return
			}
			if err = src.RefreshTree(); err != nil {
				log.Printf("[%s] refresh after renewal failed: %v", src.Name, err)
				return
			}
		} else {
			log.Printf("[%s] refresh failed: %v", src.Name, err)
			return
		}
	}

	for i := range e.shares {
		share := &e.shares[i]
		e.syncShare(changedAccount, share)
	}
}

func (e *SyncEngine) syncShare(changedAccount string, share *ShareConfig) {
	srcPath, ok := share.Paths[changedAccount]
	if !ok {
		return
	}
	src := e.accounts[changedAccount]

	srcDoc, err := e.resolveDoc(src, changedAccount, share, srcPath)
	if err != nil {
		log.Printf("[%s] share %q: source doc not found at %q: %v", changedAccount, share.Name, srcPath, err)
		return
	}

	for dstName, dstPath := range share.Paths {
		if dstName == changedAccount {
			continue
		}
		dst, ok := e.accounts[dstName]
		if !ok {
			continue
		}
		e.syncPair(src, dst, srcDoc, dstName, share, dstPath)
	}
}

func (e *SyncEngine) resolveDoc(acct *Account, acctName string, share *ShareConfig, path string) (*sync15.BlobDoc, error) {
	if id, ok := share.DocIDs[acctName]; ok && id != "" {
		doc, err := acct.FindDocByID(id)
		if err == nil {
			return doc, nil
		}
	}
	doc, err := acct.FindDocByPath(path)
	if err != nil {
		return nil, err
	}
	share.DocIDs[acctName] = doc.DocumentID
	return doc, nil
}

func (e *SyncEngine) syncPair(src, dst *Account, srcDoc *sync15.BlobDoc, dstName string, share *ShareConfig, dstPath string) {
	if err := dst.RefreshTree(); err != nil {
		log.Printf("[%s] refresh failed: %v", dstName, err)
		return
	}

	dstDoc, err := e.resolveDoc(dst, dstName, share, dstPath)
	if err != nil {
		log.Printf("[%s] share %q: dest doc not found, copying entire document", dstName, share.Name)
		e.copyFullDoc(src, dst, srcDoc, dstName, share, dstPath)
		return
	}

	if dstDoc.DocumentID != srcDoc.DocumentID {
		log.Printf("[%s] share %q: UUID mismatch (src=%s dst=%s), replacing with source copy",
			dstName, share.Name, srcDoc.DocumentID, dstDoc.DocumentID)
		e.replaceWithSourceDoc(src, dst, srcDoc, dstDoc, dstName, share)
		return
	}

	if srcDoc.Hash == dstDoc.Hash {
		log.Printf("[%s→%s] share %q: already in sync", src.Name, dstName, share.Name)
		return
	}

	changed, err := copyChangedBlobs(dst, src, srcDoc, dstDoc)
	if err != nil {
		log.Printf("[%s→%s] share %q: copy blobs failed: %v", src.Name, dstName, share.Name, err)
		return
	}

	err = dst.SyncWithOperation(func(t *sync15.HashTree) error {
		doc, err := t.FindDoc(dstDoc.DocumentID)
		if err != nil {
			return err
		}
		doc.Files = make([]*sync15.Entry, len(srcDoc.Files))
		for i, f := range srcDoc.Files {
			eCopy := *f
			doc.Files[i] = &eCopy
		}
		doc.Metadata = srcDoc.Metadata
		doc.Metadata.Parent = dstDoc.Metadata.Parent
		doc.Metadata.DocName = dstDoc.Metadata.DocName

		if err := doc.Rehash(); err != nil {
			return err
		}

		indexReader, err := doc.IndexReader()
		if err != nil {
			return err
		}
		if err := dst.Storage.UploadBlob(doc.Hash, addExt(doc.DocumentID, archive.DocSchemaExt), indexReader); err != nil {
			return err
		}

		return t.Rehash()
	}, true)

	if err != nil {
		log.Printf("[%s→%s] share %q: sync failed: %v", src.Name, dstName, share.Name, err)
		return
	}

	log.Printf("[%s→%s] share %q: synced %d changed blobs", src.Name, dstName, share.Name, changed)
}

func (e *SyncEngine) replaceWithSourceDoc(src, dst *Account, srcDoc, dstDoc *sync15.BlobDoc, dstName string, share *ShareConfig) {
	dstParent := dstDoc.Metadata.Parent
	dstName_ := dstDoc.Metadata.DocName

	if err := dst.SyncWithOperation(func(t *sync15.HashTree) error {
		return t.Remove(dstDoc.DocumentID)
	}, false); err != nil {
		log.Printf("[%s] share %q: failed to remove old doc: %v", dstName, share.Name, err)
		return
	}

	if err := copyDocBlobs(dst, src, srcDoc); err != nil {
		log.Printf("[%s] share %q: copy blobs failed: %v", dstName, share.Name, err)
		return
	}

	newDoc := cloneBlobDoc(srcDoc)
	newDoc.Metadata.Parent = dstParent
	newDoc.Metadata.DocName = dstName_

	metaHash, metaReader, err := newDoc.MetadataHashAndReader()
	if err != nil {
		log.Printf("[%s] share %q: metadata hash failed: %v", dstName, share.Name, err)
		return
	}
	if err := dst.Storage.UploadBlob(metaHash, addExt(newDoc.DocumentID, archive.MetadataExt), metaReader); err != nil {
		log.Printf("[%s] share %q: upload metadata failed: %v", dstName, share.Name, err)
		return
	}

	if err := newDoc.Rehash(); err != nil {
		log.Printf("[%s] share %q: rehash failed: %v", dstName, share.Name, err)
		return
	}

	indexReader, err := newDoc.IndexReader()
	if err != nil {
		log.Printf("[%s] share %q: index reader failed: %v", dstName, share.Name, err)
		return
	}
	if err := dst.Storage.UploadBlob(newDoc.Hash, addExt(newDoc.DocumentID, archive.DocSchemaExt), indexReader); err != nil {
		log.Printf("[%s] share %q: upload index failed: %v", dstName, share.Name, err)
		return
	}

	if err := dst.SyncWithOperation(func(t *sync15.HashTree) error {
		return t.Add(newDoc)
	}, true); err != nil {
		log.Printf("[%s] share %q: sync new doc failed: %v", dstName, share.Name, err)
		return
	}

	share.DocIDs[dstName] = newDoc.DocumentID
	log.Printf("[%s] share %q: replaced mismatched doc, now using UUID %s", dstName, share.Name, newDoc.DocumentID)
}

func (e *SyncEngine) copyFullDoc(src, dst *Account, srcDoc *sync15.BlobDoc, dstName string, share *ShareConfig, dstPath string) {
	parentPath, docName := splitPath(dstPath)

	parentNode, err := dst.FileTree.NodeByPath(parentPath, dst.FileTree.Root())
	if err != nil {
		log.Printf("[%s] parent path %q not found: %v", dstName, parentPath, err)
		return
	}

	if err := copyDocBlobs(dst, src, srcDoc); err != nil {
		log.Printf("[%s] copy full doc blobs failed: %v", dstName, err)
		return
	}

	newDoc := cloneBlobDoc(srcDoc)
	newDoc.Metadata.Parent = parentNode.Id()
	newDoc.Metadata.DocName = docName

	metaHash, metaReader, err := newDoc.MetadataHashAndReader()
	if err != nil {
		log.Printf("[%s] metadata hash failed: %v", dstName, err)
		return
	}
	if err := dst.Storage.UploadBlob(metaHash, addExt(newDoc.DocumentID, archive.MetadataExt), metaReader); err != nil {
		log.Printf("[%s] upload metadata failed: %v", dstName, err)
		return
	}

	if err := newDoc.Rehash(); err != nil {
		log.Printf("[%s] rehash failed: %v", dstName, err)
		return
	}

	indexReader, err := newDoc.IndexReader()
	if err != nil {
		log.Printf("[%s] index reader failed: %v", dstName, err)
		return
	}
	if err := dst.Storage.UploadBlob(newDoc.Hash, addExt(newDoc.DocumentID, archive.DocSchemaExt), indexReader); err != nil {
		log.Printf("[%s] upload index failed: %v", dstName, err)
		return
	}

	err = dst.SyncWithOperation(func(t *sync15.HashTree) error {
		return t.Add(newDoc)
	}, true)
	if err != nil {
		log.Printf("[%s] sync new doc failed: %v", dstName, err)
		return
	}

	share.DocIDs[dstName] = newDoc.DocumentID
	log.Printf("[%s] share %q: full document copied from %s", dstName, share.Name, src.Name)
}

func splitPath(path string) (parent, name string) {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i], path[i+1:]
		}
	}
	return "/", path
}

