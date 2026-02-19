package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/juruen/rmapi/api/sync15"
	"github.com/juruen/rmapi/archive"
	"github.com/juruen/rmapi/config"
	"github.com/juruen/rmapi/filetree"
	"github.com/juruen/rmapi/model"
	"github.com/juruen/rmapi/transport"
	"gopkg.in/yaml.v2"
)

const maxConcurrent = 20

type Account struct {
	Name        string
	HttpCtx     *transport.HttpClientCtx
	Storage     *sync15.BlobStorage
	Tree        *sync15.HashTree
	FileTree    *filetree.FileTreeCtx
	CloudHost   string
	TokenFile   string
	mu          sync.Mutex
}

func NewAccount(name string, cfg AccountConfig) (*Account, error) {
	tokens, err := loadTokenFile(cfg.TokenFile)
	if err != nil {
		return nil, fmt.Errorf("account %s: %w", name, err)
	}

	if cfg.CloudHost != "" {
		setHostURLs(cfg.CloudHost)
	}

	httpCtx := transport.CreateHttpClientCtx(tokens)

	if tokens.UserToken == "" {
		userToken, err := renewUserToken(&httpCtx)
		if err != nil {
			return nil, fmt.Errorf("account %s: renewing user token: %w", name, err)
		}
		tokens.UserToken = userToken
		httpCtx.Tokens.UserToken = userToken
		saveTokenFile(cfg.TokenFile, tokens)
	}

	storage := sync15.NewBlobStorage(&httpCtx)
	tree := &sync15.HashTree{}

	if err := tree.Mirror(storage, maxConcurrent); err != nil {
		return nil, fmt.Errorf("account %s: initial mirror: %w", name, err)
	}

	ft := sync15.DocumentsFileTree(tree)

	return &Account{
		Name:      name,
		HttpCtx:   &httpCtx,
		Storage:   storage,
		Tree:      tree,
		FileTree:  ft,
		CloudHost: cfg.CloudHost,
		TokenFile: cfg.TokenFile,
	}, nil
}

func (a *Account) RootHash() (string, int64, error) {
	return a.Storage.GetRootIndex()
}

func (a *Account) RefreshTree() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.Tree.Mirror(a.Storage, maxConcurrent); err != nil {
		return err
	}
	a.FileTree = sync15.DocumentsFileTree(a.Tree)
	return nil
}

func (a *Account) FindDocByPath(path string) (*sync15.BlobDoc, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	node, err := a.FileTree.NodeByPath(path, a.FileTree.Root())
	if err != nil {
		return nil, fmt.Errorf("path %q: %w", path, err)
	}
	return a.Tree.FindDoc(node.Id())
}

func (a *Account) FindDocByID(id string) (*sync15.BlobDoc, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.Tree.FindDoc(id)
}

func (a *Account) SyncWithOperation(op func(t *sync15.HashTree) error, notify bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	err := sync15.Sync(a.Storage, a.Tree, op, notify)
	if err != nil {
		return err
	}
	a.FileTree = sync15.DocumentsFileTree(a.Tree)
	return nil
}

func (a *Account) RenewToken() error {
	userToken, err := renewUserToken(a.HttpCtx)
	if err != nil {
		return err
	}
	a.HttpCtx.Tokens.UserToken = userToken
	tokens := model.AuthTokens{
		DeviceToken: a.HttpCtx.Tokens.DeviceToken,
		UserToken:   userToken,
	}
	return saveTokenFile(a.TokenFile, tokens)
}

func loadTokenFile(path string) (model.AuthTokens, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return model.AuthTokens{}, err
	}
	var tokens model.AuthTokens
	if err := yaml.Unmarshal(data, &tokens); err != nil {
		return model.AuthTokens{}, err
	}
	return tokens, nil
}

func saveTokenFile(path string, tokens model.AuthTokens) error {
	data, err := yaml.Marshal(tokens)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}

func renewUserToken(httpCtx *transport.HttpClientCtx) (string, error) {
	resp := transport.BodyString{}
	err := httpCtx.Post(transport.DeviceBearer, config.NewUserDevice, nil, &resp)
	if err != nil {
		return "", err
	}
	return resp.Content, nil
}

func setHostURLs(host string) {
	config.NewTokenDevice = host + "/token/json/2/device/new"
	config.NewUserDevice = host + "/token/json/2/user/new"
	config.ListDocs = host + "/document-storage/json/2/docs"
	config.UpdateStatus = host + "/document-storage/json/2/upload/update-status"
	config.UploadRequest = host + "/document-storage/json/2/upload/request"
	config.DeleteEntry = host + "/document-storage/json/2/delete"
	config.UploadBlob = host + "/sync/v2/signed-urls/uploads"
	config.DownloadBlob = host + "/sync/v2/signed-urls/downloads"
	config.SyncComplete = host + "/sync/v2/sync-complete"
	config.BlobUrl = host + "/sync/v3/files/"
	config.RootGet = host + "/sync/v4/root"
	config.RootPut = host + "/sync/v3/root"
}

func addExt(name string, ext archive.RmExt) string {
	return name + "." + string(ext)
}

func bufferBlob(src *sync15.BlobStorage, hash, name string) (*bytes.Reader, error) {
	rc, err := src.GetReader(hash, name)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(data), nil
}

func copyDocBlobs(dst, src *Account, srcDoc *sync15.BlobDoc) error {
	for _, f := range srcDoc.Files {
		buf, err := bufferBlob(src.Storage, f.Hash, f.DocumentID)
		if err != nil {
			return fmt.Errorf("reading file %s: %w", f.DocumentID, err)
		}
		if err := dst.Storage.UploadBlob(f.Hash, f.DocumentID, buf); err != nil {
			return fmt.Errorf("uploading file %s: %w", f.DocumentID, err)
		}
	}

	indexReader, err := srcDoc.IndexReader()
	if err != nil {
		return fmt.Errorf("reading doc index: %w", err)
	}
	err = dst.Storage.UploadBlob(srcDoc.Hash, addExt(srcDoc.DocumentID, archive.DocSchemaExt), indexReader)
	if err != nil {
		return fmt.Errorf("uploading doc index: %w", err)
	}
	return nil
}

func copyChangedBlobs(dst, src *Account, srcDoc, dstDoc *sync15.BlobDoc) (int, error) {
	dstHashes := make(map[string]string)
	for _, f := range dstDoc.Files {
		dstHashes[f.DocumentID] = f.Hash
	}

	changed := 0
	for _, sf := range srcDoc.Files {
		if hash, ok := dstHashes[sf.DocumentID]; ok && hash == sf.Hash {
			continue
		}
		buf, err := bufferBlob(src.Storage, sf.Hash, sf.DocumentID)
		if err != nil {
			return changed, fmt.Errorf("reading %s: %w", sf.DocumentID, err)
		}
		if err := dst.Storage.UploadBlob(sf.Hash, sf.DocumentID, buf); err != nil {
			return changed, fmt.Errorf("uploading %s: %w", sf.DocumentID, err)
		}
		changed++
	}

	return changed, nil
}

func cloneBlobDoc(src *sync15.BlobDoc) *sync15.BlobDoc {
	d := &sync15.BlobDoc{
		Entry:    src.Entry,
		Metadata: src.Metadata,
	}
	d.Files = make([]*sync15.Entry, len(src.Files))
	for i, f := range src.Files {
		eCopy := *f
		d.Files[i] = &eCopy
	}
	return d
}

