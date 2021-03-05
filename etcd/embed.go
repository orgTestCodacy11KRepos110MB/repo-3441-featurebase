// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcd

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net"
	"path"
	"sort"
	"strings"

	"github.com/pilosa/pilosa/v2/disco"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pilosa/pilosa/v2/topology"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/clientv3util"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/etcdserver/api/v3client"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/pkg/types"
)

type Options struct {
	Name         string `toml:"name"`
	Dir          string `toml:"dir"`
	LClientURL   string `toml:"listen-client-url"`
	AClientURL   string `toml:"advertise-client-url"`
	LPeerURL     string `toml:"listen-peer-url"`
	APeerURL     string `toml:"advertise-peer-url"`
	ClusterURL   string `toml:"cluster-url"`
	InitCluster  string `toml:"initial-cluster"`
	ClusterName  string `toml:"cluster-name"`
	HeartbeatTTL int64  `toml:"heartbeat-ttl"`

	LPeerSocket   []*net.TCPListener
	LClientSocket []*net.TCPListener
}

var (
	_ disco.DisCo     = &Etcd{}
	_ disco.Schemator = &Etcd{}
	_ disco.Stator    = &Etcd{}
	_ disco.Metadator = &Etcd{}
	_ disco.Resizer   = &Etcd{}
	_ disco.Sharder   = &Etcd{}
)

const (
	heartbeatPrefix = "/heartbeat/"
	schemaPrefix    = "/schema/"
	resizePrefix    = "/resize/"
	metadataPrefix  = "/metadata/"
	shardPrefix     = "/shard/"
	lockPrefix      = "/lock/"
)

type Etcd struct {
	options  Options
	replicas int

	e   *embed.Etcd
	cli *clientv3.Client

	heartbeatLeasedKV, resizeLeasedKV *leasedKV
}

func NewEtcd(opt Options, replicas int) *Etcd {
	e := &Etcd{
		options:  opt,
		replicas: replicas,
	}

	if e.options.HeartbeatTTL == 0 {
		e.options.HeartbeatTTL = 5 // seconds
	}
	return e
}

// Close implements io.Closer
func (e *Etcd) Close() error {
	if e.e != nil {
		if e.resizeLeasedKV != nil {
			e.resizeLeasedKV.Stop()
			e.resizeLeasedKV = nil
		}
		if e.heartbeatLeasedKV != nil {
			e.heartbeatLeasedKV.Stop()
		}

		e.e.Close()
		<-e.e.Server.StopNotify()
	}

	if e.cli != nil {
		e.cli.Close()
	}

	return nil
}

func parseOptions(opt Options) *embed.Config {
	cfg := embed.NewConfig()
	cfg.Debug = false // true gives data races on grpc.EnableTracing in etcd
	cfg.LogLevel = "error"
	cfg.Logger = "zap"
	cfg.Name = opt.Name
	cfg.Dir = opt.Dir
	cfg.InitialClusterToken = opt.ClusterName
	cfg.LCUrls = types.MustNewURLs([]string{opt.LClientURL})
	if opt.AClientURL != "" {
		cfg.ACUrls = types.MustNewURLs([]string{opt.AClientURL})
	} else {
		cfg.ACUrls = cfg.LCUrls
	}
	cfg.LPUrls = types.MustNewURLs([]string{opt.LPeerURL})
	if opt.APeerURL != "" {
		cfg.APUrls = types.MustNewURLs([]string{opt.APeerURL})
	} else {
		cfg.APUrls = cfg.LPUrls
	}

	lps := make([]*net.TCPListener, len(opt.LPeerSocket))
	copy(lps, opt.LPeerSocket)
	cfg.LPeerSocket = lps

	lcs := make([]*net.TCPListener, len(opt.LPeerSocket))
	copy(lcs, opt.LClientSocket)
	cfg.LClientSocket = lcs

	if opt.InitCluster != "" {
		cfg.InitialCluster = opt.InitCluster
		cfg.ClusterState = embed.ClusterStateFlagNew
	} else {
		cfg.InitialCluster = cfg.Name + "=" + opt.APeerURL
	}

	if opt.ClusterURL != "" {
		cfg.ClusterState = embed.ClusterStateFlagExisting

		cli, err := clientv3.NewFromURL(opt.ClusterURL)
		if err != nil {
			panic(err)
		}
		defer cli.Close()

		log.Println("Cluster Members:")
		mIDs, mNames, mURLs := memberList(cli)
		for i, id := range mIDs {
			log.Printf("\tid: %d, name: %s, url: %s\n", id, mNames[i], mURLs[i])
			cfg.InitialCluster += "," + mNames[i] + "=" + mURLs[i]
		}

		log.Println("Joining Cluster:")
		id, name := memberAdd(cli, opt.APeerURL)
		log.Printf("\tid: %d, name: %s\n", id, name)
	}

	return cfg
}

// Start starts etcd and hearbeat
func (e *Etcd) Start(ctx context.Context) (disco.InitialClusterState, error) {
	opts := parseOptions(e.options)
	state := disco.InitialClusterState(opts.ClusterState)

	etcd, err := embed.StartEtcd(opts)
	if err != nil {
		return state, errors.Wrap(err, "starting etcd")
	}
	e.e = etcd
	e.cli = v3client.New(e.e.Server)

	select {
	case <-ctx.Done():
		e.e.Server.Stop()
		return state, ctx.Err()

	case err := <-e.e.Err():
		return state, err

	case <-e.e.Server.ReadyNotify():
		return state, e.startHeartbeat(ctx)
	}
}

func (e *Etcd) startHeartbeat(ctx context.Context) error {
	key := heartbeatPrefix + e.e.Server.ID().String()
	e.heartbeatLeasedKV = newLeasedKV(e.cli, key, e.options.HeartbeatTTL)

	if err := e.heartbeatLeasedKV.Start(string(disco.NodeStateStarting)); err != nil {
		return errors.Wrap(err, "startHeartbeat: starting a new heartbeat")
	}

	return nil
}

func (e *Etcd) NodeState(ctx context.Context, peerID string) (disco.NodeState, error) {
	return e.nodeState(ctx, peerID)
}

func (e *Etcd) nodeState(ctx context.Context, peerID string) (disco.NodeState, error) {
	resp, err := e.cli.Txn(ctx).
		If(clientv3util.KeyMissing(path.Join(resizePrefix, peerID))).
		Then(clientv3.OpGet(path.Join(heartbeatPrefix, peerID))).
		Commit()
	if err != nil {
		return disco.NodeStateUnknown, err
	}

	if !resp.Succeeded {
		return disco.NodeStateResizing, nil
	}

	if len(resp.Responses) == 0 {
		return disco.NodeStateUnknown, disco.ErrNoResults
	}

	kvs := resp.Responses[0].GetResponseRange().Kvs
	if len(kvs) == 0 {
		return disco.NodeStateUnknown, disco.ErrNoResults
	}
	if len(kvs) > 1 {
		return disco.NodeStateUnknown, disco.ErrTooManyResults
	}

	return disco.NodeState(kvs[0].Value), nil
}

func (e *Etcd) NodeStates(ctx context.Context) (map[string]disco.NodeState, error) {
	members := e.e.Server.Cluster().Members()
	ops := make([]clientv3.Op, 2*(len(members)))
	for i, member := range members {
		peerID := member.ID.String()
		ops[2*i] = clientv3.OpGet(path.Join(resizePrefix, peerID), clientv3.WithCountOnly())
		ops[2*i+1] = clientv3.OpGet(path.Join(heartbeatPrefix, peerID))
	}

	resp, err := e.cli.Txn(ctx).Then(ops...).Commit()
	if err != nil {
		return nil, err
	}

	out := make(map[string]disco.NodeState, len(members))
	for i, member := range members {
		peerID := member.ID.String()
		if resp.Responses[2*i].GetResponseRange().Count > 0 {
			// This node is processing a resize operation.
			out[peerID] = disco.NodeStateResizing
			continue
		}

		kvs := resp.Responses[2*i+1].GetResponseRange().Kvs
		switch len(kvs) {
		case 0:
			// The node has not reported a state.
			out[peerID] = disco.NodeStateUnknown
		case 1:
			// The node has reported its state.
			out[peerID] = disco.NodeState(kvs[0].Value)
		default:
			return nil, disco.ErrTooManyResults
		}
	}

	return out, nil
}

func (e *Etcd) Started(ctx context.Context) (err error) {
	return e.heartbeatLeasedKV.Set(ctx, string(disco.NodeStateStarted))
}

func (e *Etcd) ID() string {
	if e.e == nil || e.e.Server == nil {
		return ""
	}
	return e.e.Server.ID().String()
}

func (e *Etcd) Peers() []*disco.Peer {
	var peers []*disco.Peer
	for _, member := range e.e.Server.Cluster().Members() {
		peers = append(peers, &disco.Peer{ID: member.ID.String(), URL: member.PickPeerURL()})
	}
	return peers
}

func (e *Etcd) IsLeader() bool {
	if e.e == nil || e.e.Server == nil {
		return false
	}
	return e.e.Server.Leader() == e.e.Server.ID()
}

func (e *Etcd) Leader() *disco.Peer {
	id := e.e.Server.Leader()
	peer := &disco.Peer{ID: id.String()}

	if m := e.e.Server.Cluster().Member(id); m != nil {
		peer.URL = m.PickPeerURL()
	}

	return peer
}

func (e *Etcd) ClusterState(ctx context.Context) (disco.ClusterState, error) {
	if e.e == nil {
		return disco.ClusterStateUnknown, nil
	}

	var (
		heartbeats int = 0
		resize     bool
		starting   bool
	)
	states, err := e.NodeStates(ctx)
	if err != nil {
		return disco.ClusterStateUnknown, err
	}

	for _, state := range states {
		switch state {
		case disco.NodeStateStarting:
			starting = true
		case disco.NodeStateResizing:
			resize = true
		case disco.NodeStateUnknown:
			continue
		}

		heartbeats++
	}

	if resize {
		return disco.ClusterStateResizing, nil
	}

	if starting {
		return disco.ClusterStateStarting, nil
	}

	if heartbeats < len(states) {
		if len(states)-heartbeats >= e.replicas {
			return disco.ClusterStateDown, nil
		}

		return disco.ClusterStateDegraded, nil
	}

	return disco.ClusterStateNormal, nil
}

func (e *Etcd) Resize(ctx context.Context) (func([]byte) error, error) {
	key := path.Join(resizePrefix, e.e.Server.ID().String())
	if e.resizeLeasedKV == nil {
		e.resizeLeasedKV = newLeasedKV(e.cli, key, e.options.HeartbeatTTL)
	}

	if err := e.resizeLeasedKV.Start(""); err != nil {
		return nil, errors.Wrap(err, "Resize: creates a new hearbeat")
	}

	return func(value []byte) error {
		log.Println("Update progress:", key, string(value))
		return e.putKey(ctx, key, string(value), clientv3.WithIgnoreLease())
	}, nil
}

func (e *Etcd) DoneResize() error {
	if e.resizeLeasedKV != nil {
		e.resizeLeasedKV.Stop()
	}

	e.resizeLeasedKV = nil
	return nil
}

func (e *Etcd) Watch(ctx context.Context, peerID string, onUpdate func([]byte) error) error {
	key := path.Join(resizePrefix, peerID)
	for resp := range e.cli.Watch(ctx, key) {
		if err := resp.Err(); err != nil {
			return errors.Wrapf(err, "Watch: key (%s) response", key)
		}

		for _, ev := range resp.Events {
			switch ev.Type {
			case mvccpb.PUT:
				if onUpdate != nil && ev.Kv.Value != nil {
					if err := onUpdate(ev.Kv.Value); err != nil {
						return err
					}
				}

			case mvccpb.DELETE:
				// nothing to watch - key was deleted
				return errors.WithMessagef(disco.ErrKeyDeleted, "Watch key %s", key)
			}
		}
	}

	return nil
}

func (e *Etcd) DeleteNode(ctx context.Context, nodeID string) error {
	id, err := types.IDFromString(nodeID)
	if err != nil {
		return err
	}

	_, err = e.cli.MemberRemove(ctx, uint64(id))
	if err != nil {
		return errors.Wrap(err, "DeleteNode: removes an existing member from the cluster")
	}

	return nil
}

func (e *Etcd) Schema(ctx context.Context) (disco.Schema, error) {
	keys, vals, err := e.getKeyWithPrefix(ctx, schemaPrefix)
	if err != nil {
		return nil, err
	}

	// The logic in the following for loop assumes that the list of keys is
	// ordered such that index comes before field, which comes before view.
	// For example:
	//   /index1
	//   /index1/field1
	//   /index1/field1/view1
	//   /index1/field1/view2
	//   /index1/field2
	//   /index2
	//   /index2/field1
	//
	m := make(disco.Schema)
	for i, k := range keys {
		tokens := strings.Split(strings.Trim(k, "/"), "/")
		// token[0] contains the schemaPrefix

		// token[1]: index
		index := tokens[1]
		if _, ok := m[index]; !ok {
			m[index] = &disco.Index{
				Data:   vals[i],
				Fields: make(map[string]*disco.Field),
			}
			continue
		}
		flds := m[index].Fields

		// token[2]: field
		if len(tokens) > 2 {
			field := tokens[2]
			if _, ok := flds[field]; !ok {
				flds[field] = &disco.Field{
					Data:  vals[i],
					Views: make(map[string]struct{}),
				}
				continue
			}
			views := flds[field].Views

			// token[3]: view
			if len(tokens) > 3 {
				view := tokens[3]
				views[view] = struct{}{}
			}
		}
	}
	return m, nil
}

func (e *Etcd) Metadata(ctx context.Context, peerID string) ([]byte, error) {
	resp, err := e.cli.KV.Get(ctx, path.Join(metadataPrefix, peerID))
	if err != nil {
		return nil, err
	}
	kvs := resp.Kvs

	if len(kvs) > 1 {
		return nil, disco.ErrTooManyResults
	}

	if len(kvs) == 0 {
		return nil, disco.ErrNoResults
	}

	return kvs[0].Value, nil
}

func (e *Etcd) SetMetadata(ctx context.Context, metadata []byte) error {
	err := e.putKey(ctx, path.Join(metadataPrefix,
		e.e.Server.ID().String()),
		string(metadata),
	)
	if err != nil {
		return errors.Wrap(err, "SetMetadata")
	}

	return nil
}

func (e *Etcd) CreateIndex(ctx context.Context, name string, val []byte) error {
	key := schemaPrefix + name

	// Set up Op to write index value as bytes.
	op := clientv3.OpPut(key, "")
	op.WithValueBytes(val)

	// Check for key existence, and execute Op within a transaction.
	resp, err := e.cli.Txn(ctx).
		If(clientv3util.KeyMissing(key)).
		Then(op).
		Commit()
	if err != nil {
		return errors.Wrap(err, "executing transaction")
	}

	if !resp.Succeeded {
		return disco.ErrIndexExists
	}

	return nil
}

func (e *Etcd) Index(ctx context.Context, name string) ([]byte, error) {
	return e.getKeyBytes(ctx, schemaPrefix+name)
}

func (e *Etcd) DeleteIndex(ctx context.Context, name string) (err error) {
	key := schemaPrefix + name
	// Deleting index and fields in one transaction.
	_, err = e.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), ">", -1)).
		Then(
			clientv3.OpDelete(key+"/", clientv3.WithPrefix()), // deleting index fields
			clientv3.OpDelete(key),                            // deleting index
		).Commit()

	return errors.Wrap(err, "DeleteIndex")
}

func (e *Etcd) Field(ctx context.Context, indexName string, name string) ([]byte, error) {
	key := schemaPrefix + indexName + "/" + name
	return e.getKeyBytes(ctx, key)
}

func (e *Etcd) CreateField(ctx context.Context, indexName string, name string, val []byte) error {
	key := schemaPrefix + indexName + "/" + name

	// Set up Op to write field value as bytes.
	op := clientv3.OpPut(key, "")
	op.WithValueBytes(val)

	// Check for key existence, and execute Op within a transaction.
	resp, err := e.cli.Txn(ctx).
		If(clientv3util.KeyMissing(key)).
		Then(op).
		Commit()
	if err != nil {
		return errors.Wrap(err, "executing transaction")
	}

	if !resp.Succeeded {
		return disco.ErrFieldExists
	}

	return nil
}

func (e *Etcd) DeleteField(ctx context.Context, indexname string, name string) (err error) {
	key := schemaPrefix + indexname + "/" + name
	// Deleting field and views in one transaction.
	_, err = e.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), ">", -1)).
		Then(
			clientv3.OpDelete(key+"/", clientv3.WithPrefix()), // deleting field views
			clientv3.OpDelete(key),                            // deleting field
		).Commit()

	return errors.Wrap(err, "DeleteField")
}

func (e *Etcd) View(ctx context.Context, indexName, fieldName, name string) (bool, error) {
	key := schemaPrefix + indexName + "/" + fieldName + "/" + name
	return e.keyExists(ctx, key)
}

// CreateView differs from CreateIndex and CreateField in that it does not
// return an error if the view already exists. If this logic needs to be
// changed, we likely need to return disco.ErrViewExists.
func (e *Etcd) CreateView(ctx context.Context, indexName, fieldName, name string) (err error) {
	key := schemaPrefix + indexName + "/" + fieldName + "/" + name

	// Check for key existence, and execute Op within a transaction.
	_, err = e.cli.Txn(ctx).
		If(clientv3util.KeyMissing(key)).
		Then(clientv3.OpPut(key, "")).
		Commit()
	if err != nil {
		return errors.Wrap(err, "executing transaction")
	}

	return nil
}

func (e *Etcd) DeleteView(ctx context.Context, indexName, fieldName, name string) error {
	return e.delKey(ctx, schemaPrefix+indexName+"/"+fieldName+"/"+name, false)
}

func (e *Etcd) putKey(ctx context.Context, key, val string, opts ...clientv3.OpOption) error {
	if _, err := e.cli.Txn(ctx).
		Then(clientv3.OpPut(key, val, opts...)).
		Commit(); err != nil {
		return errors.Wrapf(err, "putKey: Put(%s, %s)", key, val)
	}

	return nil
}

func (e *Etcd) getKeyBytes(ctx context.Context, key string) ([]byte, error) {
	// Get the current value for the key.
	op := clientv3.OpGet(key)
	resp, err := e.cli.Txn(ctx).Then(op).Commit()
	if err != nil {
		return nil, err
	}

	if len(resp.Responses) == 0 {
		return nil, errors.New("key does not exist")
	}

	kvs := resp.Responses[0].GetResponseRange().Kvs
	if len(kvs) == 0 {
		return nil, errors.New("key does not exist")
	}

	return kvs[0].Value, nil
}

func (e *Etcd) getKeyWithPrefix(ctx context.Context, key string) (keys []string, values [][]byte, err error) {
	resp, err := e.cli.Txn(ctx).
		Then(clientv3.OpGet(key, clientv3.WithPrefix())).
		Commit()
	if err != nil {
		return nil, nil, err
	}

	if len(resp.Responses) == 0 {
		return nil, nil, errors.New("key does not exist")
	}

	kvs := resp.Responses[0].GetResponseRange().Kvs
	if len(kvs) == 0 {
		return nil, nil, nil
	}

	keys = make([]string, len(kvs))
	values = make([][]byte, len(kvs))
	for i, kv := range kvs {
		keys[i] = string(kv.Key)
		values[i] = kv.Value
	}

	return keys, values, nil
}

func (e *Etcd) keyExists(ctx context.Context, key string) (bool, error) {
	resp, err := e.cli.Txn(ctx).
		If(clientv3util.KeyExists(key)).
		Then(clientv3.OpGet(key, clientv3.WithCountOnly())).
		Commit()
	if err != nil {
		return false, err
	}
	if !resp.Succeeded {
		return false, nil
	}

	if len(resp.Responses) == 0 {
		return false, nil
	}
	return resp.Responses[0].GetResponseRange().Count > 0, nil
}

func (e *Etcd) delKey(ctx context.Context, key string, withPrefix bool) (err error) {
	if withPrefix {
		_, err = e.cli.Delete(ctx, key, clientv3.WithPrefix())
	} else {
		_, err = e.cli.Delete(ctx, key)
	}
	return err
}

func memberList(cli *clientv3.Client) (ids []uint64, names []string, urls []string) {
	ml, err := cli.MemberList(context.TODO())
	if err != nil {
		panic(err)
	}
	n := len(ml.Members)
	ids = make([]uint64, n)
	names = make([]string, n)
	urls = make([]string, n)

	for i, m := range ml.Members {
		ids[i], names[i], urls[i] = m.ID, m.Name, m.PeerURLs[0]
	}
	return
}

func memberAdd(cli *clientv3.Client, peerURL string) (id uint64, name string) {
	ma, err := cli.MemberAdd(context.TODO(), []string{peerURL})
	if err != nil {
		return 0, ""
	}

	return ma.Member.ID, ma.Member.Name
}

// Shards implements the Sharder interface.
func (e *Etcd) Shards(ctx context.Context, index, field string) (*roaring.Bitmap, error) {
	return e.shards(ctx, index, field)
}

func (e *Etcd) shards(ctx context.Context, index, field string) (*roaring.Bitmap, error) {
	key := path.Join(shardPrefix, index, field)

	// Get the current shards for the field.
	resp, err := e.cli.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	bm := roaring.NewBitmap()

	if len(resp.Kvs) == 0 {
		return bm, nil
	}

	bytes := resp.Kvs[0].Value
	if err = bm.UnmarshalBinary(bytes); err != nil {
		return nil, errors.Wrap(err, "unmarshalling shards")
	}

	return bm, nil
}

// AddShards implements the Sharder interface.
func (e *Etcd) AddShards(ctx context.Context, index, field string, shards *roaring.Bitmap) (*roaring.Bitmap, error) {
	key := path.Join(shardPrefix, index, field)

	// This tended to add more overhead than it saved.
	// // Read shards outside of a lock just to check if shard is already included.
	// // If shard is already included, no-op.
	// if currentShards, err := e.shards(ctx, cli, index, field); err != nil {
	// 	return nil, errors.Wrap(err, "reading shards")
	// } else if currentShards.Count() == currentShards.Union(shards).Count() {
	// 	return currentShards, nil
	// }

	// Create a session to acquire a lock.
	sess, _ := concurrency.NewSession(e.cli)
	defer sess.Close()

	muKey := path.Join(lockPrefix, index, field)
	mu := concurrency.NewMutex(sess, muKey)

	// Acquire lock (or wait to have it).
	if err := mu.Lock(ctx); err != nil {
		return nil, errors.Wrap(err, "acquiring lock")
	}

	// Read shards within lock.
	globalShards, err := e.shards(ctx, index, field)
	if err != nil {
		return nil, errors.Wrap(err, "reading shards")
	}

	// Union shard into shards.
	globalShards.UnionInPlace(shards)

	// Write shards to etcd.
	var buf bytes.Buffer
	if _, err := globalShards.WriteTo(&buf); err != nil {
		return nil, errors.Wrap(err, "writing shards to bytes buffer")
	}

	op := clientv3.OpPut(key, "")
	op.WithValueBytes(buf.Bytes())

	if _, err := e.cli.Do(ctx, op); err != nil {
		return nil, errors.Wrap(err, "doing op")
	}

	// Release lock.
	if err := mu.Unlock(ctx); err != nil {
		return nil, errors.Wrap(err, "releasing lock")
	}

	return globalShards, nil
}

// AddShard implements the Sharder interface.
func (e *Etcd) AddShard(ctx context.Context, index, field string, shard uint64) error {
	key := path.Join(shardPrefix, index, field)

	// Read shards outside of a lock just to check if shard is already included.
	// If shard is already included, no-op.
	if shards, err := e.shards(ctx, index, field); err != nil {
		return errors.Wrap(err, "reading shards")
	} else if shards.Contains(shard) {
		return nil
	}

	// According to the previous read, shard is not yet included in shards. So
	// we will acquire a distributed lock, read shards again (in case it has
	// been updated since we last read it), add shard to shards, and finally
	// write shards to etcd.

	// Create a session to acquire a lock.
	sess, _ := concurrency.NewSession(e.cli)
	defer sess.Close()

	muKey := path.Join(lockPrefix, index, field)
	mu := concurrency.NewMutex(sess, muKey)

	// Acquire lock (or wait to have it).
	if err := mu.Lock(ctx); err != nil {
		return errors.Wrap(err, "acquiring lock")
	}

	// Read shards again (within lock).
	shards, err := e.shards(ctx, index, field)
	if err != nil {
		return errors.Wrap(err, "reading shards")
	}

	if shards.Contains(shard) {
		return nil
	}

	// Union shard into shards.
	shards.UnionInPlace(roaring.NewBitmap(shard))

	// Write shards to etcd.
	var buf bytes.Buffer
	if _, err := shards.WriteTo(&buf); err != nil {
		return errors.Wrap(err, "writing shards to bytes buffer")
	}

	op := clientv3.OpPut(key, "")
	op.WithValueBytes(buf.Bytes())

	if _, err := e.cli.Do(ctx, op); err != nil {
		return errors.Wrap(err, "doing op")
	}

	// Release lock.
	if err := mu.Unlock(ctx); err != nil {
		return errors.Wrap(err, "releasing lock")
	}

	return nil
}

// RemoveShard implements the Sharder interface.
func (e *Etcd) RemoveShard(ctx context.Context, index, field string, shard uint64) error {
	key := path.Join(shardPrefix, index, field)

	// Read shards outside of a lock just to check if shard is already excluded.
	// If shard is already excluded, no-op.
	if shards, err := e.shards(ctx, index, field); err != nil {
		return errors.Wrap(err, "reading shards")
	} else if !shards.Contains(shard) {
		return nil
	}

	// According to the previous read, shard is included in shards. So
	// we will acquire a distributed lock, read shards again (in case it has
	// been updated since we last read it), remove shard from shards, and finally
	// write shards to etcd.

	// Create a session to acquire a lock.
	sess, _ := concurrency.NewSession(e.cli)
	defer sess.Close()

	muKey := path.Join(lockPrefix, index, field)
	mu := concurrency.NewMutex(sess, muKey)

	// Acquire lock (or wait to have it).
	if err := mu.Lock(ctx); err != nil {
		return errors.Wrap(err, "acquiring lock")
	}

	// Read shards again (within lock).
	shards, err := e.shards(ctx, index, field)
	if err != nil {
		return errors.Wrap(err, "reading shards")
	}

	if !shards.Contains(shard) {
		return nil
	}

	// Remove shard from shards.
	if _, err := shards.RemoveN(shard); err != nil {
		return errors.Wrap(err, "removing shard")
	}

	// If this is removing the last bit from the shards bitmap, then instead of
	// writing an empty bitmap, just delete the key.
	if shards.Count() == 0 {
		_, err := e.cli.Delete(ctx, key)
		return err
	}

	// Write shards to etcd.
	var buf bytes.Buffer
	if _, err := shards.WriteTo(&buf); err != nil {
		return errors.Wrap(err, "writing shards to bytes buffer")
	}

	op := clientv3.OpPut(key, "")
	op.WithValueBytes(buf.Bytes())

	if _, err := e.cli.Do(ctx, op); err != nil {
		return errors.Wrap(err, "doing op")
	}

	// Release lock.
	if err := mu.Unlock(ctx); err != nil {
		return errors.Wrap(err, "releasing lock")
	}

	return nil
}

// Nodes implements the Noder interface. It returns the sorted list of nodes
// based on the etcd peers.
func (e *Etcd) Nodes() []*topology.Node {
	peers := e.Peers()
	// For N>1, this might actually reduce GC load. Maybe.
	nodeData := make([]topology.Node, len(peers))
	nodes := make([]*topology.Node, len(peers))
	for i, peer := range peers {
		node := &nodeData[i]

		if meta, err := e.Metadata(context.Background(), peer.ID); err != nil {
			log.Println(err, "getting metadata") // TODO: handle this with a logger
		} else if err := json.Unmarshal(meta, node); err != nil {
			log.Println(err, "unmarshaling json metadata")
		}

		node.ID = peer.ID

		nodes[i] = node
	}

	// Nodes must be sorted.
	sort.Sort(topology.ByID(nodes))

	return nodes
}

// PrimaryNodeID implements the Noder interface.
func (e *Etcd) PrimaryNodeID(hasher topology.Hasher) string {
	return topology.PrimaryNodeID(e.NodeIDs(), hasher)
}

// NodeIDs returns the list of node IDs in the etcd cluster.
func (e *Etcd) NodeIDs() []string {
	peers := e.Peers()
	ids := make([]string, len(peers))
	for i, peer := range peers {
		ids[i] = peer.ID
	}
	return ids
}

// SetNodes implements the Noder interface as NOP
// (because we can't force to set nodes for etcd).
func (e *Etcd) SetNodes(nodes []*topology.Node) {}

// AppendNode implements the Noder interface as NOP
// (because resizer is responsible for adding new nodes).
func (e *Etcd) AppendNode(node *topology.Node) {}

// RemoveNode implements the Noder interface as NOP
// (because resizer is responsible for removing existing nodes)
func (e *Etcd) RemoveNode(nodeID string) bool {
	return false
}
