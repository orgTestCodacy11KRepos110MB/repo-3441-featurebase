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

package ctl

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/http"
	"github.com/pilosa/pilosa/v2/server"
	"github.com/pilosa/pilosa/v2/topology"
)

// BackupCommand represents a command for backing up a Pilosa node.
type BackupCommand struct { // nolint: maligned
	tlsConfig *tls.Config

	// Destination host and port.
	Host string `json:"host"`

	// Path to write the backup to.
	OutputDir string

	// If true, skips file sync.
	NoSync bool

	// Reusable client.
	client pilosa.InternalClient

	// Standard input/output
	*pilosa.CmdIO

	TLS server.TLSConfig
}

// NewBackupCommand returns a new instance of BackupCommand.
func NewBackupCommand(stdin io.Reader, stdout, stderr io.Writer) *BackupCommand {
	return &BackupCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
	}
}

// Run executes the main program execution.
func (cmd *BackupCommand) Run(ctx context.Context) (err error) {
	// Validate arguments.
	if cmd.OutputDir == "" {
		return fmt.Errorf("-o flag required")
	}

	// Parse TLS configuration for node-specific clients.
	tls := cmd.TLSConfiguration()
	if cmd.tlsConfig, err = server.GetTLSConfig(&tls, cmd.Logger()); err != nil {
		return fmt.Errorf("parsing tls config: %w", err)
	}

	// Create a client to the server.
	client, err := commandClient(cmd)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}
	cmd.client = client

	// Determine the field type in order to correctly handle the input data.
	indexes, err := cmd.client.Schema(ctx)
	if err != nil {
		return fmt.Errorf("getting schema: %w", err)
	}
	schema := &pilosa.Schema{Indexes: indexes}

	// Ensure output directory doesn't exist; then create output directory.
	if _, err := os.Stat(cmd.OutputDir); !os.IsNotExist(err) {
		return fmt.Errorf("output directory already exists")
	} else if err := os.MkdirAll(cmd.OutputDir, 0777); err != nil {
		return err
	}

	// Backup schema.
	if err := cmd.backupSchema(ctx, schema); err != nil {
		return fmt.Errorf("cannot back up schema: %w", err)
	} else if err := cmd.backupIDAllocData(ctx); err != nil {
		return fmt.Errorf("cannot back up id alloc data: %w", err)
	}

	// Backup data for each index.
	for _, ii := range schema.Indexes {
		if err := cmd.backupIndex(ctx, ii); err != nil {
			return err
		}
	}

	return nil
}

// backupSchema writes the schema to the archive.
func (cmd *BackupCommand) backupSchema(ctx context.Context, schema *pilosa.Schema) error {
	logger := cmd.Logger()
	logger.Printf("backing up schema")

	buf, err := json.MarshalIndent(schema, "", "\t")
	if err != nil {
		return fmt.Errorf("marshaling schema: %w", err)
	}

	if err := ioutil.WriteFile(filepath.Join(cmd.OutputDir, "schema"), buf, 0666); err != nil {
		return fmt.Errorf("writing schema: %w", err)
	}

	return nil
}

func (cmd *BackupCommand) backupIDAllocData(ctx context.Context) error {
	logger := cmd.Logger()
	logger.Printf("backing up id alloc data")

	rc, err := cmd.client.IDAllocDataReader(ctx)
	if err != nil {
		return fmt.Errorf("fetching id alloc data reader: %w", err)
	}
	defer rc.Close()

	f, err := os.Create(filepath.Join(cmd.OutputDir, "idalloc"))
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(f, rc); err != nil {
		return err
	} else if err := cmd.syncFile(f); err != nil {
		return err
	}
	return f.Close()
}

// backupIndex backs up all shards for a given index.
func (cmd *BackupCommand) backupIndex(ctx context.Context, ii *pilosa.IndexInfo) error {
	logger := cmd.Logger()
	logger.Printf("backing up index: %q", ii.Name)

	shards, err := cmd.client.AvailableShards(ctx, ii.Name)
	if err != nil {
		return fmt.Errorf("cannot find available shards for index %q: %w", ii.Name, err)
	}

	// Back up all bitmap data for the index.
	for _, shard := range shards {
		if err := cmd.backupShard(ctx, ii.Name, shard); err != nil {
			return fmt.Errorf("cannot backup shard %d on index %q: %w", shard, ii.Name, err)
		}
	}

	// Back up translation data after bitmap data so we ensure we can translate all data.
	if err := cmd.backupIndexTranslateData(ctx, ii.Name); err != nil {
		return err
	}

	// Back up field translation data.
	for _, fi := range ii.Fields {
		if err := cmd.backupFieldTranslateData(ctx, ii.Name, fi.Name); err != nil {
			return fmt.Errorf("cannot backup field translation data for field %q on index %q: %w", fi.Name, ii.Name, err)
		}
	}

	return nil
}

// backupShard backs up a single shard from a single index.
func (cmd *BackupCommand) backupShard(ctx context.Context, indexName string, shard uint64) (err error) {
	nodes, err := cmd.client.FragmentNodes(ctx, indexName, shard)
	if err != nil {
		return fmt.Errorf("cannot determine fragment nodes: %w", err)
	} else if len(nodes) == 0 {
		return fmt.Errorf("no nodes available")
	}

	for _, node := range nodes {
		if e := cmd.backupShardNode(ctx, indexName, shard, node); e == nil {
			return nil // backup ok, exit
		} else if err == nil {
			err = e // save first error, try next node
		}
	}
	return err
}

// backupShardNode backs up a single shard from a single index on a specific node.
func (cmd *BackupCommand) backupShardNode(ctx context.Context, indexName string, shard uint64, node *topology.Node) error {
	logger := cmd.Logger()
	logger.Printf("backing up shard: index=%q id=%d", indexName, shard)

	client := http.NewInternalClientFromURI(&node.URI, http.GetHTTPClient(cmd.tlsConfig))
	rc, err := client.ShardReader(ctx, indexName, shard)
	if err != nil {
		return fmt.Errorf("fetching shard reader: %w", err)
	}
	defer rc.Close()

	filename := filepath.Join(cmd.OutputDir, "indexes", indexName, "shards", fmt.Sprintf("%04d", shard))
	if err := os.MkdirAll(filepath.Dir(filename), 0777); err != nil {
		return err
	}

	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(f, rc); err != nil {
		return err
	} else if err := cmd.syncFile(f); err != nil {
		return err
	}
	return f.Close()
}

func (cmd *BackupCommand) backupIndexTranslateData(ctx context.Context, name string) error {
	// TODO: Fetch holder partition count.
	partitionN := topology.DefaultPartitionN
	for partitionID := 0; partitionID < partitionN; partitionID++ {
		if err := cmd.backupIndexPartitionTranslateData(ctx, name, partitionID); err != nil {
			return fmt.Errorf("cannot backup index translation data for partition %d on %q: %w", partitionID, name, err)
		}
	}
	return nil
}

func (cmd *BackupCommand) backupIndexPartitionTranslateData(ctx context.Context, name string, partitionID int) error {
	logger := cmd.Logger()
	logger.Printf("backing up index translation data: %s/%d", name, partitionID)

	rc, err := cmd.client.IndexTranslateDataReader(ctx, name, partitionID)
	if err == pilosa.ErrTranslateStoreNotFound {
		return nil
	} else if err != nil {
		return fmt.Errorf("fetching translate data reader: %w", err)
	}
	defer rc.Close()

	filename := filepath.Join(cmd.OutputDir, "indexes", name, "translate", fmt.Sprintf("%04d", partitionID))
	if err := os.MkdirAll(filepath.Dir(filename), 0777); err != nil {
		return err
	}

	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(f, rc); err != nil {
		return err
	} else if err := cmd.syncFile(f); err != nil {
		return err
	}
	return f.Close()
}

func (cmd *BackupCommand) backupFieldTranslateData(ctx context.Context, indexName, fieldName string) error {
	logger := cmd.Logger()
	logger.Printf("backing up field translation data: %s/%s", indexName, fieldName)

	rc, err := cmd.client.FieldTranslateDataReader(ctx, indexName, fieldName)
	if err == pilosa.ErrTranslateStoreNotFound {
		return nil
	} else if err != nil {
		return fmt.Errorf("fetching translate data reader: %w", err)
	}
	defer rc.Close()

	filename := filepath.Join(cmd.OutputDir, "indexes", indexName, "fields", fieldName, "translate")
	if err := os.MkdirAll(filepath.Dir(filename), 0777); err != nil {
		return err
	}

	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(f, rc); err != nil {
		return err
	} else if err := cmd.syncFile(f); err != nil {
		return err
	}
	return f.Close()
}

func (cmd *BackupCommand) syncFile(f *os.File) error {
	if cmd.NoSync {
		return nil
	}
	return f.Sync()
}

func (cmd *BackupCommand) TLSHost() string { return cmd.Host }

func (cmd *BackupCommand) TLSConfiguration() server.TLSConfig { return cmd.TLS }
