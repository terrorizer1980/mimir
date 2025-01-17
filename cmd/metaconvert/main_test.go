// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"fmt"
	"path"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

func TestConvertTenantBlocks(t *testing.T) {
	dir := t.TempDir()
	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: dir})
	require.NoError(t, err)

	ctx := context.Background()

	blockWithNoLabelsButManyOtherFields := ulid.MustNew(1, nil)
	blockWithWrongTenant := ulid.MustNew(2, nil)
	blockWithManyMimirLabels := ulid.MustNew(3, nil)
	blockWithNoChangesRequired := ulid.MustNew(4, nil)

	const tenant = "target_tenant"

	inputMetas := map[ulid.ULID]metadata.Meta{
		blockWithNoLabelsButManyOtherFields: {
			BlockMeta: tsdb.BlockMeta{
				ULID: blockWithNoLabelsButManyOtherFields,

				MinTime: 100,
				MaxTime: 200,
				Version: 1,

				Compaction: tsdb.BlockMetaCompaction{
					Level:   5,
					Sources: []ulid.ULID{blockWithNoLabelsButManyOtherFields},
				},
			},

			Thanos: metadata.Thanos{
				Version: 10,
				Downsample: metadata.ThanosDownsample{
					Resolution: 15,
				},
				Source: "ingester",
			},
		},

		blockWithWrongTenant: {
			BlockMeta: tsdb.BlockMeta{
				ULID: blockWithWrongTenant,
			},

			Thanos: metadata.Thanos{
				Labels: map[string]string{
					"test":                           "label",
					mimir_tsdb.TenantIDExternalLabel: "wrong tenant",
				},
			},
		},

		blockWithManyMimirLabels: {
			BlockMeta: tsdb.BlockMeta{
				ULID: blockWithManyMimirLabels,
			},

			Thanos: metadata.Thanos{
				Labels: map[string]string{
					mimir_tsdb.TenantIDExternalLabel:         "fake",
					mimir_tsdb.CompactorShardIDExternalLabel: "1_of_10",
					mimir_tsdb.IngesterIDExternalLabel:       "ingester-1",
				},
			},
		},

		blockWithNoChangesRequired: {
			BlockMeta: tsdb.BlockMeta{
				ULID: blockWithNoChangesRequired,
			},

			Thanos: metadata.Thanos{
				Labels: map[string]string{
					mimir_tsdb.TenantIDExternalLabel: tenant,
				},
			},
		},
	}

	for b, m := range inputMetas {
		require.NoError(t, uploadMetadata(ctx, bkt, m, path.Join(b.String(), metadata.MetaFilename)))
	}

	logs := &concurrency.SyncBuffer{}
	logger := log.NewLogfmtLogger(logs)

	// Run conversion
	assert.NoError(t, convertTenantBlocks(ctx, bkt, tenant, false, logger))

	expected := map[ulid.ULID]metadata.Meta{
		blockWithNoLabelsButManyOtherFields: {
			BlockMeta: tsdb.BlockMeta{
				ULID: blockWithNoLabelsButManyOtherFields,

				MinTime: 100,
				MaxTime: 200,
				Version: 1,

				Compaction: tsdb.BlockMetaCompaction{
					Level:   5,
					Sources: []ulid.ULID{blockWithNoLabelsButManyOtherFields},
				},
			},

			Thanos: metadata.Thanos{
				Version: 10,
				Labels: map[string]string{
					mimir_tsdb.TenantIDExternalLabel: tenant,
				},
				Downsample: metadata.ThanosDownsample{
					Resolution: 15,
				},
				Source: "ingester",
			},
		},

		blockWithWrongTenant: {
			BlockMeta: tsdb.BlockMeta{
				ULID: blockWithWrongTenant,
			},

			Thanos: metadata.Thanos{
				Labels: map[string]string{
					mimir_tsdb.TenantIDExternalLabel: tenant,
				},
			},
		},

		blockWithManyMimirLabels: {
			BlockMeta: tsdb.BlockMeta{
				ULID: blockWithManyMimirLabels,
			},

			Thanos: metadata.Thanos{
				Labels: map[string]string{
					mimir_tsdb.TenantIDExternalLabel:         tenant,
					mimir_tsdb.CompactorShardIDExternalLabel: "1_of_10",
					mimir_tsdb.IngesterIDExternalLabel:       "ingester-1",
				},
			},
		},

		blockWithNoChangesRequired: {
			BlockMeta: tsdb.BlockMeta{
				ULID: blockWithNoChangesRequired,
			},

			Thanos: metadata.Thanos{
				Labels: map[string]string{
					mimir_tsdb.TenantIDExternalLabel: tenant,
				},
			},
		},
	}

	for b, m := range expected {
		meta, err := block.DownloadMeta(ctx, logger, bkt, b)
		require.NoError(t, err)
		require.Equal(t, m, meta)
	}

	assert.Equal(t, []string{
		`level=warn tenant=target_tenant msg="updating tenant label" block=00000000010000000000000000 old_value= new_value=target_tenant`,
		`level=info tenant=target_tenant msg="changes required, uploading meta.json file" block=00000000010000000000000000`,
		`level=info tenant=target_tenant msg="meta.json file uploaded successfully" block=00000000010000000000000000`,
		`level=warn tenant=target_tenant msg="updating tenant label" block=00000000020000000000000000 old_value="wrong tenant" new_value=target_tenant`,
		`level=warn tenant=target_tenant msg="removing unknown label" block=00000000020000000000000000 label=test value=label`,
		`level=info tenant=target_tenant msg="changes required, uploading meta.json file" block=00000000020000000000000000`,
		`level=info tenant=target_tenant msg="meta.json file uploaded successfully" block=00000000020000000000000000`,
		`level=warn tenant=target_tenant msg="updating tenant label" block=00000000030000000000000000 old_value=fake new_value=target_tenant`,
		`level=info tenant=target_tenant msg="changes required, uploading meta.json file" block=00000000030000000000000000`,
		`level=info tenant=target_tenant msg="meta.json file uploaded successfully" block=00000000030000000000000000`,
		`level=info tenant=target_tenant msg="no changes required" block=00000000040000000000000000`,
	}, strings.Split(strings.TrimSpace(logs.String()), "\n"))
}

func TestConvertTenantBlocksDryMode(t *testing.T) {
	dir := t.TempDir()
	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: dir})
	require.NoError(t, err)

	ctx := context.Background()

	blockWithNoLabelsButManyOtherFields := ulid.MustNew(1, nil)
	blockWithWrongTenant := ulid.MustNew(2, nil)
	blockWithManyMimirLabels := ulid.MustNew(3, nil)
	blockWithNoChangesRequired := ulid.MustNew(4, nil)

	const tenant = "target_tenant"

	inputMetas := map[ulid.ULID]metadata.Meta{
		blockWithNoLabelsButManyOtherFields: {
			BlockMeta: tsdb.BlockMeta{
				ULID: blockWithNoLabelsButManyOtherFields,

				MinTime: 100,
				MaxTime: 200,
				Version: 1,

				Compaction: tsdb.BlockMetaCompaction{
					Level:   5,
					Sources: []ulid.ULID{blockWithNoLabelsButManyOtherFields},
				},
			},

			Thanos: metadata.Thanos{
				Version: 10,
				Downsample: metadata.ThanosDownsample{
					Resolution: 15,
				},
				Source: "ingester",
			},
		},

		blockWithWrongTenant: {
			BlockMeta: tsdb.BlockMeta{
				ULID: blockWithWrongTenant,
			},

			Thanos: metadata.Thanos{
				Labels: map[string]string{
					"test":                           "label",
					mimir_tsdb.TenantIDExternalLabel: "wrong tenant",
				},
			},
		},

		blockWithManyMimirLabels: {
			BlockMeta: tsdb.BlockMeta{
				ULID: blockWithManyMimirLabels,
			},

			Thanos: metadata.Thanos{
				Labels: map[string]string{
					mimir_tsdb.TenantIDExternalLabel:         "fake",
					mimir_tsdb.CompactorShardIDExternalLabel: "1_of_10",
					mimir_tsdb.IngesterIDExternalLabel:       "ingester-1",
				},
			},
		},

		blockWithNoChangesRequired: {
			BlockMeta: tsdb.BlockMeta{
				ULID: blockWithNoChangesRequired,
			},

			Thanos: metadata.Thanos{
				Labels: map[string]string{
					mimir_tsdb.TenantIDExternalLabel: tenant,
				},
			},
		},
	}

	for b, m := range inputMetas {
		require.NoError(t, uploadMetadata(ctx, bkt, m, path.Join(b.String(), metadata.MetaFilename)))
	}

	logs := &concurrency.SyncBuffer{}
	logger := log.NewLogfmtLogger(logs)

	// Run conversion
	assert.NoError(t, convertTenantBlocks(ctx, bkt, tenant, true, logger))

	for b, m := range inputMetas {
		meta, err := block.DownloadMeta(ctx, logger, bkt, b)
		require.NoError(t, err)
		require.Equal(t, m, meta)
	}

	fmt.Println(logs.String())

	assert.Equal(t, []string{
		`level=warn tenant=target_tenant msg="updating tenant label" block=00000000010000000000000000 old_value= new_value=target_tenant`,
		`level=warn tenant=target_tenant msg="changes required, not uploading back due to dry run" block=00000000010000000000000000`,
		`level=warn tenant=target_tenant msg="updating tenant label" block=00000000020000000000000000 old_value="wrong tenant" new_value=target_tenant`,
		`level=warn tenant=target_tenant msg="removing unknown label" block=00000000020000000000000000 label=test value=label`,
		`level=warn tenant=target_tenant msg="changes required, not uploading back due to dry run" block=00000000020000000000000000`,
		`level=warn tenant=target_tenant msg="updating tenant label" block=00000000030000000000000000 old_value=fake new_value=target_tenant`,
		`level=warn tenant=target_tenant msg="changes required, not uploading back due to dry run" block=00000000030000000000000000`,
		`level=info tenant=target_tenant msg="no changes required" block=00000000040000000000000000`,
	}, strings.Split(strings.TrimSpace(logs.String()), "\n"))
}
