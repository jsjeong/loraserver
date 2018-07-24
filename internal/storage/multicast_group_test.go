package storage

import (
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/stretchr/testify/require"

	"github.com/brocaar/lorawan"
)

func (ts *StorageTestSuite) GetMulticastGroup() MulticastGroup {
	return MulticastGroup{
		MCAddr:         lorawan.DevAddr{1, 2, 3, 4},
		MCNetSKey:      lorawan.AES128Key{1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8},
		FCnt:           10,
		GroupType:      MulticastGroupB,
		DR:             5,
		Frequency:      868300000,
		PingSlotPeriod: 16,
	}
}

func (ts *StorageTestSuite) TestMulticastGroup() {

	ts.T().Run("Create", func(t *testing.T) {
		assert := require.New(t)

		mc := ts.GetMulticastGroup()
		err := CreateMulticastGroup(ts.Tx(), &mc)
		assert.Nil(err)

		mc.CreatedAt = mc.CreatedAt.Round(time.Second).UTC()
		mc.UpdatedAt = mc.UpdatedAt.Round(time.Second).UTC()

		t.Run("Get", func(t *testing.T) {
			assert := require.New(t)
			mcGet, err := GetMulticastGroup(ts.Tx(), mc.ID, false)
			assert.Nil(err)

			mcGet.CreatedAt = mcGet.CreatedAt.Round(time.Second).UTC()
			mcGet.UpdatedAt = mcGet.UpdatedAt.Round(time.Second).UTC()

			assert.Equal(mc, mcGet)
		})

		t.Run("Update", func(t *testing.T) {
			assert := require.New(t)

			mc.MCAddr = lorawan.DevAddr{4, 3, 2, 1}
			mc.MCNetSKey = lorawan.AES128Key{8, 7, 6, 5, 4, 3, 2, 1, 8, 7, 6, 5, 4, 3, 2, 1}
			mc.FCnt = 20
			mc.GroupType = MulticastGroupC
			mc.Frequency = 868100000
			mc.PingSlotPeriod = 32

			assert.Nil(UpdateMulticastGroup(ts.Tx(), &mc))

			mc.UpdatedAt = mc.UpdatedAt.Round(time.Second).UTC()

			mcGet, err := GetMulticastGroup(ts.Tx(), mc.ID, false)
			assert.Nil(err)

			mcGet.CreatedAt = mcGet.CreatedAt.Round(time.Second).UTC()
			mcGet.UpdatedAt = mcGet.UpdatedAt.Round(time.Second).UTC()

			assert.Equal(mc, mcGet)
		})

		t.Run("Delete", func(t *testing.T) {
			assert := require.New(t)

			assert.Nil(DeleteMulticastGroup(ts.Tx(), mc.ID))
			assert.Equal(ErrDoesNotExist, DeleteMulticastGroup(ts.Tx(), mc.ID))

			_, err := GetMulticastGroup(ts.Tx(), mc.ID, false)
			assert.Equal(ErrDoesNotExist, err)
		})
	})
}

func (ts *StorageTestSuite) TestMulticastQueue() {
	assert := require.New(ts.T())

	mg := ts.GetMulticastGroup()
	assert.NoError(CreateMulticastGroup(ts.Tx(), &mg))

	ts.T().Run("Create", func(t *testing.T) {
		assert := require.New(t)

		gps1 := 100 * time.Second
		gps2 := 110 * time.Second

		qi1 := MulticastQueueItem{
			MulticastGroupID:        mg.ID,
			FCnt:                    10,
			FPort:                   20,
			FRMPayload:              []byte{1, 2, 3, 4},
			EmitAtTimeSinceGPSEpoch: &gps1,
		}

		qi2 := MulticastQueueItem{
			MulticastGroupID:        mg.ID,
			FCnt:                    11,
			FPort:                   20,
			FRMPayload:              []byte{1, 2, 3, 4},
			EmitAtTimeSinceGPSEpoch: &gps2,
		}

		assert.NoError(CreateMulticastQueueItem(ts.Tx(), &qi1))
		assert.NoError(CreateMulticastQueueItem(ts.Tx(), &qi2))

		t.Run("List", func(t *testing.T) {
			assert := require.New(t)

			items, err := GetMulticastQueueItemsForMulticastGroup(ts.Tx(), mg.ID)
			assert.NoError(err)
			assert.Len(items, 2)

			assert.EqualValues(items[0].FCnt, 10)
			assert.EqualValues(items[1].FCnt, 11)
		})

		t.Run("Next queue item", func(t *testing.T) {
			assert := require.New(t)

			nqi, err := GetNextMulticastQueueItemForMulticastGroup(ts.Tx(), mg.ID)
			assert.NoError(err)

			assert.Equal(qi1.FCnt, nqi.FCnt)
		})

		t.Run("Max emit at", func(t *testing.T) {
			assert := require.New(t)

			d, err := GetMaxEmitAtTimeSinceGPSEpochForMulticastGroup(ts.Tx(), mg.ID)
			assert.NoError(err)
			assert.Equal(gps2, d)
		})

		t.Run("Delete", func(t *testing.T) {
			assert := require.New(t)

			assert.NoError(DeleteMulticastQueueItem(ts.Tx(), mg.ID, 10))
			items, err := GetMulticastQueueItemsForMulticastGroup(ts.Tx(), mg.ID)
			assert.NoError(err)
			assert.Len(items, 1)
		})

		t.Run("Flush", func(t *testing.T) {
			assert := require.New(t)

			assert.NoError(FlushMulticastQueueForMulticastGroup(ts.Tx(), mg.ID))
			items, err := GetMulticastQueueItemsForMulticastGroup(ts.Tx(), mg.ID)
			assert.NoError(err)
			assert.Len(items, 0)
		})
	})
}

func (ts *StorageTestSuite) TestGetMulticastGroupsWithQueueItems() {
	assert := require.New(ts.T())

	mg1 := ts.GetMulticastGroup()
	mg1.GroupType = MulticastGroupC
	assert.NoError(CreateMulticastGroup(ts.DB(), &mg1))
	mg2 := ts.GetMulticastGroup()
	mg2.GroupType = MulticastGroupC
	assert.NoError(CreateMulticastGroup(ts.DB(), &mg2))

	qi1 := MulticastQueueItem{
		MulticastGroupID: mg1.ID,
		FCnt:             10,
		FPort:            20,
		FRMPayload:       []byte{1, 2, 3, 4},
	}

	assert.NoError(CreateMulticastQueueItem(ts.DB(), &qi1))

	Transaction(ts.DB(), func(tx sqlx.Ext) error {
		groups, err := GetMulticastGroupsWithQueueItems(tx, 10)
		assert.NoError(err)
		assert.Len(groups, 1)

		// new transaction must return 0 items as the first one did lock
		// the multicast-group
		Transaction(ts.DB(), func(tx sqlx.Ext) error {
			groups, err := GetMulticastGroupsWithQueueItems(tx, 10)
			assert.NoError(err)
			assert.Len(groups, 0)
			return nil
		})

		return nil
	})
}
