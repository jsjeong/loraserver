package storage

import (
	"testing"
	"time"

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
			mcGet, err := GetMulticastGroup(ts.Tx(), mc.ID)
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

			mcGet, err := GetMulticastGroup(ts.Tx(), mc.ID)
			assert.Nil(err)

			mcGet.CreatedAt = mcGet.CreatedAt.Round(time.Second).UTC()
			mcGet.UpdatedAt = mcGet.UpdatedAt.Round(time.Second).UTC()

			assert.Equal(mc, mcGet)
		})

		t.Run("Delete", func(t *testing.T) {
			assert := require.New(t)

			assert.Nil(DeleteMulticastGroup(ts.Tx(), mc.ID))
			assert.Equal(ErrDoesNotExist, DeleteMulticastGroup(ts.Tx(), mc.ID))

			_, err := GetMulticastGroup(ts.Tx(), mc.ID)
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

		qi1 := MulticastQueueItem{
			MulticastGroupID: mg.ID,
			FCnt:             10,
			FPort:            20,
			FRMPayload:       []byte{1, 2, 3, 4},
		}

		qi2 := MulticastQueueItem{
			MulticastGroupID: mg.ID,
			FCnt:             11,
			FPort:            20,
			FRMPayload:       []byte{1, 2, 3, 4},
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
