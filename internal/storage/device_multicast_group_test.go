package storage

import (
	"testing"

	"github.com/satori/go.uuid"

	"github.com/brocaar/lorawan"
	"github.com/stretchr/testify/require"
)

func (ts *StorageTestSuite) TestDeviceMulticastGroup() {
	assert := require.New(ts.T())

	mg := MulticastGroup{
		GroupType: MulticastGroupB,
	}
	assert.Nil(CreateMulticastGroup(ts.Tx(), &mg))

	sp := ServiceProfile{}
	dp := DeviceProfile{}
	rp := RoutingProfile{}

	assert.Nil(CreateServiceProfile(ts.Tx(), &sp))
	assert.Nil(CreateDeviceProfile(ts.Tx(), &dp))
	assert.Nil(CreateRoutingProfile(ts.Tx(), &rp))

	d := Device{
		DevEUI:           lorawan.EUI64{1, 2, 3, 4, 5, 6, 7, 8},
		ServiceProfileID: sp.ID,
		DeviceProfileID:  dp.ID,
		RoutingProfileID: rp.ID,
	}
	assert.Nil(CreateDevice(ts.Tx(), &d))

	ts.T().Run("Add", func(t *testing.T) {
		assert := require.New(t)

		assert.Nil(AddDeviceToMulticastGroup(ts.Tx(), d.DevEUI, mg.ID))

		t.Run("Get multicast-groups for DevEUI", func(t *testing.T) {
			assert := require.New(t)

			groups, err := GetMulticastGroupsForDevEUI(ts.Tx(), d.DevEUI)
			assert.Nil(err)
			assert.Len(groups, 1)
			assert.Equal([]uuid.UUID{mg.ID}, groups)
		})

		t.Run("Get DevEUIs for multicast-group", func(t *testing.T) {
			assert := require.New(t)

			devEUIs, err := GetDevEUIsForMulticastGroup(ts.Tx(), mg.ID)
			assert.NoError(err)
			assert.Len(devEUIs, 1)
			assert.Equal(d.DevEUI, devEUIs[0])
		})

		t.Run("Remove", func(t *testing.T) {
			assert := require.New(t)

			assert.Nil(RemoveDeviceFromMulticastGroup(ts.Tx(), d.DevEUI, mg.ID))
			groups, err := GetMulticastGroupsForDevEUI(ts.Tx(), d.DevEUI)
			assert.Nil(err)
			assert.Len(groups, 0)
		})
	})
}
