package api

import (
	"context"
	"testing"

	"github.com/brocaar/loraserver/internal/storage"
	"github.com/brocaar/lorawan"

	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/brocaar/loraserver/api/ns"
	"github.com/brocaar/loraserver/internal/test"
)

type NetworkServerAPITestSuite struct {
	suite.Suite
	test.DatabaseTestSuiteBase

	api ns.NetworkServerServiceServer
}

func (ts *NetworkServerAPITestSuite) SetupSuite() {
	ts.DatabaseTestSuiteBase.SetupSuite()
	ts.api = NewNetworkServerAPI()
}

func (ts *NetworkServerAPITestSuite) TestMulticastGroup() {
	mg := ns.MulticastGroup{
		McAddr:         []byte{1, 2, 3, 4},
		McNetSKey:      []byte{1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8},
		FCnt:           10,
		GroupType:      ns.MulticastGroupType_CLASS_B,
		Dr:             5,
		Frequency:      868300000,
		PingSlotPeriod: 16,
	}

	ts.T().Run("Create", func(t *testing.T) {
		assert := require.New(t)
		createResp, err := ts.api.CreateMulticastGroup(context.Background(), &ns.CreateMulticastGroupRequest{
			MulticastGroup: &mg,
		})
		assert.Nil(err)
		assert.Len(createResp.Id, 16)
		assert.NotEqual(uuid.Nil.Bytes(), createResp.Id)
		mg.Id = createResp.Id

		t.Run("Get", func(t *testing.T) {
			assert := require.New(t)
			getResp, err := ts.api.GetMulticastGroup(context.Background(), &ns.GetMulticastGroupRequest{
				Id: createResp.Id,
			})
			assert.Nil(err)
			assert.NotNil(getResp.MulticastGroup)
			assert.NotNil(getResp.CreatedAt)
			assert.NotNil(getResp.UpdatedAt)
			assert.Equal(&mg, getResp.MulticastGroup)
		})

		t.Run("Update", func(t *testing.T) {
			assert := require.New(t)

			mgUpdated := ns.MulticastGroup{
				Id:             createResp.Id,
				McAddr:         []byte{4, 3, 2, 1},
				McNetSKey:      []byte{8, 7, 6, 5, 4, 3, 2, 1, 8, 7, 6, 5, 4, 3, 2, 1},
				FCnt:           20,
				GroupType:      ns.MulticastGroupType_CLASS_C,
				Dr:             3,
				Frequency:      868100000,
				PingSlotPeriod: 32,
			}

			_, err := ts.api.UpdateMulticastGroup(context.Background(), &ns.UpdateMulticastGroupRequest{
				MulticastGroup: &mgUpdated,
			})
			assert.Nil(err)

			getResp, err := ts.api.GetMulticastGroup(context.Background(), &ns.GetMulticastGroupRequest{
				Id: createResp.Id,
			})
			assert.Nil(err)
			assert.Equal(&mgUpdated, getResp.MulticastGroup)
		})

		t.Run("Delete", func(t *testing.T) {
			assert := require.New(t)

			_, err := ts.api.DeleteMulticastGroup(context.Background(), &ns.DeleteMulticastGroupRequest{
				Id: createResp.Id,
			})
			assert.Nil(err)

			_, err = ts.api.DeleteMulticastGroup(context.Background(), &ns.DeleteMulticastGroupRequest{
				Id: createResp.Id,
			})
			assert.NotNil(err)
			assert.Equal(codes.NotFound, grpc.Code(err))

			_, err = ts.api.GetMulticastGroup(context.Background(), &ns.GetMulticastGroupRequest{
				Id: createResp.Id,
			})
			assert.NotNil(err)
			assert.Equal(codes.NotFound, grpc.Code(err))
		})
	})
}

func (ts *NetworkServerAPITestSuite) TestDevice() {
	assert := require.New(ts.T())

	rp := storage.RoutingProfile{}
	assert.NoError(storage.CreateRoutingProfile(ts.DB(), &rp))

	sp := storage.ServiceProfile{}
	assert.NoError(storage.CreateServiceProfile(ts.DB(), &sp))

	dp := storage.DeviceProfile{}
	assert.NoError(storage.CreateDeviceProfile(ts.DB(), &dp))

	devEUI := lorawan.EUI64{1, 2, 3, 4, 5, 6, 7, 8}

	ts.T().Run("Create", func(t *testing.T) {
		assert := require.New(t)

		d := &ns.Device{
			DevEui:           devEUI[:],
			DeviceProfileId:  dp.ID.Bytes(),
			ServiceProfileId: sp.ID.Bytes(),
			RoutingProfileId: rp.ID.Bytes(),
			SkipFCntCheck:    true,
		}

		_, err := ts.api.CreateDevice(context.Background(), &ns.CreateDeviceRequest{
			Device: d,
		})
		assert.NoError(err)

		t.Run("Get", func(t *testing.T) {
			assert := require.New(t)

			getResp, err := ts.api.GetDevice(context.Background(), &ns.GetDeviceRequest{
				DevEui: devEUI[:],
			})
			assert.NoError(err)
			assert.Equal(d, getResp.Device)
		})

		t.Run("Update", func(t *testing.T) {
			assert := require.New(t)

			rp2 := storage.RoutingProfile{}
			assert.NoError(storage.CreateRoutingProfile(ts.DB(), &rp2))

			d.RoutingProfileId = rp2.ID.Bytes()
			_, err := ts.api.UpdateDevice(context.Background(), &ns.UpdateDeviceRequest{
				Device: d,
			})
			assert.NoError(err)

			getResp, err := ts.api.GetDevice(context.Background(), &ns.GetDeviceRequest{
				DevEui: devEUI[:],
			})
			assert.NoError(err)
			assert.Equal(d, getResp.Device)
		})

		t.Run("Multicast-groups", func(t *testing.T) {
			assert := require.New(t)

			mg1 := storage.MulticastGroup{}
			assert.NoError(storage.CreateMulticastGroup(ts.DB(), &mg1))

			t.Run("Add", func(t *testing.T) {
				_, err := ts.api.AddDeviceToMulticastGroup(context.Background(), &ns.AddDeviceToMulticastGroupRequest{
					DevEui:           devEUI[:],
					MulticastGroupId: mg1.ID.Bytes(),
				})
				assert.NoError(err)

				t.Run("List", func(t *testing.T) {
					assert := require.New(t)

					listResp, err := ts.api.GetMulticastGroupsForDevice(context.Background(), &ns.GetMulticastGroupsForDeviceRequest{
						DevEui: devEUI[:],
					})
					assert.NoError(err)
					assert.Equal([]*ns.DeviceMulticastGroup{
						{
							MulticastGroupId: mg1.ID.Bytes(),
						},
					}, listResp.MulticastGroups)
				})

				t.Run("Remove", func(t *testing.T) {
					assert := require.New(t)

					_, err := ts.api.RemoveDeviceFromMulticastGroup(context.Background(), &ns.RemoveDeviceFromMulticastGroupRequest{
						DevEui:           devEUI[:],
						MulticastGroupId: mg1.ID.Bytes(),
					})
					assert.NoError(err)

					_, err = ts.api.RemoveDeviceFromMulticastGroup(context.Background(), &ns.RemoveDeviceFromMulticastGroupRequest{
						DevEui:           devEUI[:],
						MulticastGroupId: mg1.ID.Bytes(),
					})
					assert.Error(err)
					assert.Equal(codes.NotFound, grpc.Code(err))
				})
			})
		})

		t.Run("Delete", func(t *testing.T) {
			assert := require.New(t)

			_, err := ts.api.DeleteDevice(context.Background(), &ns.DeleteDeviceRequest{
				DevEui: devEUI[:],
			})
			assert.NoError(err)

			_, err = ts.api.DeleteDevice(context.Background(), &ns.DeleteDeviceRequest{
				DevEui: devEUI[:],
			})
			assert.Error(err)
			assert.Equal(codes.NotFound, grpc.Code(err))
		})
	})
}

func TestNetworkServerAPINew(t *testing.T) {
	suite.Run(t, new(NetworkServerAPITestSuite))
}
