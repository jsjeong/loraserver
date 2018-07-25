package multicast

import (
	"crypto/rand"
	"encoding/binary"

	"github.com/brocaar/loraserver/internal/config"
	"github.com/brocaar/loraserver/internal/storage"
	"github.com/brocaar/lorawan"
	"github.com/pkg/errors"
)

type multicastContext struct {
	// Multicast-group.
	MulticastGroup storage.MulticastGroup

	// Token defines a random token.
	Token uint16

	// Gateways defines the set of gateways to use for transmission.
	Gateways []lorawan.EUI64
}

var multicastTasks = []func(*multicastContext) error{
	setToken,
	setGateways,
}

// HandleScheduleNextQueueItem handles scheduling the next multicast queue-item
// for the given multicast-group.
func HandleScheduleNextQueueItem(mg storage.MulticastGroup) error {
	ctx := multicastContext{
		MulticastGroup: mg,
	}

	for _, t := range multicastTasks {
		if err := t(&ctx); err != nil {
			return err
		}
	}

	return nil
}

func setToken(ctx *multicastContext) error {
	b := make([]byte, 2)
	_, err := rand.Read(b)
	if err != nil {
		return errors.Wrap(err, "read random error")
	}
	ctx.Token = binary.BigEndian.Uint16(b)
	return nil
}

func setGateways(ctx *multicastContext) error {
	devEUIs, err := storage.GetDevEUIsForMulticastGroup(config.C.PostgreSQL.DB, ctx.MulticastGroup.ID)
	if err != nil {
		return errors.Wrap(err, "get deveuis for multicast-group error")
	}

	rxInfoSets, err := storage.GetDeviceGatewayRXInfoSetForDevEUIs(config.C.Redis.Pool, devEUIs)
	if err != nil {
		return errors.Wrap(err, "get device gateway rx-info set for deveuis errors")
	}

	ctx.Gateways, err = GetMinimumGatewaySet(rxInfoSets)
	if err != nil {
		return errors.Wrap(err, "get minimin gateway-set error")
	}

	return nil
}
