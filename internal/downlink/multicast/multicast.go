package multicast

import (
	"crypto/rand"
	"encoding/binary"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/brocaar/loraserver/api/gw"
	"github.com/brocaar/loraserver/internal/config"
	"github.com/brocaar/loraserver/internal/framelog"
	"github.com/brocaar/loraserver/internal/storage"
	"github.com/brocaar/lorawan"
)

var errAbort = errors.New("")

type multicastContext struct {
	Token          uint16
	DB             sqlx.Ext
	MulticastGroup storage.MulticastGroup
	QueueItem      storage.MulticastQueueItem
	TXInfo         gw.TXInfo
	PHYPayload     lorawan.PHYPayload
}

var multicastTasks = []func(*multicastContext) error{
	setToken,
	getNextQueueItem,
	removeQueueItem,
	validatePayloadSize,
	setTXInfo,
	setPHYPayload,
	sendDownlinkData,
	logDownlinkFrameForGateway,
}

// HandleScheduleNextQueueItem handles the scheduling of the next queue-item
// for the given multicast-group.
func HandleScheduleNextQueueItem(db sqlx.Ext, mg storage.MulticastGroup) error {
	ctx := multicastContext{
		DB:             db,
		MulticastGroup: mg,
	}

	for _, t := range multicastTasks {
		if err := t(&ctx); err != nil {
			if err == errAbort {
				return nil
			}
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

func getNextQueueItem(ctx *multicastContext) error {
	var err error
	ctx.QueueItem, err = storage.GetNextMulticastQueueItemForMulticastGroup(ctx.DB, ctx.MulticastGroup.ID)
	if err != nil {
		return errors.Wrap(err, "get next multicast queue-item error")
	}

	return nil
}

func removeQueueItem(ctx *multicastContext) error {
	if err := storage.DeleteMulticastQueueItem(ctx.DB, ctx.QueueItem.ID); err != nil {
		return errors.Wrap(err, "delete multicast queue-item error")
	}

	return nil
}

func validatePayloadSize(ctx *multicastContext) error {
	maxSize, err := config.C.NetworkServer.Band.Band.GetMaxPayloadSizeForDataRateIndex("", "", ctx.MulticastGroup.DR)
	if err != nil {
		return errors.Wrap(err, "get max payload-size for data-rate index error")
	}

	if len(ctx.QueueItem.FRMPayload) > maxSize.N {
		log.WithFields(log.Fields{
			"multicast_group_id": ctx.MulticastGroup.ID,
			"dr":                 ctx.MulticastGroup.DR,
			"max_frm_payload_size": maxSize.N,
			"frm_payload_size":     len(ctx.QueueItem.FRMPayload),
		}).Error("payload exceeds max size for data-rate")

		return errAbort
	}

	return nil
}

func setTXInfo(ctx *multicastContext) error {
	txInfo := gw.TXInfo{
		MAC:         ctx.QueueItem.GatewayID,
		Immediately: ctx.QueueItem.EmitAtTimeSinceGPSEpoch == nil,
		Frequency:   ctx.MulticastGroup.Frequency,
	}

	if ctx.QueueItem.EmitAtTimeSinceGPSEpoch != nil {
		gpsEpoch := gw.Duration(*ctx.QueueItem.EmitAtTimeSinceGPSEpoch)
		txInfo.TimeSinceGPSEpoch = &gpsEpoch
	}

	var err error
	txInfo.DataRate, err = config.C.NetworkServer.Band.Band.GetDataRate(ctx.MulticastGroup.DR)
	if err != nil {
		return errors.Wrap(err, "get data-rate error")
	}

	if config.C.NetworkServer.NetworkSettings.DownlinkTXPower != -1 {
		txInfo.Power = config.C.NetworkServer.NetworkSettings.DownlinkTXPower
	} else {
		txInfo.Power = config.C.NetworkServer.Band.Band.GetDownlinkTXPower(txInfo.Frequency)
	}

	// will be refactored
	txInfo.CodeRate = "4/5"
	ctx.TXInfo = txInfo

	return nil
}

func setPHYPayload(ctx *multicastContext) error {
	ctx.PHYPayload = lorawan.PHYPayload{
		MHDR: lorawan.MHDR{
			MType: lorawan.UnconfirmedDataDown,
			Major: lorawan.LoRaWANR1,
		},
		MACPayload: &lorawan.MACPayload{
			FHDR: lorawan.FHDR{
				DevAddr: ctx.MulticastGroup.MCAddr,
				FCnt:    ctx.QueueItem.FCnt,
			},
			FPort:      &ctx.QueueItem.FPort,
			FRMPayload: []lorawan.Payload{&lorawan.DataPayload{Bytes: ctx.QueueItem.FRMPayload}},
		},
	}

	// using LoRaWAN1_0 vs LoRaWAN1_1 only makes a difference when setting the
	// confirmed frame-counter
	if err := ctx.PHYPayload.SetDownlinkDataMIC(lorawan.LoRaWAN1_1, 0, ctx.MulticastGroup.MCNetSKey); err != nil {
		return errors.Wrap(err, "set downlink data mic error")
	}

	return nil
}

func sendDownlinkData(ctx *multicastContext) error {
	if err := config.C.NetworkServer.Gateway.Backend.Backend.SendTXPacket(gw.TXPacket{
		Token:      ctx.Token,
		TXInfo:     ctx.TXInfo,
		PHYPayload: ctx.PHYPayload,
	}); err != nil {
		return errors.Wrap(err, "send downlink frame to gateway error")
	}

	return nil
}

func logDownlinkFrameForGateway(ctx *multicastContext) error {
	downlinkFrame, err := framelog.CreateDownlinkFrame(ctx.Token, ctx.PHYPayload, ctx.TXInfo)
	if err != nil {
		errors.Wrap(err, "create downlink frame error")
	}

	if err := framelog.LogDownlinkFrameForGateway(downlinkFrame); err != nil {
		log.WithError(err).Error("log downlink frame for gateway error")
	}

	return nil
}
