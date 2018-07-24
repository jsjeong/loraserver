package storage

import (
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	uuid "github.com/satori/go.uuid"

	"github.com/brocaar/loraserver/internal/config"
	"github.com/brocaar/loraserver/internal/gps"
	"github.com/brocaar/lorawan"
)

// MulticastGroupType type defines the multicast-group type.
type MulticastGroupType string

// Possible multicast-group types.
const (
	MulticastGroupB MulticastGroupType = "B"
	MulticastGroupC MulticastGroupType = "C"
)

// MulticastGroup defines a multicast-group.
type MulticastGroup struct {
	ID             uuid.UUID          `db:"id"`
	CreatedAt      time.Time          `db:"created_at"`
	UpdatedAt      time.Time          `db:"updated_at"`
	MCAddr         lorawan.DevAddr    `db:"mc_addr"`
	MCNetSKey      lorawan.AES128Key  `db:"mc_net_s_key"`
	FCnt           uint32             `db:"f_cnt"`
	GroupType      MulticastGroupType `db:"group_type"`
	DR             int                `db:"dr"`
	Frequency      int                `db:"frequency"`
	PingSlotPeriod int                `db:"ping_slot_period"`
}

// MulticastQueueItem defines a multicast queue-item.
type MulticastQueueItem struct {
	MulticastGroupID        uuid.UUID      `db:"multicast_group_id"`
	FCnt                    uint32         `db:"f_cnt"`
	CreatedAt               time.Time      `db:"created_at"`
	FPort                   uint8          `db:"f_port"`
	FRMPayload              []byte         `db:"frm_payload"`
	EmitAtTimeSinceGPSEpoch *time.Duration `db:"emit_at_time_since_gps_epoch"`
}

// Validate validates the MulticastQueueItem.
func (m MulticastQueueItem) Validate() error {
	if m.FPort == 0 {
		return ErrInvalidFPort
	}
	return nil
}

// CreateMulticastGroup creates the given multi-cast group.
func CreateMulticastGroup(db sqlx.Execer, mg *MulticastGroup) error {
	now := time.Now()
	mg.CreatedAt = now
	mg.UpdatedAt = now

	if mg.ID == uuid.Nil {
		mg.ID = uuid.NewV4()
	}

	_, err := db.Exec(`
		insert into multicast_group (
			id,
			created_at,
			updated_at,
			mc_addr,
			mc_net_s_key,
			f_cnt,
			group_type,
			dr,
			frequency,
			ping_slot_period
		) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		mg.ID,
		mg.CreatedAt,
		mg.UpdatedAt,
		mg.MCAddr[:],
		mg.MCNetSKey[:],
		mg.FCnt,
		mg.GroupType,
		mg.DR,
		mg.Frequency,
		mg.PingSlotPeriod,
	)
	if err != nil {
		return handlePSQLError(err, "insert error")
	}

	log.WithFields(log.Fields{
		"id": mg.ID,
	}).Info("multicast-group created")

	return nil
}

// GetMulticastGroup returns the multicast-group for the given ID.
func GetMulticastGroup(db sqlx.Queryer, id uuid.UUID, forUpdate bool) (MulticastGroup, error) {
	var mg MulticastGroup
	var fu string

	if forUpdate {
		fu = " for update"
	}

	err := sqlx.Get(db, &mg, `
		select
			*
		from
			multicast_group
		where
			id = $1`+fu,
		id,
	)
	if err != nil {
		return mg, handlePSQLError(err, "select error")
	}
	return mg, nil
}

// UpdateMulticastGroup updates the given multicast-grup.
func UpdateMulticastGroup(db sqlx.Execer, mg *MulticastGroup) error {
	mg.UpdatedAt = time.Now()

	res, err := db.Exec(`
		update
			multicast_group
		set
			updated_at = $2,
			mc_addr = $3,
			mc_net_s_key = $4,
			f_cnt = $5,
			group_type = $6,
			dr = $7,
			frequency = $8,
			ping_slot_period = $9
		where
			id = $1`,
		mg.ID,
		mg.UpdatedAt,
		mg.MCAddr[:],
		mg.MCNetSKey[:],
		mg.FCnt,
		mg.GroupType,
		mg.DR,
		mg.Frequency,
		mg.PingSlotPeriod,
	)
	if err != nil {
		return handlePSQLError(err, "update error")
	}
	ra, err := res.RowsAffected()
	if err != nil {
		return handlePSQLError(err, "get rows affected error")
	}
	if ra == 0 {
		return ErrDoesNotExist
	}

	log.WithFields(log.Fields{
		"id": mg.ID,
	}).Info("multicast-group updated")

	return nil
}

// DeleteMulticastGroup deletes the multicast-group matching the given ID.
func DeleteMulticastGroup(db sqlx.Execer, id uuid.UUID) error {
	res, err := db.Exec(`
		delete from
			multicast_group
		where
			id = $1`,
		id,
	)
	if err != nil {
		return handlePSQLError(err, "delete error")
	}

	ra, err := res.RowsAffected()
	if err != nil {
		return handlePSQLError(err, "get rows affected error")
	}
	if ra == 0 {
		return ErrDoesNotExist
	}

	log.WithFields(log.Fields{
		"id": id,
	}).Info("multicast-group deleted")

	return nil
}

// CreateMulticastQueueItem adds the given item to the queue.
func CreateMulticastQueueItem(db sqlx.Execer, qi *MulticastQueueItem) error {
	if err := qi.Validate(); err != nil {
		return err
	}

	qi.CreatedAt = time.Now()

	_, err := db.Exec(`
		insert into multicast_queue (
			created_at,
			multicast_group_id,
			f_cnt,
			f_port,
			frm_payload,
			emit_at_time_since_gps_epoch
		) values ($1, $2, $3, $4, $5, $6)
		`,
		qi.CreatedAt,
		qi.MulticastGroupID,
		qi.FCnt,
		qi.FPort,
		qi.FRMPayload,
		qi.EmitAtTimeSinceGPSEpoch,
	)
	if err != nil {
		return handlePSQLError(err, "insert error")
	}

	log.WithFields(log.Fields{
		"multicast_group_id": qi.MulticastGroupID,
		"f_cnt":              qi.FCnt,
	}).Info("multicast queue-item created")

	return nil
}

// DeleteMulticastQueueItem deletes the queue-item given an id.
func DeleteMulticastQueueItem(db sqlx.Execer, multicastGroupID uuid.UUID, fCnt uint32) error {
	res, err := db.Exec(`
		delete from
			multicast_queue
		where
			multicast_group_id = $1
			and f_cnt = $2
	`, multicastGroupID, fCnt)
	if err != nil {
		return handlePSQLError(err, "delete error")
	}
	ra, err := res.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "get rows affected error")
	}
	if ra == 0 {
		return ErrDoesNotExist
	}

	log.WithFields(log.Fields{
		"multicast_group_id": multicastGroupID,
		"f_cnt":              fCnt,
	}).Info("multicast queue-item deleted")

	return nil
}

// FlushMulticastQueueForMulticastGroup flushes the multicast-queue given
// a multicast-group id.
func FlushMulticastQueueForMulticastGroup(db sqlx.Execer, multicastGroupID uuid.UUID) error {
	res, err := db.Exec(`
		delete from
			multicast_queue
		where
			multicast_group_id = $1
	`, multicastGroupID)
	if err != nil {
		return handlePSQLError(err, "delete error")
	}
	ra, err := res.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "get rows affected error")
	}
	if ra == 0 {
		return ErrDoesNotExist
	}

	log.WithFields(log.Fields{
		"multicast_group_id": multicastGroupID,
	}).Info("multicast-group queue flushed")

	return nil
}

// GetMulticastQueueItemsForMulticastGroup returns all queue-items given
// a multicast-group id.
func GetMulticastQueueItemsForMulticastGroup(db sqlx.Queryer, multicastGroupID uuid.UUID) ([]MulticastQueueItem, error) {
	var items []MulticastQueueItem

	err := sqlx.Select(db, &items, `
		select
			*
		from
			multicast_queue
		where
			multicast_group_id = $1
		order by
			f_cnt
	`, multicastGroupID)
	if err != nil {
		return nil, handlePSQLError(err, "select error")
	}

	return items, nil
}

// GetMulticastGroupsWithQueueItems returns a slice of multicast-groups that
// contain queue items.
// The multicast-group records will be locked for update so that multiple
// instnaces can run this query in parallel without the rist of duplicate
// scheduling.
func GetMulticastGroupsWithQueueItems(db sqlx.Ext, count int) ([]MulticastGroup, error) {
	gpsEpochScheduleTime := gps.Time(time.Now().Add(config.SchedulerInterval * 2)).TimeSinceGPSEpoch()

	var multicastGroups []MulticastGroup
	err := sqlx.Select(db, &multicastGroups, `
		select
			mg.*
		from multicast_group mg
		where exists (
			select
				1
			from
				multicast_queue mq
			where
				mq.multicast_group_id = mg.id
				and (
					mg.group_type = 'C'
					or (
						mg.group_type = 'B'
						and mq.emit_at_time_since_gps_epoch <= $2
					)
				)
		)
		limit $1
		for update of mg skip locked
	`, count, gpsEpochScheduleTime)
	if err != nil {
		return nil, handlePSQLError(err, "select error")
	}
	return multicastGroups, nil
}

// GetNextMulticastQueueItemForMulticastGroup returns the next muticast
// queue-item given a multicast-group.
func GetNextMulticastQueueItemForMulticastGroup(db sqlx.Queryer, multicastGroupID uuid.UUID) (MulticastQueueItem, error) {
	var qi MulticastQueueItem
	err := sqlx.Get(db, &qi, `
		select
			*
		from
			multicast_queue
		where
			multicast_group_id = $1
		order by
			f_cnt
		limit 1
	`, multicastGroupID)
	if err != nil {
		return qi, handlePSQLError(err, "select error")
	}

	return qi, nil
}

// GetMaxEmitAtTimeSinceGPSEpochForMulticastGroup returns the maximum / last GPS
// epoch scheduling timestamp for the given multicast-group.
func GetMaxEmitAtTimeSinceGPSEpochForMulticastGroup(db sqlx.Queryer, multicastGroupID uuid.UUID) (time.Duration, error) {
	var timeSinceGPSEpoch time.Duration
	err := sqlx.Get(db, &timeSinceGPSEpoch, `
		select
			coalesce(max(emit_at_time_since_gps_epoch), 0)
		from
			multicast_queue
		where
			multicast_group_id = $1
	`, multicastGroupID)
	if err != nil {
		return 0, handlePSQLError(err, "select error")
	}

	return timeSinceGPSEpoch, nil
}
