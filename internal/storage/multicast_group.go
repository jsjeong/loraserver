package storage

import (
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	uuid "github.com/satori/go.uuid"

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
	MulticastGroupID uuid.UUID `db:"multicast_group_id"`
	FCnt             uint32    `db:"f_cnt"`
	CreatedAt        time.Time `db:"created_at"`
	FPort            uint8     `db:"f_port"`
	FRMPayload       []byte    `db:"frm_payload"`
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
func GetMulticastGroup(db sqlx.Queryer, id uuid.UUID) (MulticastGroup, error) {
	var mg MulticastGroup
	err := sqlx.Get(db, &mg, `
		select
			*
		from
			multicast_group
		where
			id = $1`,
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
			frm_payload
		) values ($1, $2, $3, $4, $5)
		`,
		qi.CreatedAt,
		qi.MulticastGroupID,
		qi.FCnt,
		qi.FPort,
		qi.FRMPayload,
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
