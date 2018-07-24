-- +migrate Up
create table multicast_group (
    id uuid primary key,
    created_at timestamp with time zone not null,
    updated_at timestamp with time zone not null,
    mc_addr bytea,
    mc_net_s_key bytea,
    f_cnt int not null,
    group_type char not null,
    dr int not null,
    frequency int not null,
    ping_slot_period int not null
);

create table device_multicast_group (
    dev_eui bytea references device on delete cascade,
    multicast_group_id uuid references multicast_group on delete cascade,
    created_at timestamp with time zone not null,

    primary key(multicast_group_id, dev_eui)
);

create table multicast_queue (
    multicast_group_id uuid not null references multicast_group,
    f_cnt int not null,
    created_at timestamp with time zone not null,
    f_port int not null,
    frm_payload bytea,
    emit_at_time_since_gps_epoch bigint,

    primary key(multicast_group_id, f_cnt)
);

create index idx_multicast_queue_emit_at_time_since_gps_epoch on multicast_queue(emit_at_time_since_gps_epoch);

-- +migrate Down
drop index idx_multicast_queue_emit_at_time_since_gps_epoch;

drop table multicast_queue;

drop table device_multicast_group;

drop table multicast_group;
