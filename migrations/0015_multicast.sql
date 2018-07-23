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

-- +migrate Down
drop table device_multicast_group;

drop table multicast_group;
