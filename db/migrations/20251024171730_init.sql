-- +goose Up
-- +goose StatementBegin
create table coordinates (
	id bigserial not null primary key,
	user_id bigint not null,
    latitude float not null,
    longitude float not null,
	created_at timestamp with time zone not null
);
CREATE UNIQUE INDEX ix_coordinates_user_id_unique ON coordinates (user_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
drop index if exists ix_coordinates_user_id_unique;
drop table if exists coordinates;
-- +goose StatementEnd
