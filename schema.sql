CREATE TABLE IF NOT EXISTS locks (
	name CHARACTER VARYING(255) PRIMARY KEY,
	record_version_number BIGINT,
	data BYTEA,
	owner CHARACTER VARYING(255)
);
CREATE SEQUENCE IF NOT EXISTS locks_rvn CYCLE OWNED BY locks.record_version_number;
