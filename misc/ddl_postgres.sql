DROP TABLE migration_span;
DROP TABLE migration_work;
DROP TABLE migration_table;
DROP TABLE migration_issue;
DROP TABLE migration;
DROP TABLE session;
DROP TABLE s3;
DROP TABLE database;

DROP SEQUENCE sq_database;
DROP SEQUENCE sq_s3;
DROP SEQUENCE sq_session;
DROP SEQUENCE sq_migration;
DROP SEQUENCE sq_migration_issue;
DROP SEQUENCE sq_migration_table;
DROP SEQUENCE sq_migration_span;
DROP SEQUENCE sq_migration_work;


CREATE SEQUENCE sq_database
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 65535
	START 1
	CACHE 1
	NO CYCLE;

CREATE TABLE database (
	id int4 DEFAULT nextval('sq_database'::regclass) NOT NULL,
	bn_pwd bytea NOT NULL,
	cd_engine varchar(64) NOT NULL,
	cd_name varchar(64) NOT NULL,
	cd_type varchar(10) NOT NULL,
	ds_driver varchar(128),
	ds_version varchar(128),
	nm_client varchar(128),
	nm_host varchar(64) NOT NULL,
	nm_user varchar(64) NOT NULL,
	nr_port int4 NOT NULL,
	CONSTRAINT ck_database_type CHECK (((cd_type)::text = ANY (ARRAY[
      ('mysql'::character varying)::text,
      ('oracle'::character varying)::text,
      ('postgres'::character varying)::text,
      ('sqlserver'::character varying)::text]))),
	CONSTRAINT pk_database PRIMARY KEY (id),
	CONSTRAINT uk_database UNIQUE (cd_engine)
);


CREATE SEQUENCE sq_s3
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 65535
	START 1
	CACHE 1
	NO CYCLE;

CREATE TABLE s3 (
	id int4 DEFAULT nextval('sq_s3'::regclass) NOT NULL,
	bn_secret_key bytea NOT NULL,
	cd_engine varchar(64) NOT NULL,
	cd_type varchar(10) NOT NULL,
	ds_endpoint_url varchar(256) NOT NULL,
	ds_version varchar(128),
	is_secure_access bool DEFAULT false NOT NULL,
	nm_access_key varchar(64) NOT NULL,
	nm_bucket varchar(64) NOT NULL,
	nm_region varchar(64),
	CONSTRAINT ck_s3_type CHECK (((cd_type)::text = ANY (ARRAY[
      ('aws'::character varying)::text,
      ('gcs'::character varying)::text,
      ('minio'::character varying)::text]))),
	CONSTRAINT pk_s3 PRIMARY KEY (id),
	CONSTRAINT uk_s3 UNIQUE (cd_engine)
);


CREATE SEQUENCE sq_session
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;

CREATE TABLE session (
	id int8 DEFAULT nextval('sq_session'::regclass) NOT NULL,
	cd_session varchar(64) NOT NULL,
    cd_state varchar(1) NOT NULL,
	id_source_db int4 NOT NULL,
	id_target_db int4 NOT NULL,
	id_target_s3 int4,
    nm_source_schema varchar(16) NOT NULL,
    nm_target_schema varchar(16) NOT NULL,
	ts_creation timestamp NOT NULL,
	CONSTRAINT ck_session_state CHECK (((cd_state)::text = ANY (ARRAY[
      ('C'::character varying)::text,
      ('S'::character varying)::text,
      ('F'::character varying)::text]))),
    CONSTRAINT fk_session_source_db FOREIGN KEY (id_source_db) REFERENCES database(id),
    CONSTRAINT fk_session_target_db FOREIGN KEY (id_target_db) REFERENCES database(id),
    CONSTRAINT fk_session_target_s3 FOREIGN KEY (id_target_s3) REFERENCES s3(id),
	CONSTRAINT pk_session PRIMARY KEY (id),
	CONSTRAINT uk_session UNIQUE (cd_session)
);


CREATE SEQUENCE sq_migration
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;

CREATE TABLE migration (
	id int8 DEFAULT nextval('sq_migration'::regclass) NOT NULL,
	cd_step varchar(2) NOT NULL,
	id_session int8 NOT NULL,
	nm_badge varchar(64) NOT NULL,
    ds_exclude_relations varchar(256),
    ds_include_relations varchar(256),
    is_flatten_storage bool,
    is_optimize_pks bool,
    is_process_indexes bool,
    is_process_views bool,
    is_reflect_filetype bool,
    is_relax_reflection bool,
    is_skip_nonempty bool,
    nr_lobdata_channels int2,
    nr_lobdata_channel_size int8,
    nr_plaindata_channels int2,
    nr_plaindata_channel_size int8,
	ts_start timestamp,
	ts_finish timestamp,
	CONSTRAINT ck_migration_step CHECK (((cd_step)::text = ANY (ARRAY[
      ('CL'::character varying)::text, ('CP'::character varying)::text,
      ('ML'::character varying)::text, ('MM'::character varying)::text,
      ('MP'::character varying)::text, ('SP'::character varying)::text]))),
    CONSTRAINT ck_migration_lobdata_channels CHECK (nr_lobdata_channels >= 0),
    CONSTRAINT ck_migration_lobdata_channel_size CHECK (nr_lobdata_channel_size >= 0),
    CONSTRAINT ck_migration_plaindata_channels CHECK (nr_plaindata_channels >= 0),
    CONSTRAINT ck_migration_plaindata_channel_size CHECK (nr_plaindata_channel_size >= 0),
    CONSTRAINT fk_migration_session FOREIGN KEY (id_session) REFERENCES session(id),
	CONSTRAINT pk_migration PRIMARY KEY (id),
	CONSTRAINT uk_migration_1 UNIQUE (nm_badge),
	CONSTRAINT uk_migration_2 UNIQUE (id_session, cd_step)
);


CREATE SEQUENCE sq_migration_issue
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;

CREATE TABLE migration_issue (
	id int8 DEFAULT nextval('sq_migration_issue'::regclass) NOT NULL,
	id_migration int8 NOT NULL,
    cd_type varchar(1) NOT NULL,
	ds_issue varchar(2048) NOT NULL,
	ts_onset timestamp NOT NULL,
	CONSTRAINT ck_migration_issue CHECK (((cd_type)::text = ANY (ARRAY[
      ('C'::character varying)::text,
	  ('E'::character varying)::text,
      ('W'::character varying)::text]))),
    CONSTRAINT fk_migration_issue_migration FOREIGN KEY (id_migration) REFERENCES migration(id),
	CONSTRAINT pk_migration_issue PRIMARY KEY (id)
);


CREATE SEQUENCE sq_migration_table
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;

CREATE TABLE migration_table (
	id int8 DEFAULT nextval('sq_migration_table'::regclass) NOT NULL,
	id_migration int8 NOT NULL,
	nm_table varchar(64) NOT NULL,
    ds_exclude_columns varchar(256),
    ds_exclude_constraints varchar(256),
    ds_named_lobdata varchar(4000),
    ds_omit_defaults varchar(256),
    ds_override_columns varchar(256),
    ds_remove_ctrlchars varchar(256),
    nr_batch_size_in int8,
    nr_batch_size_out int8,
    nr_chunk_size int8,
    nr_incremental_count int8,
    nr_incremental_offset int8,
    CONSTRAINT ck_migration_table_batch_size_in CHECK (nr_batch_size_in >= 0),
    CONSTRAINT ck_migration_table_batch_size_out CHECK (nr_batch_size_out >= 0),
    CONSTRAINT ck_migration_table_chunk_size CHECK (nr_chunk_size >= 0),
    CONSTRAINT ck_migration_table_incremental_count CHECK (nr_incremental_count >= 0),
    CONSTRAINT ck_migration_table_incremental_offset CHECK (nr_incremental_count >= 0),
    CONSTRAINT fk_migration_table_migration FOREIGN KEY (id_migration) REFERENCES migration(id),
	CONSTRAINT pk_migration_table PRIMARY KEY (id),
	CONSTRAINT uk_migration_table UNIQUE (id_migration, nm_table)
);


CREATE SEQUENCE sq_migration_work
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;

CREATE TABLE migration_work (
	id int8 DEFAULT nextval('sq_migration_work'::regclass) NOT NULL,
	id_migration int8 NOT NULL,
	nm_table varchar(64) NOT NULL,
	ts_start timestamp,
	ts_finish timestamp,
    CONSTRAINT fk_migration_work_migration FOREIGN KEY (id_migration) REFERENCES migration(id),
	CONSTRAINT pk_migration_work PRIMARY KEY (id),
	CONSTRAINT uk_migration_work UNIQUE (id_migration, nm_table)
);


CREATE SEQUENCE sq_migration_span
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;

CREATE TABLE migration_span (
	id int8 DEFAULT nextval('sq_migration_span'::regclass) NOT NULL,
	id_migration_work int8 NOT NULL,
    is_done bool DEFAULT false NOT NULL,
    nr_first_row int8 NOT NULL,
    nr_last_row int8 NOT NULL,
    CONSTRAINT ck_migration_span CHECK (nr_first_row >= 0 AND nr_last_row >= 0 AND nr_last_row >= nr_first_row),
    CONSTRAINT fk_migration_span_table FOREIGN KEY (id_migration_work) REFERENCES migration_work(id),
	CONSTRAINT pk_migration_span PRIMARY KEY (id),
	CONSTRAINT uk_migration_span UNIQUE (id_migration_work, nr_first_row)
);
