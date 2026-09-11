CREATE SEQUENCE pydbrief.sq_database
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 65535
	START 1
	CACHE 1
	NO CYCLE;

CREATE TABLE pydbrief.database (
	id int4 DEFAULT nextval('sq_database'::regclass) NOT NULL,
	cd_engine varchar(64) NOT NULL,
	cd_name varchar(64) NOT NULL,
	cd_type varchar(10) NOT NULL,
	ds_driver varchar(128),
	ds_version varchar(128),
	nm_client varchar(128),
	nm_host varchar(64) NOT NULL,
	nm_pwd varchar(64) NOT NULL,
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



CREATE SEQUENCE pydbrief.sq_s3
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 65535
	START 1
	CACHE 1
	NO CYCLE;

CREATE TABLE pydbrief.s3 (
	id int4 DEFAULT nextval('sq_s3'::regclass) NOT NULL,
	cd_engine varchar(64) NOT NULL,
	cd_type varchar(10) NOT NULL,
	ds_endpoint_url varchar(256) NOT NULL,
	ds_version varchar(128),
	is_secure_acess bool DEFAULT false NOT NULL,
	nm_access_key varchar(64) NOT NULL,
	nm_bucket varchar(64) NOT NULL,
	nm_secret_key varchar(64) NOT NULL,
	CONSTRAINT ck_s3_type CHECK (((cd_type)::text = ANY (ARRAY[
      ('aws'::character varying)::text,
      ('minio'::character varying)::text]))),
	CONSTRAINT pk_s3 PRIMARY KEY (id),
	CONSTRAINT uk_s3 UNIQUE (cd_engine)
);



CREATE SEQUENCE pydbrief.sq_session
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;

CREATE TABLE pydbrief.session (
	id int8 DEFAULT nextval('sq_session'::regclass) NOT NULL,
	cd_session varchar(64) NOT NULL,
	id_source_db int4 NOT NULL,
	id_target_db int4 NOT NULL,
	id_target_s3 int4,
	ts_creation timestamp NOT NULL,
    CONSTRAINT fk_session_source_db FOREIGN KEY (id_source_db) REFERENCES pydbrief.database(id),
    CONSTRAINT fk_session_target_db FOREIGN KEY (id_target_db) REFERENCES pydbrief.database(id,
    CONSTRAINT fk_session_target_s3 FOREIGN KEY (id_target_s3) REFERENCES pydbrief.s3(id),
	CONSTRAINT pk_session PRIMARY KEY (id),
	CONSTRAINT uk_session UNIQUE (cd_session)
);



CREATE SEQUENCE pydbrief.sq_migration
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;

CREATE TABLE pydbrief.migration (
	id int8 DEFAULT nextval('sq_migration'::regclass) NOT NULL,
	cd_step varchar(2) NOT NULL,
	id_session int8 NOT NULL,
	nm_badge varchar(64) NOT NULL,
    nr_batch_size_in int8 NOT NULL,
    nr_batch_size_out int8 NOT NULL,
    nr_chunk_size int8 NOT NULL,
    nr_chunk_incremental int8 NOT NULL,
	ts_start timestamp,
	ts_finish timestamp,
	CONSTRAINT ck_migration_step CHECK (((cd_step)::text = ANY (ARRAY[
      ('CL'::character varying)::text, ('CP'::character varying)::text,
      ('ML'::character varying)::text, ('MM'::character varying)::text,
      ('MP'::character varying)::text, ('SL'::character varying)::text,
      ('SP'::character varying)::text]))),
    CONSTRAINT fk_migration_session FOREIGN KEY (id_session) REFERENCES pydbrief.session(id),
	CONSTRAINT pk_migration PRIMARY KEY (id),
	CONSTRAINT uk_migration UNIQUE (nm_badge)
);



CREATE SEQUENCE pydbrief.sq_migration_spec
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;

CREATE TABLE pydbrief.migration_spec (
	id int8 DEFAULT nextval('sq_migration_spec'::regclass) NOT NULL,
	id_migration int8 NOT NULL,
	cd_spec varchar(64) NOT NULL,
    vl_spec varchar(256),
	CONSTRAINT ck_migration_specp CHECK (((cd_spec)::text = ANY (ARRAY[
      ('exclude-columns'::character varying)::text,
      ('exclude-relations'::character varying)::text,
      ('flatten-storage'::character varying)::text,
      ('from-schema'::character varying)::text,
      ('include-relations'::character varying)::text,
      ('incremental-migrations'::character varying)::text,
      ('named-lobdata'::character varying)::text,
      ('omit-defaults'::character varying)::text,
      ('optimize-pks'::character varying)::text,
      ('override-columns'::character varying)::text,
      ('process-indexes'::character varying)::text,
      ('process-views'::character varying)::text,
      ('reflect-filetype'::character varying)::text,
      ('relax-reflection'::character varying)::text,
      ('remove-ctrlchars'::character varying)::text,
      ('skip-nonempty'::character varying)::text,
      ('to-schema'::character varying)::text]))),
    CONSTRAINT fk_migration_spec_migration FOREIGN KEY (id_migration) REFERENCES pydbrief.migration(id),
	CONSTRAINT pk_migration_spec PRIMARY KEY (id),
	CONSTRAINT uk_migration_spec UNIQUE (id_migration, cd_spec)



CREATE SEQUENCE pydbrief.sq_migration_table
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;

CREATE TABLE pydbrief.migration_table (
	id int8 DEFAULT nextval('sq_migration_table'::regclass) NOT NULL,
	id_migration int8 NOT NULL,
	nm_table varchar(64) NOT NULL,
    nr_batch_size_in int8 NOT NULL,
    nr_batch_size_out int8 NOT NULL,
    nr_chunk_size int8 NOT NULL,
    nr_chunk_incremental int8 NOT NULL,
	ts_start timestamp,
	ts_finish timestamp,
    CONSTRAINT fk_migration_table_migration FOREIGN KEY (id_migration) REFERENCES pydbrief.migration(id),
	CONSTRAINT pk_migration_table PRIMARY KEY (id),
	CONSTRAINT uk_migration_table UNIQUE (id_migration, nm_table)
);



CREATE SEQUENCE pydbrief.sq_migration_span
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;

CREATE TABLE pydbrief.migration_span (
	id int8 DEFAULT nextval('sq_migration_span'::regclass) NOT NULL,
	id_migration_table int8 NOT NULL,
    nr_first_row int8 not null,
    nr_last_row int8 NOT NULL,
    CONSTRAINT fk_migration_span_migration_table FOREIGN KEY (id_migration_table) REFERENCES pydbrief.migration_table(id),
	CONSTRAINT pk_migration_span PRIMARY KEY (id),
	CONSTRAINT uk_migration_span UNIQUE (id_migration_table, nr_first_row)
);




