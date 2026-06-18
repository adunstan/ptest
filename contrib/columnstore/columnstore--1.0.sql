/* contrib/columnstore/columnstore--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION columnstore" to load this file. \quit

CREATE FUNCTION columnstore_tableam_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE ACCESS METHOD columnstore TYPE TABLE HANDLER columnstore_tableam_handler;
COMMENT ON ACCESS METHOD columnstore IS 'columnar storage table access method';

--
-- Event trigger to set autovacuum defaults on columnstore tables.
--
-- The columnstore delta store is a write buffer that should be compacted
-- into columnar row groups of CS_ROWS_PER_ROWGROUP (100,000) rows.
-- PostgreSQL's default autovacuum_vacuum_insert_scale_factor (0.2) makes
-- the delta grow proportionally with table size, which hurts read
-- performance on large tables.  We want a fixed compaction cadence
-- aligned with the row group size, so we set:
--   autovacuum_vacuum_insert_threshold = 100000
--   autovacuum_vacuum_insert_scale_factor = 0
-- on every CREATE TABLE that uses USING columnstore, unless the user
-- explicitly specified those options in WITH (...).
--
CREATE FUNCTION columnstore_set_autovacuum_defaults()
RETURNS event_trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    obj record;
    am_oid oid;
    has_threshold bool;
    has_scale bool;
BEGIN
    SELECT oid INTO am_oid FROM pg_am WHERE amname = 'columnstore';
    -- Only creation commands: applying the defaults on ALTER TABLE would
    -- re-impose them after a user's ALTER TABLE ... RESET (...) of these
    -- options, and would re-fire this trigger on its own SET subcommands.
    -- A table converted to columnstore with ALTER TABLE ... SET ACCESS
    -- METHOD therefore does not get the defaults automatically; set them
    -- explicitly if wanted.
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands()
                WHERE command_tag IN ('CREATE TABLE', 'CREATE TABLE AS',
                                      'SELECT INTO')
    LOOP
        -- Partitioned parents may carry the AM (it is inherited by future
        -- partitions) but cannot take storage parameters themselves; each
        -- leaf partition fires this trigger on its own CREATE.
        IF (SELECT relam FROM pg_class
            WHERE oid = obj.objid AND relkind <> 'p') = am_oid THEN
            -- Check which of the two options the user already set, parsing
            -- reloptions with pg_options_to_table rather than matching the
            -- raw 'name=value' text.  LEFT JOIN LATERAL keeps the pg_class
            -- row when reloptions is NULL (no options), so the COALESCEd
            -- bool_or yields false and the defaults are applied.
            SELECT
                COALESCE(bool_or(t.option_name =
                                 'autovacuum_vacuum_insert_threshold'), false),
                COALESCE(bool_or(t.option_name =
                                 'autovacuum_vacuum_insert_scale_factor'), false)
                INTO has_threshold, has_scale
                FROM pg_class c
                LEFT JOIN LATERAL pg_options_to_table(c.reloptions) AS t ON true
                WHERE c.oid = obj.objid;

            IF NOT has_threshold THEN
                EXECUTE format(
                    'ALTER TABLE %s SET (autovacuum_vacuum_insert_threshold = 100000)',
                    obj.objid::regclass
                );
            END IF;
            IF NOT has_scale THEN
                EXECUTE format(
                    'ALTER TABLE %s SET (autovacuum_vacuum_insert_scale_factor = 0)',
                    obj.objid::regclass
                );
            END IF;
        END IF;
    END LOOP;
END;
$$;

CREATE EVENT TRIGGER columnstore_auto_vacuum_defaults
    ON ddl_command_end
    WHEN TAG IN ('CREATE TABLE', 'CREATE TABLE AS', 'SELECT INTO')
    EXECUTE FUNCTION columnstore_set_autovacuum_defaults();
