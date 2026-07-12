DO $$
BEGIN
    IF current_database() = COALESCE(current_setting('cron.database_name', true), current_database()) THEN
        EXECUTE 'CREATE EXTENSION IF NOT EXISTS pg_cron';
        PERFORM cron.schedule('@daily', 'CALL partman.run_maintenance_proc()');
    ELSE
        RAISE NOTICE 'Skipping pg_cron setup in database % because cron.database_name targets %',
            current_database(),
            COALESCE(current_setting('cron.database_name', true), '<unset>');
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Skipping pg_cron setup in database %: %', current_database(), SQLERRM;
END;
$$;
