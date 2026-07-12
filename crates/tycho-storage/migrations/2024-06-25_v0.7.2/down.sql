DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
        EXECUTE 'DROP EXTENSION IF EXISTS pg_cron';
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Skipping pg_cron teardown in database %: %', current_database(), SQLERRM;
END;
$$;
