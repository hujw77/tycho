DO $$
BEGIN
    EXECUTE 'DROP EXTENSION IF EXISTS pg_stat_statements';
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Skipping pg_stat_statements teardown in database %: %', current_database(), SQLERRM;
END;
$$;
