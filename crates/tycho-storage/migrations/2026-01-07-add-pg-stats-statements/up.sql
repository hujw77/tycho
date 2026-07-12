DO $$
BEGIN
    EXECUTE 'CREATE EXTENSION IF NOT EXISTS pg_stat_statements';
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Skipping pg_stat_statements setup in database %: %', current_database(), SQLERRM;
END;
$$;
