-- Schedule the cleanup function to run daily at 12:00 PM
-- This is to ensure that the cleanup function runs during team work hours and allows for timely reactions
-- to any issues it may cause with indexer DB interactions.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
        PERFORM cron.schedule('clean_transaction_table', '0 12 * * *', 'SELECT clean_transaction_table();');
    ELSE
        RAISE NOTICE 'Skipping clean_transaction_table cron reschedule because pg_cron is not installed in %',
            current_database();
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Skipping clean_transaction_table cron reschedule in database %: %',
            current_database(),
            SQLERRM;
END;
$$;
