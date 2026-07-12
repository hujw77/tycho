-- Schedule the cleanup function to run daily at 12:30 AM (after partition pruning at midnight)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
        PERFORM cron.schedule('clean_transaction_table', '30 0 * * *', 'SELECT clean_transaction_table();');
    ELSE
        RAISE NOTICE 'Skipping clean_transaction_table cron rollback schedule because pg_cron is not installed in %',
            current_database();
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Skipping clean_transaction_table cron rollback schedule in database %: %',
            current_database(),
            SQLERRM;
END;
$$;
