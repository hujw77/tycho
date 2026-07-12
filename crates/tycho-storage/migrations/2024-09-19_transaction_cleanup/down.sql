DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
        PERFORM cron.unschedule('clean_transaction_table');
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Skipping clean_transaction_table cron removal in database %: %',
            current_database(),
            SQLERRM;
END;
$$;

DROP FUNCTION IF EXISTS clean_transaction_table();

DROP INDEX IF EXISTS idx_contract_code_modify_tx;
DROP INDEX IF EXISTS idx_protocol_component_creation_tx;
DROP INDEX IF EXISTS idx_protocol_component_deletion_tx;
DROP INDEX IF EXISTS idx_account_creation_tx;
DROP INDEX IF EXISTS idx_account_deletion_tx;
DROP INDEX IF EXISTS idx_account_balance_modify_tx;
DROP INDEX IF EXISTS idx_component_balance_modify_tx;
DROP INDEX IF EXISTS idx_protocol_state_modify_tx;
DROP INDEX IF EXISTS idx_contract_storage_modify_tx;
