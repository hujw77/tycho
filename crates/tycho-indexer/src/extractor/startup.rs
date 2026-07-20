use std::{collections::HashMap, slice};

use tracing::{info, instrument};
use tycho_common::{
    models::{
        blockchain::{Block, Transaction},
        contract::AccountDelta,
        Address, Chain, ExtractionState,
    },
    storage::{ChainGateway, ContractStateGateway, ExtractionStateGateway},
    traits::{AccountExtractor, StorageSnapshotRequest},
    Bytes,
};
use tycho_ethereum::{rpc::EthereumRpcClient, services::account_extractor::EVMAccountExtractor};
use tycho_storage::postgres::cache::CachedGateway;

use crate::extractor::runtime_target_planning::{ResolvedInitializedAccountsRequest, ResolvedRuntimeTargets};
use crate::extractor::{
    runtime_targets_startup::{BuiltManagedRunnersBatch, PreparedRuntimeTargetsStartup},
    ExtractionError,
};

pub use crate::extractor::runtime_targets_startup::ResolvedRuntimeTargetsBuildContext;

async fn with_transaction<F, Fut, R>(gw: &CachedGateway, block: &Block, f: F) -> R
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = R>,
{
    gw.start_transaction(block, Some("accountExtractor"))
        .await;
    let result = f().await;
    gw.commit_transaction(0)
        .await
        .expect("Failed to commit transaction");
    result
}

#[instrument(skip_all, fields(n_accounts = %accounts.len(), block_id = block_id))]
pub async fn initialize_accounts(
    accounts: Vec<Address>,
    block_id: u64,
    rpc: &EthereumRpcClient,
    chain: Chain,
    cached_gw: &CachedGateway,
) {
    if accounts.is_empty() {
        return;
    }
    let (block, extracted_accounts) = get_accounts_data(accounts, block_id, rpc, chain).await;

    info!(block_number = block.number, "Initializing accounts");

    let tx = Transaction {
        hash: Bytes::random(32),
        block_hash: block.hash.clone(),
        from: Bytes::from([0u8; 20]),
        to: None,
        index: 0,
    };

    with_transaction(cached_gw, &block, || async {
        cached_gw
            .upsert_block(slice::from_ref(&block))
            .await
            .expect("Failed to insert block");

        cached_gw
            .upsert_tx(slice::from_ref(&tx))
            .await
            .expect("Failed to insert tx");
    })
    .await;

    for account_update in extracted_accounts.into_values() {
        with_transaction(cached_gw, &block, || async {
            let new_account = account_update.ref_into_account(&tx);
            info!(block_number = block.number, contract_address = ?new_account.address, "NewContract");

            cached_gw
                .insert_contract(&new_account)
                .await
                .expect("Failed to insert contract");
            cached_gw
                .update_contracts(&[(tx.hash.clone(), account_update)])
                .await
                .expect("Failed to update contract");
        })
        .await;
    }

    with_transaction(cached_gw, &block, || async {
        let state = ExtractionState::new(
            "accountExtractor".to_string(),
            chain,
            None,
            "account_cursor".as_bytes(),
            block.hash.clone(),
        );

        cached_gw
            .save_state(&state)
            .await
            .expect("Failed to save cursor");
    })
    .await;
}

async fn get_accounts_data(
    accounts: Vec<Address>,
    block_id: u64,
    rpc: &EthereumRpcClient,
    chain: Chain,
) -> (Block, HashMap<Bytes, AccountDelta>) {
    let account_extractor = EVMAccountExtractor::new(rpc, chain);

    let block = account_extractor
        .get_block_data(block_id)
        .await
        .expect("Failed to get block data");

    let requests = accounts
        .iter()
        .map(|address| StorageSnapshotRequest { address: address.clone(), slots: None })
        .collect::<Vec<_>>();

    let extracted_accounts: HashMap<Bytes, AccountDelta> = account_extractor
        .get_accounts_at_block(&block, &requests)
        .await
        .expect("Failed to extract accounts");
    (block, extracted_accounts)
}

pub async fn initialize_runtime_target_accounts(
    requests: impl IntoIterator<Item = ResolvedInitializedAccountsRequest>,
    rpc: &EthereumRpcClient,
    cached_gw: &CachedGateway,
) {
    for request in requests {
        initialize_accounts(request.accounts, request.block_id, rpc, request.chain, cached_gw)
            .await;
    }
}

impl<'a> ResolvedRuntimeTargets<'a> {
    pub(crate) async fn prepare_startup(
        &self,
        context: &ResolvedRuntimeTargetsBuildContext<'_>,
    ) -> Result<PreparedRuntimeTargetsStartup, ExtractionError> {
        context.prepare_runtime_targets_startup(self).await
    }

    pub async fn build_managed_runners(
        self,
        context: ResolvedRuntimeTargetsBuildContext<'_>,
    ) -> Result<
        (
            Vec<crate::extractor::runner::ManagedRunner>,
            Vec<crate::extractor::control::ExtractorHandle>,
        ),
        ExtractionError,
    > {
        self.prepare_startup(&context)
            .await?
            .build_managed_runners()
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn build_managed_runners_batch(
        self,
        context: ResolvedRuntimeTargetsBuildContext<'_>,
    ) -> Result<BuiltManagedRunnersBatch, ExtractionError> {
        self.prepare_startup(&context)
            .await?
            .build_managed_runners_batch()
    }

    pub async fn initialize_accounts(&self, rpc: &EthereumRpcClient, cached_gw: &CachedGateway) {
        initialize_runtime_target_accounts(
            self.coalesced_initialized_accounts_requests(),
            rpc,
            cached_gw,
        )
        .await;
    }
}
