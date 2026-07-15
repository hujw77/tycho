use std::{
    collections::{HashMap, HashSet},
    process,
};

#[cfg(test)]
use crate::config::ExtractorConfigs;
#[cfg(test)]
use crate::extractor::chain_state::ChainState;
#[cfg(test)]
use crate::extractor::family_bootstrap_registry::SharedBootstrapParamsParser;
#[cfg(test)]
use crate::extractor::family_registry::{
    default_family_runtime_registry, shared_bootstrap_member_runtime, shared_family_member_spec,
    shared_family_runtime_spec, FamilyMemberSpec, FamilyRuntimeRegistry, FamilyRuntimeSpec,
};
#[cfg(test)]
use crate::extractor::family_runtime_metadata::FamilyRuntimeConfig;
#[cfg(test)]
use crate::extractor::family_runtime_metadata::ResolvedSharedFamilyStream;
#[cfg(test)]
use crate::extractor::family_runtime_planning::DetectedFamilyRuntime;
#[cfg(test)]
use crate::extractor::runtime_target_planning::ResolvedRuntimeTarget;
#[cfg(test)]
use crate::extractor::shared_bootstrap::BootstrapBranchDescriptor;
#[cfg(test)]
use crate::extractor::startup::ResolvedRuntimeTargetsBuildContext;
#[cfg(test)]
use crate::extractor::{
    control::ExtractorHandle, extractor_config::BootstrapStrategy, models::BlockChanges,
    runner::ManagedRunner, ExtractionError,
};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use mockall::mock;
#[cfg(test)]
use prost::Message;
#[cfg(test)]
use serde::Deserialize;
#[cfg(test)]
use tycho_common::models::ImplementationType;
use tycho_common::{
    models::{
        blockchain::{
            Block, EntryPoint, EntryPointWithTracingParams, TracedEntryPoint, TracingParams,
            TracingResult, Transaction,
        },
        contract::{Account, AccountBalance, AccountDelta},
        protocol::{
            ComponentBalance, ProtocolComponent, ProtocolComponentState,
            ProtocolComponentStateDelta, QualityRange,
        },
        token::Token,
        Address, Chain, ComponentId, ContractId, EntryPointId, ExtractionState, PaginationParams,
        ProtocolType, TxHash,
    },
    storage::{
        BlockIdentifier, BlockOrTimestamp, ChainGateway, ContractStateGateway, EntryPointFilter,
        EntryPointGateway, ExtractionStateGateway, Gateway, ProtocolGateway, StorageError, Version,
        WithTotal,
    },
    Bytes,
};
#[cfg(test)]
use tycho_ethereum::rpc::EthereumRpcClient;
#[cfg(test)]
use tycho_ethereum::services::token_pre_processor::EthereumTokenPreProcessor;
#[cfg(test)]
use tycho_storage::postgres::cache::CachedGateway;

mock! {
    pub Gateway {}
    #[async_trait]
    impl ExtractionStateGateway for Gateway {
        async fn get_state(&self, name: &str, chain: &Chain) -> Result<ExtractionState, StorageError>;
        async fn save_state(&self, state: &ExtractionState) -> Result<(), StorageError>;
    }

    #[async_trait]
    impl ChainGateway for Gateway {
        async fn upsert_block(&self, new: &[Block]) -> Result<(), StorageError>;
        async fn get_block(&self, id: &BlockIdentifier) -> Result<Block, StorageError>;
        async fn upsert_tx(&self, new: &[Transaction]) -> Result<(), StorageError>;
        async fn get_tx(&self, hash: &TxHash) -> Result<Transaction, StorageError>;
        async fn revert_state(&self, to: &BlockIdentifier) -> Result<(), StorageError>;
    }

    impl EntryPointGateway for Gateway {
        #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
        fn insert_entry_points<'life0, 'life1, 'async_trait>(
            &'life0 self,
            entry_points: &'life1 HashMap<ComponentId, HashSet<EntryPoint>>,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<Output = Result<(), StorageError>>
                    + ::core::marker::Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait;

        #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
        fn insert_entry_point_tracing_params<'life0, 'life1, 'async_trait>(
            &'life0 self,
            entry_points_params: &'life1 HashMap<
                EntryPointId,
                HashSet<(TracingParams, ComponentId)>,
            >,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<Output = Result<(), StorageError>>
                    + ::core::marker::Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait;

        #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
        fn get_entry_points<'life0, 'life1, 'async_trait>(
            &'life0 self,
            filter: EntryPointFilter,
            pagination_params: Option<&'life1 PaginationParams>,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                        Output = Result<
                            WithTotal<HashMap<ComponentId, HashSet<EntryPoint>>>,
                            StorageError,
                        >,
                    > + ::core::marker::Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait;

        #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
        fn get_entry_points_tracing_params<'life0, 'life1, 'async_trait>(
            &'life0 self,
            filter: EntryPointFilter,
            pagination_params: Option<&'life1 PaginationParams>,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                        Output = Result<
                            WithTotal<HashMap<ComponentId, HashSet<EntryPointWithTracingParams>>>,
                            StorageError,
                        >,
                    > + ::core::marker::Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait;

        #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
        fn upsert_traced_entry_points<'life0, 'life1, 'async_trait>(
            &'life0 self,
            traced_entry_points: &'life1 [TracedEntryPoint],
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<Output = Result<(), StorageError>>
                    + ::core::marker::Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait;

        #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
        fn get_traced_entry_points<'life0, 'life1, 'async_trait>(
            &'life0 self,
            entry_points: &'life1 HashSet<EntryPointId>,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                        Output = Result<
                            HashMap<EntryPointId, HashMap<TracingParams, TracingResult>>,
                            StorageError,
                        >,
                    > + ::core::marker::Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait;
    }

    impl ContractStateGateway for Gateway {
        fn get_contract<'life0, 'life1, 'life2, 'async_trait>(
            &'life0 self,
            id: &'life1 ContractId,
            version: Option<&'life2 Version>,
            include_slots: bool,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<Account, StorageError>,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            Self: 'async_trait;

        #[allow(clippy::type_complexity)]
        fn get_contracts<'life0, 'life1, 'life2, 'life3, 'life4, 'async_trait>(
            &'life0 self,
            chain: &'life1 Chain,
            addresses: Option<&'life2 [Address]>,
            version: Option<&'life3 Version>,
            include_slots: bool,
            pagination_params: Option<&'life4 PaginationParams>,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<WithTotal<Vec<Account>>, StorageError>,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            'life3: 'async_trait,
            'life4: 'async_trait,
            Self: 'async_trait;

        fn insert_contract<'life0, 'life1, 'async_trait>(
            &'life0 self,
            new: &'life1 Account,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<(), StorageError>,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait;

        fn update_contracts<'life0, 'life1, 'async_trait>(
            &'life0 self,
            new: &'life1 [(TxHash, AccountDelta)],
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<(), StorageError>,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait;

        fn delete_contract<'life0, 'life1, 'life2, 'async_trait>(
            &'life0 self,
            id: &'life1 ContractId,
            at_tx: &'life2 TxHash,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<(), StorageError>,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            Self: 'async_trait;

        fn get_accounts_delta<'life0, 'life1, 'life2, 'life3, 'async_trait>(
            &'life0 self,
            chain: &'life1 Chain,
            start_version: Option<&'life2 BlockOrTimestamp>,
            end_version: &'life3 BlockOrTimestamp,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<Vec<AccountDelta>, StorageError>,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            'life3: 'async_trait,
            Self: 'async_trait;


        fn add_account_balances<'life0, 'life1, 'async_trait>(
            &'life0 self,
            account_balances: &'life1 [AccountBalance],
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<(), StorageError>,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait;

        #[allow(clippy::type_complexity)]
        fn get_account_balances<'life0, 'life1, 'life2, 'life3, 'async_trait>(
            &'life0 self,
            chain: &'life1 Chain,
            accounts: Option<&'life2 [Address]>,
            version: Option<&'life3 Version>,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<HashMap<Address, HashMap<Address, AccountBalance>>, StorageError>,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            'life3: 'async_trait,
            Self: 'async_trait;

    }

    impl ProtocolGateway for Gateway {
        #[allow(clippy::type_complexity)]
        fn get_protocol_components<'life0, 'life1, 'life2, 'life3, 'life4, 'async_trait>(
            &'life0 self,
            chain: &'life1 Chain,
            system: Option<String>,
            ids: Option<&'life2 [&'life3 str]>,
            min_tvl: Option<f64>,
            pagination_params: Option<&'life4 PaginationParams>,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<WithTotal<Vec<ProtocolComponent>>,
                        StorageError,
                    >,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            'life3: 'async_trait,
            'life4: 'async_trait,
            Self: 'async_trait;

        #[allow(clippy::type_complexity)]
        fn get_token_owners<'life0, 'life1, 'life2, 'async_trait>(
            &'life0 self,
            chain: &'life1 Chain,
            tokens: &'life2 [Address],
            min_balance: Option<f64>,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<
                        HashMap<Address, (ComponentId, Bytes)>,
                        StorageError,
                    >,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            Self: 'async_trait;

        fn add_protocol_components<'life0, 'life1, 'async_trait>(
            &'life0 self,
            new: &'life1 [ProtocolComponent],
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<(), StorageError>,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait;

        fn delete_protocol_components<'life0, 'life1, 'async_trait>(
            &'life0 self,
            to_delete: &'life1 [ProtocolComponent],
            block_ts: NaiveDateTime,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<(), StorageError>,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait;

        fn add_protocol_types<'life0, 'life1, 'async_trait>(
            &'life0 self,
            new_protocol_types: &'life1 [ProtocolType],
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<(), StorageError>,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait;

        #[allow(clippy::type_complexity)]
        fn get_protocol_states<'life0, 'life1, 'life2, 'life3, 'life4, 'async_trait>(
            &'life0 self,
            chain: &'life1 Chain,
            at: Option<Version>,
            system: Option<String>,
            ids: Option<&'life2 [&'life3 str]>,
            retrieve_balances: bool,
            pagination_params: Option<&'life4 PaginationParams>,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<WithTotal<Vec<ProtocolComponentState>>,
                        StorageError,
                    >,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            'life3: 'async_trait,
            'life4: 'async_trait,
            Self: 'async_trait;

        fn update_protocol_states<'life0, 'life1, 'async_trait>(
            &'life0 self,
            new: &'life1 [(TxHash, ProtocolComponentStateDelta)],
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<(), StorageError>,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait;

        #[allow(clippy::type_complexity)]
        fn get_tokens<'life0, 'life1, 'life2, 'life3, 'async_trait>(
            &'life0 self,
            chain: Chain,
            address: Option<&'life1 [&'life2 Address]>,
            quality: QualityRange,
            traded_n_days_ago: Option<NaiveDateTime>,
            pagination_params: Option<&'life3 PaginationParams>,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<WithTotal<Vec<Token>>, StorageError>,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            'life3: 'async_trait,
            Self: 'async_trait;

        fn add_component_balances<'life0, 'life1, 'async_trait>(
            &'life0 self,
            component_balances: &'life1 [ComponentBalance],
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<(), StorageError>,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait;

        fn add_tokens<'life0, 'life1, 'async_trait>(
            &'life0 self,
            tokens: &'life1 [Token],
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<(), StorageError>,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait;

        fn update_tokens<'life0, 'life1, 'async_trait>(
            &'life0 self,
            tokens: &'life1 [Token],
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<(), StorageError>,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait;

        fn get_protocol_states_delta<'life0, 'life1, 'life2, 'life3, 'async_trait>(
            &'life0 self,
            chain: &'life1 Chain,
            start_version: Option<&'life2 BlockOrTimestamp>,
            end_version: &'life3 BlockOrTimestamp,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<
                        Vec<ProtocolComponentStateDelta>,
                        StorageError,
                    >,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            'life3: 'async_trait,
            Self: 'async_trait;

        fn get_balance_deltas<'life0, 'life1, 'life2, 'life3, 'async_trait>(
            &'life0 self,
            chain: &'life1 Chain,
            start_version: Option<&'life2 BlockOrTimestamp>,
            target_version: &'life3 BlockOrTimestamp,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<
                        Vec<ComponentBalance>,
                        StorageError,
                    >,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            'life3: 'async_trait,
            Self: 'async_trait;

        #[allow(clippy::type_complexity)]
        fn get_component_balances<'life0, 'life1, 'life2, 'life3, 'life4, 'async_trait>(
            &'life0 self,
            chain: &'life1 Chain,
            ids: Option<&'life2 [&'life3 str]>,
            version: Option<&'life4 Version>,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<HashMap<String, HashMap<Bytes, ComponentBalance>>, StorageError>,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            'life3: 'async_trait,
            'life4: 'async_trait,
            Self: 'async_trait;

        #[allow(clippy::type_complexity)]
        fn get_token_prices<'life0, 'life1, 'async_trait>(
            &'life0 self,
            chain: &'life1 Chain,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<HashMap<Bytes, f64>, StorageError>,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait;

        fn upsert_component_tvl<'life0, 'life1, 'life2, 'async_trait>(
            &'life0 self,
            chain: &'life1 Chain,
            tvl_values: &'life2 HashMap<String, f64>,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<(), StorageError>,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            Self: 'async_trait;

        #[allow(clippy::type_complexity)]
        fn get_protocol_systems<'life0, 'life1, 'life2, 'async_trait>(
            &'life0 self,
            chain: &'life1 Chain,
            pagination_params: Option<&'life2 PaginationParams>,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<WithTotal<Vec<String>>, StorageError>,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            Self: 'async_trait;

        #[allow(clippy::type_complexity)]
        fn get_component_tvls<'life0, 'life1, 'life2, 'life3, 'life4, 'async_trait>(
            &'life0 self,
            chain: &'life1 Chain,
            system: Option<String>,
            ids: Option<&'life2 [&'life3 str]>,
            pagination_params: Option<&'life4 PaginationParams>,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<
                    Output = Result<WithTotal<HashMap<String, f64>>, StorageError>,
                > + ::core::marker::Send + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            'life3: 'async_trait,
            'life4: 'async_trait,
            Self: 'async_trait;
    }

    impl Gateway for Gateway {}
}

#[cfg(test)]
pub fn evm_contract_slots(data: impl IntoIterator<Item = (i32, i32)>) -> HashMap<Bytes, Bytes> {
    data.into_iter()
        .map(|(s, v)| {
            (Bytes::from(u32::try_from(s).unwrap()), Bytes::from(u32::try_from(v).unwrap()))
        })
        .collect()
}

/// Creates a block for testing, version 0 is not allowed and will panic.
#[cfg(test)]
pub fn block(version: u64) -> Block {
    if version == 0 {
        panic!("Block version 0 doesn't exist. Smallest version is 1");
    }

    let ts: NaiveDateTime = "2020-01-01T00:00:00"
        .parse()
        .expect("failed parsing block ts");
    Block::new(
        version,
        Chain::Ethereum,
        Bytes::from(version).lpad(32, 0),
        Bytes::from(version - 1).lpad(32, 0),
        ts + std::time::Duration::from_secs(version * 12),
    )
}

#[cfg(test)]
pub fn scripted_session_response(
    trace_prefix: &str,
    start_block: u64,
) -> crate::pb::sf::substreams::rpc::v2::Response {
    use crate::pb::sf::substreams::rpc::v2::{response::Message, Response, SessionInit};

    Response {
        message: Some(Message::Session(SessionInit {
            trace_id: format!("{trace_prefix}-{start_block}"),
            resolved_start_block: start_block,
            linear_handoff_block: start_block,
            max_parallel_workers: 1,
            attestation_public_key: String::new(),
            chain_head: start_block,
            blocks_to_process_before_start_block: 0,
            effective_blocks_to_process_before_start_block: 0,
            blocks_to_process_after_start_block: 0,
            effective_blocks_to_process_after_start_block: 0,
        })),
    }
}

#[cfg(test)]
pub fn scripted_undo_response(
    cursor_label: &str,
    last_valid_block: u64,
) -> crate::pb::sf::substreams::rpc::v2::Response {
    use crate::pb::sf::substreams::rpc::v2::{response::Message, BlockUndoSignal, Response};
    use crate::pb::sf::substreams::v1::BlockRef;

    let block_id = format!(
        "0x{}",
        std::iter::repeat(format!("{:02x}", last_valid_block as u8))
            .take(32)
            .collect::<String>()
    );
    Response {
        message: Some(Message::BlockUndoSignal(BlockUndoSignal {
            last_valid_block: Some(BlockRef { id: block_id, number: last_valid_block }),
            last_valid_cursor: format!("{cursor_label}@{last_valid_block}"),
        })),
    }
}

#[cfg(test)]
pub fn family_output_module_for_tests(family_name: &str) -> String {
    default_family_runtime_registry()
        .shared_runtime_metadata_for_family(family_name)
        .map(|metadata| metadata.output_module.to_string())
        .unwrap_or_else(|| format!("map_{family_name}_family_protocol_changes"))
}

#[cfg(test)]
pub fn family_shared_stream_name_for_tests(family_name: &str) -> String {
    default_family_runtime_registry()
        .shared_runtime_metadata_for_family(family_name)
        .map(|metadata| metadata.shared_stream_name.to_string())
        .unwrap_or_else(|| format!("{family_name}_family"))
}

#[cfg(test)]
pub fn family_durability_scope_for_tests(family_name: &str) -> String {
    default_family_runtime_registry()
        .shared_runtime_metadata_for_family(family_name)
        .map(|metadata| metadata.durability_scope.to_string())
        .unwrap_or_else(|| format!("family::{family_name}"))
}

#[cfg(test)]
pub fn family_shared_extractor_id_for_tests(family_name: &str, chain: Chain) -> String {
    default_family_runtime_registry()
        .shared_stream_identity_for_family(chain, family_name)
        .map(|identity| identity.extractor_id)
        .unwrap_or_else(|| format!("{chain}:{}", family_shared_stream_name_for_tests(family_name)))
}

#[cfg(test)]
pub fn family_member_protocol_systems_for_tests(family_name: &str) -> Vec<String> {
    default_family_runtime_registry()
        .member_protocol_systems_for_family(family_name)
        .unwrap_or_else(|| {
            panic!(
                "family `{family_name}` must resolve member protocol systems in the runtime registry"
            )
        })
        .into_iter()
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
#[allow(dead_code)]
pub fn family_detected_runtime_for_tests(
    family_name: &str,
    chain: Chain,
    shared_spkg: impl Into<String>,
) -> DetectedFamilyRuntime {
    default_family_runtime_registry()
        .detected_family_runtime(family_name, chain, shared_spkg)
        .unwrap_or_else(|_| {
            panic!("family `{family_name}` must resolve a detected runtime in the registry")
        })
}

#[cfg(test)]
#[allow(dead_code)]
pub fn family_resolved_shared_stream_for_tests(
    family_name: &str,
    chain: Chain,
    shared_spkg: impl Into<String>,
) -> ResolvedSharedFamilyStream {
    family_detected_runtime_for_tests(family_name, chain, shared_spkg).resolved_shared_stream()
}

#[cfg(test)]
pub fn write_family_defaults_config_for_tests(
    file_prefix: &str,
    unique: &str,
    family_name: &str,
    shared_spkg_path: &str,
    stop_block: Option<i64>,
    extractor_configs_yaml: &str,
) -> std::path::PathBuf {
    let stop_block_yaml = stop_block
        .map(|value| format!("    stop_block: {value}\n"))
        .unwrap_or_default();
    let config_path = std::env::temp_dir().join(format!("{file_prefix}-{unique}.yaml"));
    let shared_module = family_output_module_for_tests(family_name);
    std::fs::write(
        &config_path,
        format!(
            r#"
family_runtimes:
  {family_name}:
    shared_spkg: "{shared_spkg_path}"
    shared_module: "{shared_module}"
{stop_block_yaml}extractors:
{extractor_configs_yaml}
"#,
        ),
    )
    .expect("write temp family-default config");
    config_path
}

#[cfg(test)]
pub fn write_uniswap_family_defaults_config(
    file_prefix: &str,
    unique: &str,
    shared_spkg_path: &str,
    start_block: i64,
    stop_block: Option<i64>,
) -> std::path::PathBuf {
    write_uniswap_family_defaults_config_with_member_names(
        file_prefix,
        unique,
        shared_spkg_path,
        start_block,
        stop_block,
        "uniswap_v2",
        "uniswap_v3",
    )
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub struct FamilyDefaultsFixtureMemberSpec<'a> {
    pub extractor_name: &'a str,
    pub protocol_system: &'a str,
    pub protocol_type_name: &'a str,
    pub module_name: &'a str,
    pub start_block: i64,
    pub substreams_module_name: Option<&'a str>,
    pub substreams_params: Option<&'a str>,
}

#[cfg(test)]
fn render_family_defaults_fixture_extractors_yaml(
    family_name: &str,
    members: &[FamilyDefaultsFixtureMemberSpec<'_>],
) -> String {
    members
        .iter()
        .map(|member| {
            format!(
                r#"  {extractor_name}:
    name: "{extractor_name}"
    protocol_system: "{protocol_system}"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1
    start_block: {start_block}
    protocol_types:
      - name: "{protocol_type_name}"
        financial_type: "Swap"
    module_name: "{module_name}"
    family_runtime:
      family: "{family_name}"
"#,
                extractor_name = member.extractor_name,
                protocol_system = member.protocol_system,
                start_block = member.start_block,
                protocol_type_name = member.protocol_type_name,
                module_name = member.module_name,
                family_name = family_name,
            )
        })
        .collect()
}

#[cfg(test)]
fn render_family_defaults_fixture_member_defaults_yaml(
    members: &[FamilyDefaultsFixtureMemberSpec<'_>],
) -> String {
    let body = members
        .iter()
        .filter_map(|member| match (member.substreams_module_name, member.substreams_params) {
            (Some(module_name), Some(params)) => Some(format!(
                r#"      {protocol_system}:
        substreams_params:
          {module_name}: "{params}"
"#,
                protocol_system = member.protocol_system,
                module_name = module_name,
                params = params,
            )),
            _ => None,
        })
        .collect::<String>();

    if body.is_empty() {
        String::new()
    } else {
        format!("    members:\n{body}")
    }
}

#[cfg(test)]
pub fn write_family_defaults_config_with_shared_bootstrap_for_tests(
    file_prefix: &str,
    unique: &str,
    family_name: &str,
    shared_spkg_path: &str,
    bootstrap_path: Option<&str>,
    durability_scope: Option<&str>,
    stop_block: Option<i64>,
    members: &[FamilyDefaultsFixtureMemberSpec<'_>],
) -> std::path::PathBuf {
    let stop_block_yaml = stop_block
        .map(|value| format!("    stop_block: {value}\n"))
        .unwrap_or_default();
    let bootstrap_yaml = bootstrap_path
        .map(|path| format!("    bootstrap:\n      params: \"@{path}\"\n"))
        .unwrap_or_default();
    let durability_scope_yaml = durability_scope
        .map(|scope| format!("    durability_scope: \"{scope}\"\n"))
        .unwrap_or_default();
    let member_defaults_yaml = render_family_defaults_fixture_member_defaults_yaml(members);
    let extractor_configs_yaml =
        render_family_defaults_fixture_extractors_yaml(family_name, members);
    let config_path = std::env::temp_dir().join(format!("{file_prefix}-{unique}.yaml"));

    std::fs::write(
        &config_path,
        format!(
            r#"
family_runtimes:
  {family_name}:
    shared_spkg: "{shared_spkg_path}"
    shared_module: "{shared_module}"
{durability_scope_yaml}{stop_block_yaml}{bootstrap_yaml}{member_defaults_yaml}extractors:
{extractor_configs_yaml}
"#,
            family_name = family_name,
            shared_module = family_output_module_for_tests(family_name),
            durability_scope_yaml = durability_scope_yaml,
            stop_block_yaml = stop_block_yaml,
            bootstrap_yaml = bootstrap_yaml,
            member_defaults_yaml = member_defaults_yaml,
            extractor_configs_yaml = extractor_configs_yaml,
        ),
    )
    .expect("write temp family-default config");

    config_path
}

#[cfg(test)]
pub fn write_uniswap_family_defaults_config_with_member_names(
    file_prefix: &str,
    unique: &str,
    shared_spkg_path: &str,
    start_block: i64,
    stop_block: Option<i64>,
    v2_name: &str,
    v3_name: &str,
) -> std::path::PathBuf {
    write_family_defaults_config_with_shared_bootstrap_for_tests(
        file_prefix,
        unique,
        "uniswap",
        shared_spkg_path,
        None,
        None,
        stop_block,
        &[
            FamilyDefaultsFixtureMemberSpec {
                extractor_name: v2_name,
                protocol_system: "uniswap_v2",
                protocol_type_name: "uniswap_v2_pool",
                module_name: "v2_map_pool_events",
                start_block,
                substreams_module_name: None,
                substreams_params: None,
            },
            FamilyDefaultsFixtureMemberSpec {
                extractor_name: v3_name,
                protocol_system: "uniswap_v3",
                protocol_type_name: "uniswap_v3_pool",
                module_name: "v3_map_protocol_changes",
                start_block,
                substreams_module_name: None,
                substreams_params: None,
            },
        ],
    )
}

#[cfg(test)]
pub fn write_uniswap_family_defaults_config_with_member_names_and_runtime_overrides(
    file_prefix: &str,
    unique: &str,
    shared_spkg_path: &str,
    bootstrap_path: Option<&str>,
    durability_scope: Option<&str>,
    start_block: i64,
    stop_block: Option<i64>,
    v2_name: &str,
    v3_name: &str,
) -> std::path::PathBuf {
    write_family_defaults_config_with_shared_bootstrap_for_tests(
        file_prefix,
        unique,
        "uniswap",
        shared_spkg_path,
        bootstrap_path,
        durability_scope,
        stop_block,
        &[
            FamilyDefaultsFixtureMemberSpec {
                extractor_name: v2_name,
                protocol_system: "uniswap_v2",
                protocol_type_name: "uniswap_v2_pool",
                module_name: "v2_map_pool_events",
                start_block,
                substreams_module_name: None,
                substreams_params: None,
            },
            FamilyDefaultsFixtureMemberSpec {
                extractor_name: v3_name,
                protocol_system: "uniswap_v3",
                protocol_type_name: "uniswap_v3_pool",
                module_name: "v3_map_protocol_changes",
                start_block,
                substreams_module_name: None,
                substreams_params: None,
            },
        ],
    )
}

#[cfg(test)]
pub fn write_uniswap_family_defaults_config_with_shared_bootstrap(
    file_prefix: &str,
    unique: &str,
    shared_spkg_path: &str,
    bootstrap_path: &str,
    start_block: i64,
    stop_block: Option<i64>,
    v2_substreams_params: Option<&str>,
    v3_substreams_params: Option<&str>,
) -> std::path::PathBuf {
    write_family_defaults_config_with_shared_bootstrap_for_tests(
        file_prefix,
        unique,
        "uniswap",
        shared_spkg_path,
        Some(bootstrap_path),
        None,
        stop_block,
        &[
            FamilyDefaultsFixtureMemberSpec {
                extractor_name: "uniswap_v2",
                protocol_system: "uniswap_v2",
                protocol_type_name: "uniswap_v2_pool",
                module_name: "v2_map_pool_events",
                start_block,
                substreams_module_name: Some("v2_map_pool_events"),
                substreams_params: v2_substreams_params,
            },
            FamilyDefaultsFixtureMemberSpec {
                extractor_name: "uniswap_v3",
                protocol_system: "uniswap_v3",
                protocol_type_name: "uniswap_v3_pool",
                module_name: "v3_map_protocol_changes",
                start_block,
                substreams_module_name: Some("v3_map_events"),
                substreams_params: v3_substreams_params,
            },
        ],
    )
}

#[cfg(test)]
pub fn family_shared_module_for_tests(family_name: &str) -> String {
    family_output_module_for_tests(family_name)
}

#[cfg(test)]
pub fn family_runtime_config_for_tests(
    family_name: &str,
    shared_spkg: impl Into<String>,
) -> FamilyRuntimeConfig {
    FamilyRuntimeConfig {
        family: family_name.to_string(),
        shared_spkg: Some(shared_spkg.into()),
        shared_module: Some(family_shared_module_for_tests(family_name)),
        durability_scope: Some(family_durability_scope_for_tests(family_name)),
    }
}

#[cfg(test)]
pub fn uniswap_family_shared_module_for_tests() -> String {
    family_shared_module_for_tests("uniswap")
}

#[cfg(test)]
pub fn uniswap_family_runtime_config_for_tests(
    shared_spkg: impl Into<String>,
) -> FamilyRuntimeConfig {
    family_runtime_config_for_tests("uniswap", shared_spkg)
}

#[cfg(test)]
pub fn uniswap_family_protocol_systems_for_tests() -> Vec<String> {
    family_member_protocol_systems_for_tests("uniswap")
}

#[cfg(test)]
pub fn uniswap_family_durability_scope_for_tests() -> String {
    family_durability_scope_for_tests("uniswap")
}

#[cfg(test)]
pub fn write_uniswap_family_defaults_config_for_tests(
    file_prefix: &str,
    unique: &str,
    shared_spkg_path: &str,
    start_block: i64,
    stop_block: Option<i64>,
) -> std::path::PathBuf {
    write_uniswap_family_defaults_config(
        file_prefix,
        unique,
        shared_spkg_path,
        start_block,
        stop_block,
    )
}

#[cfg(test)]
pub fn write_uniswap_family_defaults_config_with_member_names_for_tests(
    file_prefix: &str,
    unique: &str,
    shared_spkg_path: &str,
    start_block: i64,
    stop_block: Option<i64>,
    v2_name: &str,
    v3_name: &str,
) -> std::path::PathBuf {
    write_uniswap_family_defaults_config_with_member_names(
        file_prefix,
        unique,
        shared_spkg_path,
        start_block,
        stop_block,
        v2_name,
        v3_name,
    )
}

#[cfg(test)]
pub fn unique_test_suffix() -> String {
    format!(
        "{}-{}",
        process::id(),
        chrono::Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_default()
    )
}

#[cfg(test)]
pub fn write_temp_substreams_package_for_tests(label: &str) -> String {
    let shared_spkg_path = std::env::temp_dir().join(format!(
        "tycho-indexer-{label}-{}-{}.spkg",
        process::id(),
        chrono::Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_default()
    ));
    std::fs::write(
        &shared_spkg_path,
        crate::pb::sf::substreams::v1::Package::default().encode_to_vec(),
    )
    .expect("write temp spkg");
    shared_spkg_path
        .to_str()
        .expect("utf8 spkg path")
        .to_string()
}

#[cfg(test)]
#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct BuildExtractorsTestContext<'a> {
    pub chain_state: ChainState,
    pub endpoint_url: &'a str,
    pub s3_bucket: Option<&'a str>,
    pub substreams_api_token: &'a str,
    pub cached_gw: &'a CachedGateway,
    pub database_insert_batch_size: usize,
    pub token_pre_processor: &'a EthereumTokenPreProcessor,
    pub rpc_client: &'a EthereumRpcClient,
    pub runtime: Option<&'a tokio::runtime::Handle>,
    pub partial_blocks: bool,
    pub family_runtime_registry: FamilyRuntimeRegistry<'static>,
}

#[cfg(test)]
impl<'a> BuildExtractorsTestContext<'a> {
    pub(crate) fn runtime_targets_build_context<'b>(
        &'b self,
    ) -> ResolvedRuntimeTargetsBuildContext<'b> {
        ResolvedRuntimeTargetsBuildContext::new(
            self.chain_state,
            self.endpoint_url,
            self.s3_bucket,
            self.substreams_api_token,
            self.cached_gw,
            self.database_insert_batch_size,
            self.token_pre_processor,
            self.rpc_client,
            self.runtime.cloned(),
            false,
            self.partial_blocks,
            self.family_runtime_registry,
        )
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) async fn build_all_extractors_for_tests(
    config: &ExtractorConfigs,
    context: BuildExtractorsTestContext<'_>,
) -> Result<(Vec<ManagedRunner>, Vec<ExtractorHandle>), ExtractionError> {
    config
        .resolved_indexer_runtime_plan_with_registry(context.family_runtime_registry)?
        .build_managed_runners(context.runtime_targets_build_context())
        .await
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) async fn build_all_extractors_from_config_path_with_registry_for_tests(
    config_path: &std::path::Path,
    context: BuildExtractorsTestContext<'_>,
) -> Result<(Vec<ManagedRunner>, Vec<ExtractorHandle>), ExtractionError> {
    let loaded_runtime_plan = crate::config::LoadedIndexerRuntimePlan::from_yaml_with_registry(
        config_path
            .to_str()
            .expect("config path should be utf8"),
        context.family_runtime_registry,
    )?;
    loaded_runtime_plan
        .resolved_runtime_plan()?
        .build_managed_runners(context.runtime_targets_build_context())
        .await
}

#[cfg(test)]
#[allow(dead_code, clippy::too_many_arguments)]
pub(crate) async fn build_all_extractors_from_config_path_with_default_family_registry_for_tests(
    config_path: &std::path::Path,
    chain_state: ChainState,
    _chains: &[Chain],
    endpoint_url: &str,
    s3_bucket: Option<&str>,
    substreams_api_token: &str,
    cached_gw: &CachedGateway,
    database_insert_batch_size: usize,
    token_pre_processor: &EthereumTokenPreProcessor,
    rpc_client: &EthereumRpcClient,
    runtime: Option<&tokio::runtime::Handle>,
    partial_blocks: bool,
) -> Result<(Vec<ManagedRunner>, Vec<ExtractorHandle>), ExtractionError> {
    build_all_extractors_from_config_path_with_registry_for_tests(
        config_path,
        BuildExtractorsTestContext {
            chain_state,
            endpoint_url,
            s3_bucket,
            substreams_api_token,
            cached_gw,
            database_insert_batch_size,
            token_pre_processor,
            rpc_client,
            runtime,
            partial_blocks,
            family_runtime_registry: default_family_runtime_registry(),
        },
    )
    .await
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn build_all_extractors_with_default_family_registry_for_tests(
    config: &ExtractorConfigs,
    chain_state: ChainState,
    _chains: &[Chain],
    endpoint_url: &str,
    s3_bucket: Option<&str>,
    substreams_api_token: &str,
    cached_gw: &CachedGateway,
    database_insert_batch_size: usize,
    token_pre_processor: &EthereumTokenPreProcessor,
    rpc_client: &EthereumRpcClient,
    runtime: Option<&tokio::runtime::Handle>,
    partial_blocks: bool,
) -> Result<(Vec<ManagedRunner>, Vec<ExtractorHandle>), ExtractionError> {
    build_all_extractors_for_tests(
        config,
        BuildExtractorsTestContext {
            chain_state,
            endpoint_url,
            s3_bucket,
            substreams_api_token,
            cached_gw,
            database_insert_batch_size,
            token_pre_processor,
            rpc_client,
            runtime,
            partial_blocks,
            family_runtime_registry: default_family_runtime_registry(),
        },
    )
    .await
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub fn swap_extractor_config_for_tests(
    extractor_name: impl Into<String>,
    protocol_system: impl Into<String>,
    chain: Chain,
    implementation_type: ImplementationType,
    start_block: i64,
    protocol_type_name: impl Into<String>,
    spkg: impl Into<String>,
    module_name: impl Into<String>,
    family_runtime: Option<FamilyRuntimeConfig>,
) -> crate::extractor::extractor_config::ExtractorConfig {
    crate::extractor::extractor_config::ExtractorConfig::new(
        extractor_name.into(),
        chain,
        implementation_type,
        1,
        start_block,
        None,
        vec![crate::extractor::extractor_config::ProtocolTypeConfig::new(
            protocol_type_name.into(),
            tycho_common::models::FinancialType::Swap,
        )],
        spkg.into(),
        module_name.into(),
        vec![],
        0,
        None,
        None,
        HashMap::new(),
        None,
    )
    .with_protocol_system(protocol_system)
    .with_family_runtime(family_runtime)
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
struct RecordSubstreamsFixtureMemberSpec<'a> {
    protocol_system: &'a str,
    protocol_type_name: &'a str,
    module_name: &'a str,
    substreams_module_name: &'a str,
    substreams_file_name: &'a str,
    substreams_body: &'a str,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
struct RecordSubstreamsFixtureFamilySpec<'a> {
    temp_prefix: &'a str,
    extractors_file_name: &'a str,
    family_name: &'a str,
    shared_bootstrap_body: &'a str,
    members: &'a [RecordSubstreamsFixtureMemberSpec<'a>],
}

#[cfg(test)]
fn write_record_substreams_combined_family_fixture_inputs(
    shared_spkg_path: &std::path::Path,
    spec: RecordSubstreamsFixtureFamilySpec<'_>,
    registry: FamilyRuntimeRegistry<'_>,
) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!(
        "tycho-indexer-{}-{}-{}",
        spec.temp_prefix,
        std::process::id(),
        chrono::Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_default()
    ));
    let config_dir = root.join("config");
    std::fs::create_dir_all(&config_dir).expect("create record-substreams family config dir");

    std::fs::write(config_dir.join("shared_bootstrap.yaml"), spec.shared_bootstrap_body)
        .expect("write shared bootstrap file");
    for member in spec.members {
        std::fs::write(config_dir.join(member.substreams_file_name), member.substreams_body)
            .expect("write member substreams params file");
    }

    let members_yaml = spec
        .members
        .iter()
        .map(|member| {
            format!(
                r#"      {protocol_system}:
        substreams_params:
          {substreams_module_name}: "@config/{substreams_file_name}"
"#,
                protocol_system = member.protocol_system,
                substreams_module_name = member.substreams_module_name,
                substreams_file_name = member.substreams_file_name,
            )
        })
        .collect::<String>();
    let extractors_yaml = spec
        .members
        .iter()
        .map(|member| {
            format!(
                r#"  {protocol_system}:
    name: "{protocol_system}"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "{protocol_type_name}"
        financial_type: "Swap"
    module_name: "{module_name}"
    family_runtime:
      family: "{family_name}"
"#,
                protocol_system = member.protocol_system,
                protocol_type_name = member.protocol_type_name,
                module_name = member.module_name,
                family_name = spec.family_name,
            )
        })
        .collect::<String>();
    let shared_module = registry
        .output_module_for_family(spec.family_name)
        .unwrap_or_else(|| {
            panic!("expected registered family runtime output module for `{}`", spec.family_name)
        });
    let extractors_config = root.join(spec.extractors_file_name);
    std::fs::write(
        &extractors_config,
        format!(
            r#"family_runtimes:
  {family_name}:
    shared_spkg: "{}"
    shared_module: "{shared_module}"
    bootstrap:
      params: "@config/shared_bootstrap.yaml"
    members:
{members_yaml}extractors:
{extractors_yaml}
"#,
            shared_spkg_path.display(),
            family_name = spec.family_name,
            shared_module = shared_module,
            members_yaml = members_yaml,
            extractors_yaml = extractors_yaml,
        ),
    )
    .expect("write combined extractors config");

    extractors_config
}

#[cfg(test)]
pub fn write_record_substreams_family_fixture_inputs(
    shared_spkg_path: &std::path::Path,
) -> std::path::PathBuf {
    const MEMBERS: &[RecordSubstreamsFixtureMemberSpec<'_>] = &[
        RecordSubstreamsFixtureMemberSpec {
            protocol_system: "uniswap_v2",
            protocol_type_name: "uniswap_v2_pool",
            module_name: "v2_map_pool_events",
            substreams_module_name: "v2_map_pool_events",
            substreams_file_name: "uniswap_v2_substreams.yaml",
            substreams_body: r#"includes:
  - "shared_bootstrap.yaml"
params:
  pools:
    - "0x1111111111111111111111111111111111111111"
"#,
        },
        RecordSubstreamsFixtureMemberSpec {
            protocol_system: "uniswap_v3",
            protocol_type_name: "uniswap_v3_pool",
            module_name: "v3_map_protocol_changes",
            substreams_module_name: "v3_map_events",
            substreams_file_name: "uniswap_v3_substreams.yaml",
            substreams_body: r#"includes:
  - "shared_bootstrap.yaml"
params:
  pools:
    - "0x2222222222222222222222222222222222222222"
"#,
        },
    ];
    write_record_substreams_combined_family_fixture_inputs(
        shared_spkg_path,
        RecordSubstreamsFixtureFamilySpec {
            temp_prefix: "record-family-config",
            extractors_file_name: "extractors.combined.yaml",
            family_name: "uniswap",
            shared_bootstrap_body: r#"start_block: 42
params:
  pools:
    - "0x1111111111111111111111111111111111111111"
    - "0x2222222222222222222222222222222222222222"
"#,
            members: MEMBERS,
        },
        default_family_runtime_registry(),
    )
}

#[cfg(test)]
pub fn write_record_substreams_future_family_fixture_inputs(
    shared_spkg_path: &std::path::Path,
) -> std::path::PathBuf {
    write_record_substreams_future_family_fixture_inputs_with_registry(
        shared_spkg_path,
        future_family_runtime_registry_for_record_substreams_tests(),
    )
}

#[cfg(test)]
pub fn write_record_substreams_future_family_fixture_inputs_with_registry(
    shared_spkg_path: &std::path::Path,
    registry: FamilyRuntimeRegistry<'static>,
) -> std::path::PathBuf {
    const MEMBERS: &[RecordSubstreamsFixtureMemberSpec<'_>] = &[
        RecordSubstreamsFixtureMemberSpec {
            protocol_system: "future_v1",
            protocol_type_name: "future_v1_pool",
            module_name: "future_v1_map_protocol_changes",
            substreams_module_name: "future_v1_map_events",
            substreams_file_name: "future_v1_substreams.yaml",
            substreams_body: r#"includes:
  - "shared_bootstrap.yaml"
params:
  pools:
    - "0x00000000000000000000000000000000000000a1"
  pool_tokens:
    - "0x00000000000000000000000000000000000000a1:0x00000000000000000000000000000000000000a1:0x00000000000000000000000000000000000000b1"
"#,
        },
        RecordSubstreamsFixtureMemberSpec {
            protocol_system: "future_v2",
            protocol_type_name: "future_v2_pool",
            module_name: "future_v2_map_protocol_changes",
            substreams_module_name: "future_v2_map_events",
            substreams_file_name: "future_v2_substreams.yaml",
            substreams_body: r#"includes:
  - "shared_bootstrap.yaml"
params:
  pools:
    - "0x00000000000000000000000000000000000000b2"
  pool_tokens:
    - "0x00000000000000000000000000000000000000b2:0x00000000000000000000000000000000000000a2:0x00000000000000000000000000000000000000b2"
"#,
        },
    ];
    write_record_substreams_combined_family_fixture_inputs(
        shared_spkg_path,
        RecordSubstreamsFixtureFamilySpec {
            temp_prefix: "record-future-family-config",
            extractors_file_name: "extractors.future.combined.yaml",
            family_name: "future_swap",
            shared_bootstrap_body: r#"start_block: 99
params:
  pools:
    - "0x00000000000000000000000000000000000000a1"
    - "0x00000000000000000000000000000000000000b2"
  pool_tokens:
    - "0x00000000000000000000000000000000000000a1:0x00000000000000000000000000000000000000a1:0x00000000000000000000000000000000000000b1"
    - "0x00000000000000000000000000000000000000b2:0x00000000000000000000000000000000000000a2:0x00000000000000000000000000000000000000b2"
"#,
            members: MEMBERS,
        },
        registry,
    )
}

#[cfg(test)]
pub fn write_record_substreams_ambiguous_fixture_inputs(
    shared_spkg_path: &std::path::Path,
) -> std::path::PathBuf {
    let config_path = write_record_substreams_family_fixture_inputs(shared_spkg_path);
    let mut config = std::fs::read_to_string(&config_path)
        .expect("read base combined-family record-substreams config");
    config.push_str(
        r#"
  curve:
    name: "curve"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 77
    protocol_types:
      - name: "curve_pool"
        financial_type: "Swap"
    spkg: "protocols/substreams/ethereum-curve/ethereum-curve-v0.3.2.spkg"
    module_name: "map_protocol_changes"
"#,
    );
    std::fs::write(&config_path, config)
        .expect("write ambiguous record-substreams config with standalone target");
    config_path
}

#[cfg(test)]
fn future_family_branch_materializer<'a>(
    _rpc: &'a EthereumRpcClient,
    _branch: &'a BootstrapBranchDescriptor,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<BlockChanges, ExtractionError>> + Send + 'a>,
> {
    Box::pin(async {
        Err(ExtractionError::Setup(
            "future family branch materializer should not run in record-substreams tests"
                .to_string(),
        ))
    })
}

#[cfg(test)]
const FUTURE_FAMILY_RUNTIME_SPEC: FamilyRuntimeSpec = shared_family_runtime_spec(
    "future_swap",
    &[
        shared_family_member_spec(
            "future_v1",
            &["futurev1"],
            Some(shared_bootstrap_member_runtime(
                BootstrapStrategy::UniswapV2Rpc,
                SharedBootstrapParamsParser::PoolList,
                future_family_branch_materializer,
            )),
        ),
        shared_family_member_spec(
            "future_v2",
            &["futurev2"],
            Some(shared_bootstrap_member_runtime(
                BootstrapStrategy::UniswapV2Rpc,
                SharedBootstrapParamsParser::PoolList,
                future_family_branch_materializer,
            )),
        ),
    ],
    "map_future_swap_family_protocol_changes",
    "future_swap_family",
    "family::future_swap",
    None,
);

#[cfg(test)]
pub fn future_family_runtime_registry_for_record_substreams_tests() -> FamilyRuntimeRegistry<'static>
{
    FamilyRuntimeRegistry::new(&[FUTURE_FAMILY_RUNTIME_SPEC])
}

#[cfg(test)]
pub fn future_family_runtime_registry_for_record_substreams_tests_with_durability_scope(
    durability_scope: impl Into<String>,
) -> FamilyRuntimeRegistry<'static> {
    fn leak_str(value: String) -> &'static str {
        Box::leak(value.into_boxed_str())
    }

    let leaked_scope = leak_str(durability_scope.into());
    let members: &'static [FamilyMemberSpec] = Box::leak(Box::new([
        shared_family_member_spec(
            "future_v1",
            &["futurev1"],
            Some(shared_bootstrap_member_runtime(
                BootstrapStrategy::UniswapV2Rpc,
                SharedBootstrapParamsParser::PoolList,
                future_family_branch_materializer,
            )),
        ),
        shared_family_member_spec(
            "future_v2",
            &["futurev2"],
            Some(shared_bootstrap_member_runtime(
                BootstrapStrategy::UniswapV2Rpc,
                SharedBootstrapParamsParser::PoolList,
                future_family_branch_materializer,
            )),
        ),
    ]));
    let specs: &'static [FamilyRuntimeSpec] = Box::leak(Box::new([shared_family_runtime_spec(
        "future_swap",
        members,
        "map_future_swap_family_protocol_changes",
        "future_swap_family",
        leaked_scope,
        None,
    )]));

    FamilyRuntimeRegistry::new(specs)
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoCombinedFamilyIdentitySpec {
    pub family_name: String,
    pub output_module: String,
    pub shared_spkg: String,
    pub extractors_config_path: std::path::PathBuf,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoCombinedFamilyFixtureCaptureSpec {
    pub family_name: String,
    pub extractors_config_path: std::path::PathBuf,
    pub output_module: String,
    pub output_path: std::path::PathBuf,
    pub start_block: i64,
    pub stop_block: String,
    pub params: Vec<String>,
}

#[cfg(test)]
pub fn repo_combined_family_extractors_config_path_for_tests(
    file_name: &str,
) -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(file_name)
}

#[cfg(test)]
pub fn combined_family_real_history_slice_fixture_path_for_recorder_for_tests() -> std::path::PathBuf
{
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("combined_family_real_history_slice.json")
}

#[cfg(test)]
pub fn repo_combined_family_identity_for_tests(file_name: &str) -> RepoCombinedFamilyIdentitySpec {
    let config_path = repo_combined_family_extractors_config_path_for_tests(file_name);
    #[derive(Deserialize)]
    struct SharedFamilyRuntimeConfigForTests {
        shared_spkg: String,
    }

    #[derive(Deserialize)]
    struct SharedFamilyRuntimeConfigFileForTests {
        family_runtimes: HashMap<String, SharedFamilyRuntimeConfigForTests>,
    }

    let raw = std::fs::read_to_string(&config_path).expect("read repo combined family config");
    let parsed: SharedFamilyRuntimeConfigFileForTests =
        serde_yaml::from_str(&raw).expect("parse repo combined family config");
    assert_eq!(
        parsed.family_runtimes.len(),
        1,
        "expected repo combined config to declare exactly one family runtime"
    );

    let (family_name, family_runtime) = parsed
        .family_runtimes
        .into_iter()
        .next()
        .expect("repo combined family runtime");
    let output_module = default_family_runtime_registry()
        .shared_runtime_metadata_for_family(&family_name)
        .map(|metadata| metadata.output_module)
        .unwrap_or_else(|| {
            panic!("expected registered family runtime output module for `{family_name}`")
        })
        .to_string();

    RepoCombinedFamilyIdentitySpec {
        family_name,
        output_module,
        shared_spkg: family_runtime.shared_spkg,
        extractors_config_path: config_path,
    }
}

#[cfg(test)]
pub fn repo_combined_family_capture_spec_for_tests(
    file_name: &str,
    output_path: std::path::PathBuf,
    start_block: i64,
    stop_block: impl Into<String>,
    params: Vec<String>,
) -> RepoCombinedFamilyFixtureCaptureSpec {
    let identity = repo_combined_family_identity_for_tests(file_name);
    RepoCombinedFamilyFixtureCaptureSpec {
        family_name: identity.family_name,
        extractors_config_path: identity.extractors_config_path,
        output_module: identity.output_module,
        output_path,
        start_block,
        stop_block: stop_block.into(),
        params,
    }
}

#[cfg(test)]
pub fn combined_family_real_history_slice_capture_spec_for_tests(
) -> RepoCombinedFamilyFixtureCaptureSpec {
    repo_combined_family_capture_spec_for_tests(
        "extractors.uniswap_v2_v3.combined.yaml",
        combined_family_real_history_slice_fixture_path_for_recorder_for_tests(),
        25_384_601,
        "+2",
        vec![],
    )
}

#[cfg(test)]
pub fn repo_combined_family_expected_spkg_for_tests() -> String {
    repo_combined_family_identity_for_tests("extractors.uniswap_v2_v3.combined.yaml").shared_spkg
}

#[cfg(test)]
pub fn repo_combined_family_record_cli_args_for_tests(
    spec: &RepoCombinedFamilyFixtureCaptureSpec,
    output_path: &std::path::Path,
    start_block: i64,
    stop_block: &str,
    params: &[&str],
) -> Vec<String> {
    let mut cli_args = vec![
        "tycho-indexer".to_string(),
        "--database-url".to_string(),
        "postgres://unused".to_string(),
        "--endpoint".to_string(),
        "http://localhost:9000".to_string(),
        "--rpc-url".to_string(),
        "http://localhost:8545".to_string(),
        "record-substreams".to_string(),
        "--substreams-api-token".to_string(),
        "token".to_string(),
        "--extractors-config".to_string(),
        spec.extractors_config_path
            .to_string_lossy()
            .to_string(),
        "--start-block".to_string(),
        start_block.to_string(),
        "--stop-block".to_string(),
        stop_block.to_string(),
        "--output".to_string(),
        output_path
            .to_string_lossy()
            .to_string(),
    ];
    for param in params {
        cli_args.push("--params".to_string());
        cli_args.push((*param).to_string());
    }
    cli_args
}

#[cfg(test)]
fn shell_escape_cli_arg_for_tests(arg: &str) -> String {
    if arg.bytes().all(|byte| {
        byte.is_ascii_alphanumeric()
            || matches!(byte, b'-' | b'_' | b'/' | b'.' | b':' | b'+' | b'=')
    }) {
        return arg.to_string();
    }

    format!("'{}'", arg.replace('\'', "'\"'\"'"))
}

#[cfg(test)]
pub fn render_repo_combined_family_record_command_for_tests(cli_args: &[String]) -> String {
    cli_args
        .iter()
        .map(|arg| shell_escape_cli_arg_for_tests(arg))
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SharedBootstrapPoolSeed {
    pub protocol_system: String,
    pub protocol_type_name: String,
    pub component_id: String,
    pub token0: String,
    pub token1: String,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq)]
pub struct SharedBootstrapSeedUniverseSpec {
    pub chain: Chain,
    pub protocol_types: Vec<ProtocolType>,
    pub pools: Vec<SharedBootstrapPoolSeed>,
}

#[cfg(test)]
fn parse_bootstrap_query_value_for_tests<'a>(params: &'a str, key: &str) -> Option<&'a str> {
    params.split('&').find_map(|pair| {
        let (param_key, value) = pair.split_once('=')?;
        (param_key == key).then_some(value)
    })
}

#[cfg(test)]
pub fn shared_bootstrap_seed_universe_spec_from_config_path_with_registry_for_tests(
    config_path: &std::path::Path,
    registry: FamilyRuntimeRegistry<'static>,
) -> SharedBootstrapSeedUniverseSpec {
    let loaded_runtime_plan = crate::config::LoadedIndexerRuntimePlan::from_yaml_with_registry(
        config_path
            .to_str()
            .expect("shared bootstrap seed config path should be utf8"),
        registry,
    )
    .expect("load shared bootstrap seed runtime owner");
    let runtime_target = loaded_runtime_plan
        .resolved_runtime_plan()
        .expect("resolve runtime plan for shared bootstrap seed extraction")
        .into_unique_runtime_target(&format!(
            "shared bootstrap seed extraction from `{}` requires exactly one runtime target",
            config_path.display()
        ))
        .expect("shared bootstrap seed config should resolve one runtime target");

    shared_bootstrap_seed_universe_spec_from_runtime_target_for_tests(
        &runtime_target,
        &config_path.display().to_string(),
    )
}

#[cfg(test)]
pub fn shared_bootstrap_seed_universe_spec_from_runtime_target_for_tests(
    runtime_target: &ResolvedRuntimeTarget<'_>,
    config_label: &str,
) -> SharedBootstrapSeedUniverseSpec {
    let mut seeds = Vec::new();
    let mut protocol_types = Vec::new();
    let mut seen_protocol_types = std::collections::HashSet::new();

    for extractor in runtime_target.extractor_configs() {
        let protocol_system = extractor.protocol_system().to_string();
        for protocol_type in extractor.protocol_types() {
            if seen_protocol_types.insert(protocol_type.name().to_string()) {
                protocol_types.push(ProtocolType::new(
                    protocol_type.name().to_string(),
                    protocol_type.financial_type(),
                    None,
                    ImplementationType::Custom,
                ));
            }
        }
        let protocol_type_name = match extractor.protocol_types() {
            [protocol_type] => protocol_type.name().to_string(),
            protocol_types => panic!(
                "expected exactly one protocol_type for shared bootstrap seed extraction from `{}` / `{}`, found {:?}",
                config_label,
                protocol_system,
                protocol_types
                    .iter()
                    .map(|protocol_type| protocol_type.name())
                    .collect::<Vec<_>>()
            ),
        };
        let resolved_params = extractor
            .substreams_params
            .values()
            .find(|params| params.contains("pool_tokens="))
            .cloned()
            .or_else(|| {
                let bootstrap_params = extractor
                    .bootstrap
                    .as_ref()
                    .unwrap_or_else(|| {
                        panic!(
                            "expected bootstrap config for shared bootstrap seed extraction from `{}` / `{}`",
                            config_label, protocol_system
                        )
                    })
                    .params
                    .clone();

                if bootstrap_params.contains("pool_tokens=") {
                    Some(bootstrap_params)
                } else if bootstrap_params.contains("routes:")
                    || bootstrap_params.contains("includes:")
                {
                    Some(
                        crate::extractor::shared_config::parse_substreams_params_yaml(
                            &protocol_system,
                            &bootstrap_params,
                        )
                        .unwrap_or_else(|err| {
                            panic!(
                                "resolve shared bootstrap substreams params from `{}` / `{}`: {err}",
                                config_label, protocol_system
                            )
                        })
                        .1,
                    )
                } else {
                    None
                }
            })
            .unwrap_or_else(|| {
                panic!(
                    "expected resolved substreams params with pool_tokens for shared bootstrap seed extraction from `{}` / `{}`",
                    config_label, protocol_system
                )
            });

        let pool_tokens = parse_bootstrap_query_value_for_tests(&resolved_params, "pool_tokens")
            .unwrap_or("")
            .split(',')
            .filter(|entry| !entry.is_empty());

        for pool_token in pool_tokens {
            let mut parts = pool_token.split(':');
            let component_id = parts
                .next()
                .expect("pool_tokens entry should include pool")
                .to_string();
            let token0 = parts
                .next()
                .expect("pool_tokens entry should include token0")
                .to_string();
            let token1 = parts
                .next()
                .expect("pool_tokens entry should include token1")
                .to_string();
            assert!(
                parts.next().is_none(),
                "pool_tokens entry should only contain pool:token0:token1, got {pool_token}"
            );

            seeds.push(SharedBootstrapPoolSeed {
                protocol_system: protocol_system.clone(),
                protocol_type_name: protocol_type_name.clone(),
                component_id,
                token0,
                token1,
            });
        }
    }

    seeds.sort_by(|left, right| {
        left.protocol_system
            .cmp(&right.protocol_system)
            .then_with(|| {
                left.component_id
                    .cmp(&right.component_id)
            })
    });
    seeds.dedup_by(|left, right| {
        left.protocol_system == right.protocol_system && left.component_id == right.component_id
    });
    SharedBootstrapSeedUniverseSpec { chain: runtime_target.chain(), protocol_types, pools: seeds }
}

#[cfg(test)]
pub fn repo_unique_runtime_target_shared_bootstrap_seed_universe_spec_for_tests(
    extractors_file_name: &str,
) -> SharedBootstrapSeedUniverseSpec {
    let config_path = repo_combined_family_extractors_config_path_for_tests(extractors_file_name);
    shared_bootstrap_seed_universe_spec_from_config_path_with_registry_for_tests(
        &config_path,
        default_family_runtime_registry(),
    )
}

#[cfg(test)]
pub fn repo_combined_family_bootstrap_pool_seeds_for_tests(
    extractors_file_name: &str,
) -> Vec<SharedBootstrapPoolSeed> {
    repo_unique_runtime_target_shared_bootstrap_seed_universe_spec_for_tests(extractors_file_name)
        .pools
}

#[cfg(test)]
pub fn decode_hex_address_bytes_for_tests(value: &str) -> Bytes {
    Bytes::from(
        hex::decode(value.trim_start_matches("0x"))
            .unwrap_or_else(|err| panic!("decode hex address {value}: {err}")),
    )
}

#[cfg(test)]
pub async fn seed_repo_runtime_target_shared_bootstrap_universe_for_tests(
    direct_gw: &tycho_storage::postgres::direct::DirectGateway,
    extractors_file_name: &str,
) -> HashMap<String, std::collections::HashSet<String>> {
    use tycho_common::models::{
        blockchain::{Block, Transaction},
        contract::{Account, AccountDelta},
        protocol::ProtocolComponentStateDelta,
        token::Token,
        ChangeType,
    };

    let seed_spec = repo_unique_runtime_target_shared_bootstrap_seed_universe_spec_for_tests(
        extractors_file_name,
    );
    let chain = seed_spec.chain;

    let seed_block = Block::new(
        25_384_600,
        chain,
        Bytes::from(vec![0x4a; 32]),
        Bytes::from(vec![0x3a; 32]),
        chrono::DateTime::from_timestamp(1_718_500_000, 0)
            .expect("valid shared bootstrap seed timestamp")
            .naive_utc(),
    );
    direct_gw
        .upsert_block(std::slice::from_ref(&seed_block))
        .await
        .expect("seed shared bootstrap block");
    let seed_tx = Transaction::new(
        Bytes::from(vec![0x5a; 32]),
        seed_block.hash.clone(),
        Bytes::from(vec![0x7a; 20]),
        None,
        0,
    );
    direct_gw
        .upsert_tx(std::slice::from_ref(&seed_tx))
        .await
        .expect("seed shared bootstrap tx");
    let seed_timestamp = seed_block.ts;
    direct_gw
        .add_protocol_types(&seed_spec.protocol_types)
        .await
        .expect("seed shared bootstrap protocol types");
    let mut tokens_by_address: HashMap<String, Token> = HashMap::new();
    let mut protocol_components = Vec::with_capacity(seed_spec.pools.len());
    let mut contract_accounts = Vec::with_capacity(seed_spec.pools.len());
    let mut contract_deltas = Vec::with_capacity(seed_spec.pools.len());
    let mut state_deltas = Vec::new();
    let mut component_ids_by_system: HashMap<String, std::collections::HashSet<String>> =
        HashMap::new();

    for seed in seed_spec.pools {
        for token_address in [&seed.token0, &seed.token1] {
            tokens_by_address
                .entry(token_address.clone())
                .or_insert_with(|| {
                    Token::new(
                        &decode_hex_address_bytes_for_tests(token_address),
                        token_address,
                        18,
                        0,
                        &[],
                        chain,
                        100,
                    )
                });
        }
        let contract_address = Bytes::from(decode_hex_address_bytes_for_tests(&seed.component_id));
        component_ids_by_system
            .entry(seed.protocol_system.clone())
            .or_default()
            .insert(seed.component_id.clone());
        let contract = Account::new(
            chain,
            contract_address.clone(),
            format!("SharedBootstrap{}", seed.protocol_type_name),
            HashMap::new(),
            Bytes::new(),
            HashMap::new(),
            Bytes::new(),
            Bytes::new(),
            seed_tx.hash.clone(),
            seed_tx.hash.clone(),
            Some(seed_tx.hash.clone()),
        );
        let contract_delta: AccountDelta = contract.clone().into();
        contract_accounts.push(contract);
        contract_deltas.push((seed_tx.hash.clone(), contract_delta));
        protocol_components.push(ProtocolComponent::new(
            &seed.component_id,
            &seed.protocol_system,
            &seed.protocol_type_name,
            chain,
            vec![
                decode_hex_address_bytes_for_tests(&seed.token0),
                decode_hex_address_bytes_for_tests(&seed.token1),
            ],
            vec![contract_address],
            HashMap::new(),
            ChangeType::Creation,
            seed_tx.hash.clone(),
            seed_timestamp,
        ));

        if seed.protocol_system == "uniswap_v3" {
            state_deltas.push((
                seed_tx.hash.clone(),
                ProtocolComponentStateDelta {
                    component_id: seed.component_id.clone(),
                    updated_attributes: HashMap::from([
                        ("liquidity".to_string(), Bytes::from([0_u8; 16].to_vec())),
                        ("tick".to_string(), Bytes::from([0_u8; 4].to_vec())),
                        ("sqrt_price_x96".to_string(), Bytes::from(vec![1_u8])),
                        ("protocol_fees/token0".to_string(), Bytes::from([0_u8; 16].to_vec())),
                        ("protocol_fees/token1".to_string(), Bytes::from([0_u8; 16].to_vec())),
                        (
                            "ticks/-60/net-liquidity".to_string(),
                            Bytes::from(1_i128.to_be_bytes().to_vec()),
                        ),
                        (
                            "ticks/60/net-liquidity".to_string(),
                            Bytes::from((-1_i128).to_be_bytes().to_vec()),
                        ),
                    ]),
                    deleted_attributes: std::collections::HashSet::new(),
                    created_attributes: std::collections::HashSet::from([
                        "liquidity".to_string(),
                        "tick".to_string(),
                        "sqrt_price_x96".to_string(),
                        "protocol_fees/token0".to_string(),
                        "protocol_fees/token1".to_string(),
                        "ticks/-60/net-liquidity".to_string(),
                        "ticks/60/net-liquidity".to_string(),
                    ]),
                },
            ));
        }
    }

    let tokens = tokens_by_address
        .into_values()
        .collect::<Vec<_>>();
    direct_gw
        .add_tokens(&tokens)
        .await
        .expect("seed shared bootstrap tokens");
    for contract in &contract_accounts {
        direct_gw
            .insert_contract(contract)
            .await
            .expect("seed shared bootstrap contract");
    }
    direct_gw
        .update_contracts(&contract_deltas)
        .await
        .expect("seed shared bootstrap contract state");
    direct_gw
        .add_protocol_components(&protocol_components)
        .await
        .expect("seed shared bootstrap universe components");
    if !state_deltas.is_empty() {
        direct_gw
            .update_protocol_states(&state_deltas)
            .await
            .expect("seed shared bootstrap universe protocol states");
    }

    component_ids_by_system
}

pub fn family_block_response_from_block_changes(
    cursor_label: &str,
    family_changes: tycho_substreams::pb::tycho::evm::v1::BlockChanges,
) -> crate::pb::sf::substreams::rpc::v2::Response {
    family_block_response_from_block_changes_for_family("uniswap", cursor_label, family_changes)
}

pub fn family_block_response_from_block_changes_for_family(
    family_name: &str,
    cursor_label: &str,
    family_changes: tycho_substreams::pb::tycho::evm::v1::BlockChanges,
) -> crate::pb::sf::substreams::rpc::v2::Response {
    use prost::Message;

    use crate::pb::sf::substreams::{
        rpc::v2::{
            response::Message as ResponseMessage, BlockScopedData, MapModuleOutput, Response,
        },
        v1::Clock,
    };

    let number = family_changes
        .block
        .as_ref()
        .expect("family block present")
        .number;
    let output_module = family_output_module_for_tests(family_name);
    Response {
        message: Some(ResponseMessage::BlockScopedData(BlockScopedData {
            output: Some(MapModuleOutput {
                name: output_module,
                map_output: Some(prost_types::Any {
                    type_url: "type.googleapis.com/tycho.evm.v1.BlockChanges".to_string(),
                    value: family_changes.encode_to_vec(),
                }),
                debug_info: None,
            }),
            clock: Some(Clock { id: number.to_string(), number, timestamp: None }),
            cursor: format!("{cursor_label}@{number}"),
            final_block_height: number,
            debug_map_outputs: vec![],
            debug_store_outputs: vec![],
            attestation: String::new(),
            is_partial: false,
            partial_index: None,
            is_last_partial: None,
        })),
    }
}

pub fn family_block_response(
    cursor_label: &str,
    number: u64,
    block_timestamp: u64,
    changes: Vec<tycho_substreams::pb::tycho::evm::v1::TransactionChanges>,
) -> crate::pb::sf::substreams::rpc::v2::Response {
    family_block_response_for_family("uniswap", cursor_label, number, block_timestamp, changes)
}

pub fn family_block_response_for_family(
    family_name: &str,
    cursor_label: &str,
    number: u64,
    block_timestamp: u64,
    changes: Vec<tycho_substreams::pb::tycho::evm::v1::TransactionChanges>,
) -> crate::pb::sf::substreams::rpc::v2::Response {
    family_block_response_from_block_changes_for_family(
        family_name,
        cursor_label,
        tycho_substreams::pb::tycho::evm::v1::BlockChanges {
            block: Some(tycho_substreams::pb::tycho::evm::v1::Block {
                number,
                hash: vec![number as u8; 32],
                parent_hash: vec![number.saturating_sub(1) as u8; 32],
                ts: block_timestamp,
            }),
            changes,
            storage_changes: vec![],
        },
    )
}

pub fn address(byte: u8) -> Vec<u8> {
    vec![byte; 20]
}

pub fn topic_address(byte: u8) -> Vec<u8> {
    use ethabi::{ethereum_types::Address, Token};

    ethabi::encode(&[Token::Address(Address::from_slice(&address(byte)))])
}

pub fn topic_uint24(value: u32) -> Vec<u8> {
    use ethabi::{ethereum_types::U256, Token};

    ethabi::encode(&[Token::Uint(U256::from(value))])
}

pub fn v2_pair_created_block(
    number: u64,
    block_timestamp: i64,
    factory: u8,
    token0: u8,
    token1: u8,
    pair: u8,
) -> substreams_ethereum::pb::eth::v2::Block {
    use ethabi::{
        ethereum_types::{Address, U256},
        Token,
    };
    use prost_types::Timestamp;
    use substreams_ethereum::pb::eth::v2::{
        block::DetailLevel, transaction_trace::Type as TransactionType, Block, BlockHeader, Log,
        TransactionReceipt, TransactionTrace, TransactionTraceStatus,
    };

    let data = ethabi::encode(&[
        Token::Address(Address::from_slice(&address(pair))),
        Token::Uint(U256::from(1u64)),
    ]);
    let log = Log {
        address: address(factory),
        topics: vec![
            vec![
                13, 54, 72, 189, 15, 107, 168, 1, 52, 163, 59, 169, 39, 90, 197, 133, 217, 211, 21,
                240, 173, 131, 85, 205, 222, 253, 227, 26, 250, 40, 208, 233,
            ],
            topic_address(token0),
            topic_address(token1),
        ],
        data,
        index: 0,
        block_index: 0,
        ordinal: 1,
    };

    Block {
        hash: vec![number as u8; 32],
        number,
        size: 0,
        header: Some(BlockHeader {
            parent_hash: vec![number.saturating_sub(1) as u8; 32],
            timestamp: Some(Timestamp { seconds: block_timestamp, nanos: 0 }),
            ..Default::default()
        }),
        transaction_traces: vec![TransactionTrace {
            index: 0,
            hash: vec![0xaa; 32],
            from: vec![0x01; 20],
            to: address(factory),
            status: TransactionTraceStatus::Succeeded as i32,
            receipt: Some(TransactionReceipt { logs: vec![log], ..Default::default() }),
            r#type: TransactionType::TrxTypeLegacy as i32,
            ..Default::default()
        }],
        detail_level: DetailLevel::DetaillevelBase as i32,
        ..Default::default()
    }
}

pub fn v3_pool_created_block(
    number: u64,
    block_timestamp: i64,
    factory: u8,
    token0: u8,
    token1: u8,
    fee: u32,
    tick_spacing: i32,
    pool: u8,
) -> substreams_ethereum::pb::eth::v2::Block {
    use ethabi::{ethereum_types::Address, Token};
    use prost_types::Timestamp;
    use substreams_ethereum::pb::eth::v2::{
        block::DetailLevel, transaction_trace::Type as TransactionType, Block, BlockHeader, Log,
        TransactionReceipt, TransactionTrace, TransactionTraceStatus,
    };

    let data = ethabi::encode(&[
        Token::Int(tick_spacing.into()),
        Token::Address(Address::from_slice(&address(pool))),
    ]);
    let log = Log {
        address: address(factory),
        topics: vec![
            vec![
                120, 60, 202, 28, 4, 18, 221, 13, 105, 94, 120, 69, 104, 201, 109, 162, 233, 194,
                47, 249, 137, 53, 122, 46, 139, 29, 155, 43, 78, 107, 113, 24,
            ],
            topic_address(token0),
            topic_address(token1),
            topic_uint24(fee),
        ],
        data,
        index: 0,
        block_index: 0,
        ordinal: 1,
    };

    Block {
        hash: vec![number as u8; 32],
        number,
        size: 0,
        header: Some(BlockHeader {
            parent_hash: vec![number.saturating_sub(1) as u8; 32],
            timestamp: Some(Timestamp { seconds: block_timestamp, nanos: 0 }),
            ..Default::default()
        }),
        transaction_traces: vec![TransactionTrace {
            index: 0,
            hash: vec![0xcd; 32],
            from: vec![0x01; 20],
            to: address(factory),
            status: TransactionTraceStatus::Succeeded as i32,
            receipt: Some(TransactionReceipt { logs: vec![log], ..Default::default() }),
            r#type: TransactionType::TrxTypeLegacy as i32,
            ..Default::default()
        }],
        detail_level: DetailLevel::DetaillevelBase as i32,
        ..Default::default()
    }
}

#[cfg(test)]
pub mod fixtures {
    use std::{collections::HashSet, str::FromStr};

    use prost::Message;
    use tycho_common::{models::protocol::ProtocolComponentStateDelta, Bytes};
    use tycho_storage::postgres::db_fixtures::yesterday_midnight;
    use tycho_substreams::pb::tycho::evm::v1::*;

    const HASH_256_0: &str = "0000000000000000000000000000000000000000000000000000000000000000";

    pub fn pb_state_changes() -> EntityChanges {
        let res1_value = Bytes::from(1_000u64)
            .lpad(32, 0)
            .to_vec();
        let res2_value = Bytes::from(500u64).lpad(32, 0).to_vec();
        EntityChanges {
            component_id: "State1".to_owned(),
            attributes: vec![
                Attribute {
                    name: "reserve1".to_owned(),
                    value: res1_value,
                    change: ChangeType::Update.into(),
                },
                Attribute {
                    name: "reserve2".to_owned(),
                    value: res2_value,
                    change: ChangeType::Update.into(),
                },
            ],
        }
    }

    pub fn protocol_state_delta() -> ProtocolComponentStateDelta {
        let res1_value = Bytes::from(1_000u64)
            .lpad(32, 0)
            .to_vec();
        let res2_value = Bytes::from(500u64).lpad(32, 0).to_vec();
        ProtocolComponentStateDelta {
            component_id: "State1".to_string(),
            updated_attributes: vec![
                ("reserve1".to_owned(), Bytes::from(res1_value)),
                ("reserve2".to_owned(), Bytes::from(res2_value)),
            ]
            .into_iter()
            .collect(),
            deleted_attributes: HashSet::new(),
            ..Default::default()
        }
    }

    pub fn pb_protocol_component() -> ProtocolComponent {
        ProtocolComponent {
            id: "d417ff54652c09bd9f31f216b1a2e5d1e28c1dce1ba840c40d16f2b4d09b5902".to_owned(),
            tokens: vec![address_from_str(DAI_ADDRESS), address_from_str(DAI_ADDRESS)],
            contracts: vec![
                Bytes::from_str("0x31fF2589Ee5275a2038beB855F44b9Be993aA804")
                    .unwrap()
                    .0
                    .to_vec(),
                address_from_str(WETH_ADDRESS),
            ],
            static_att: vec![
                Attribute {
                    name: "balance".to_owned(),
                    value: Bytes::from(100u64).lpad(32, 0).to_vec(),
                    change: ChangeType::Creation.into(),
                },
                Attribute {
                    name: "factory_address".to_owned(),
                    value: b"0x0fwe0g240g20".to_vec(),
                    change: ChangeType::Creation.into(),
                },
            ],
            change: ChangeType::Creation.into(),
            protocol_type: Some(ProtocolType {
                name: "WeightedPool".to_string(),
                financial_type: 0,
                attribute_schema: vec![],
                implementation_type: 0,
            }),
        }
    }

    pub fn pb_blocks(version: u64) -> Block {
        if version == 0 {
            panic!("Block version 0 doesn't exist. It starts at 1");
        }
        let base_ts = yesterday_midnight()
            .and_utc()
            .timestamp() as u64;

        Block {
            number: version,
            hash: Bytes::from(version)
                .lpad(32, 0)
                .to_vec(),
            parent_hash: Bytes::from(version - 1)
                .lpad(32, 0)
                .to_vec(),
            ts: base_ts + version * 1000,
        }
    }

    pub fn pb_transactions(version: u64, index: u64) -> Transaction {
        Transaction {
            hash: Bytes::from(version * 10_000 + index)
                .lpad(32, 0)
                .to_vec(),
            from: Bytes::from(version * 100_000 + index)
                .lpad(20, 0)
                .to_vec(),
            to: Bytes::from(version * 1_000_000 + index)
                .lpad(20, 0)
                .to_vec(),
            index,
        }
    }

    const WETH_ADDRESS: &str = "C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2";
    const USDC_ADDRESS: &str = "A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48";
    const DAI_ADDRESS: &str = "6B175474E89094C44Da98b954EedeAC495271d0F";
    const USDT_ADDRESS: &str = "dAC17F958D2ee523a2206206994597C13D831ec7";

    pub fn address_from_str(token: &str) -> Vec<u8> {
        Bytes::from_str(token)
            .unwrap()
            .0
            .to_vec()
    }

    pub fn pb_block_scoped_data(
        msg: impl prost::Message,
        cursor: Option<&str>,
        final_block_height: Option<u64>,
    ) -> crate::pb::sf::substreams::rpc::v2::BlockScopedData {
        use crate::pb::sf::substreams::{rpc::v2::*, v1::Clock};
        let val = msg.encode_to_vec();
        BlockScopedData {
            output: Some(MapModuleOutput {
                name: "map_changes".to_owned(),
                map_output: Some(prost_types::Any {
                    type_url: "tycho.evm.v1.BlockChanges".to_owned(),
                    value: val,
                }),
                debug_info: None,
            }),
            clock: Some(Clock {
                id: HASH_256_0.to_string(),
                number: 420,
                timestamp: Some(prost_types::Timestamp { seconds: 1000, nanos: 0 }),
            }),
            cursor: cursor
                .unwrap_or("cursor@420")
                .to_owned(),
            final_block_height: final_block_height.unwrap_or(420),
            debug_map_outputs: vec![],
            debug_store_outputs: vec![],
            attestation: "test_attestation".to_owned(),
            is_partial: false,
            partial_index: None,
            is_last_partial: None,
        }
    }

    pub fn pb_vm_block_changes(version: u8) -> BlockChanges {
        match version {
            0 => BlockChanges {
                block: Some(Block {
                    hash: vec![0x31, 0x32, 0x33, 0x34],
                    parent_hash: vec![0x21, 0x22, 0x23, 0x24],
                    number: 1,
                    ts: 1000,
                }),
                changes: vec![
                    TransactionChanges {
                        tx: Some(Transaction {
                            hash: vec![0x11, 0x12, 0x13, 0x14],
                            from: vec![0x41, 0x42, 0x43, 0x44],
                            to: vec![0x51, 0x52, 0x53, 0x54],
                            index: 2,
                        }),
                        contract_changes: vec![ContractChange {
                            address: vec![0x61, 0x62, 0x63, 0x64],
                            balance: vec![0x71, 0x72, 0x73, 0x74],
                            code: vec![0x81, 0x82, 0x83, 0x84],
                            slots: vec![
                                ContractSlot {
                                    slot: vec![0xa1, 0xa2, 0xa3, 0xa4],
                                    value: vec![0xb1, 0xb2, 0xb3, 0xb4],
                                    previous_value: Bytes::new().into(),
                                },
                                ContractSlot {
                                    slot: vec![0xc1, 0xc2, 0xc3, 0xc4],
                                    value: vec![0xd1, 0xd2, 0xd3, 0xd4],
                                    previous_value: Bytes::new().into(),
                                },
                            ],
                            change: ChangeType::Update.into(),
                            token_balances: vec![AccountBalanceChange {
                                token: address_from_str(WETH_ADDRESS),
                                balance: 50000000.encode_to_vec(),
                            }],
                        }],
                        component_changes: vec![ProtocolComponent {
                            id: "d417ff54652c09bd9f31f216b1a2e5d1e28c1dce1ba840c40d16f2b4d09b5902"
                                .to_owned(),
                            tokens: vec![
                                address_from_str(DAI_ADDRESS),
                                address_from_str(DAI_ADDRESS),
                            ],
                            contracts: vec![
                                address_from_str(WETH_ADDRESS),
                                address_from_str(WETH_ADDRESS),
                            ],
                            static_att: vec![
                                Attribute {
                                    name: "key1".to_owned(),
                                    value: b"value1".to_vec(),
                                    change: ChangeType::Creation.into(),
                                },
                                Attribute {
                                    name: "key2".to_owned(),
                                    value: b"value2".to_vec(),
                                    change: ChangeType::Creation.into(),
                                },
                            ],
                            change: ChangeType::Creation.into(),
                            protocol_type: Some(ProtocolType {
                                name: "WeightedPool".to_string(),
                                financial_type: 0,
                                attribute_schema: vec![],
                                implementation_type: 0,
                            }),
                        }],
                        balance_changes: vec![BalanceChange {
                            token: address_from_str(WETH_ADDRESS),
                            balance: 50000000.encode_to_vec(),
                            component_id:
                                "d417ff54652c09bd9f31f216b1a2e5d1e28c1dce1ba840c40d16f2b4d09b5902"
                                    .as_bytes()
                                    .to_vec(),
                        }],
                        ..Default::default()
                    },
                    TransactionChanges {
                        tx: Some(Transaction {
                            hash: vec![
                                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
                            ],
                            from: vec![0x41, 0x42, 0x43, 0x44],
                            to: vec![0x51, 0x52, 0x53, 0x54],
                            index: 5,
                        }),
                        contract_changes: vec![ContractChange {
                            address: vec![0x61, 0x62, 0x63, 0x64],
                            balance: vec![0xf1, 0xf2, 0xf3, 0xf4],
                            code: vec![0x01, 0x02, 0x03, 0x04],
                            slots: vec![
                                ContractSlot {
                                    slot: vec![0x91, 0x92, 0x93, 0x94],
                                    value: vec![0xa1, 0xa2, 0xa3, 0xa4],
                                    previous_value: Bytes::new().into(),
                                },
                                ContractSlot {
                                    slot: vec![0xa1, 0xa2, 0xa3, 0xa4],
                                    value: vec![0xc1, 0xc2, 0xc3, 0xc4],
                                    previous_value: Bytes::new().into(),
                                },
                            ],
                            change: ChangeType::Update.into(),
                            token_balances: vec![AccountBalanceChange {
                                token: address_from_str(WETH_ADDRESS),
                                balance: 10.encode_to_vec(),
                            }],
                        }],
                        balance_changes: vec![BalanceChange {
                            token: address_from_str(WETH_ADDRESS),
                            balance: 10.encode_to_vec(),
                            component_id:
                                "d417ff54652c09bd9f31f216b1a2e5d1e28c1dce1ba840c40d16f2b4d09b5902"
                                    .to_string()
                                    .as_bytes()
                                    .to_vec(),
                        }],
                        ..Default::default()
                    },
                ],
                storage_changes: vec![],
            },
            1 => BlockChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![TransactionChanges {
                    tx: Some(pb_transactions(1, 1)),
                    contract_changes: vec![ContractChange {
                        address: address_from_str("0000000000000000000000000000000000000001"),
                        balance: 1_i32.to_be_bytes().to_vec(),
                        code: 123_i32.to_be_bytes().to_vec(),
                        slots: vec![ContractSlot {
                            slot: Bytes::from("0x01").into(),
                            value: Bytes::from("0x01").into(),
                            previous_value: Bytes::new().into(),
                        }],
                        change: ChangeType::Creation.into(),
                        token_balances: vec![
                            AccountBalanceChange {
                                token: address_from_str(WETH_ADDRESS),
                                balance: 1_i32.to_be_bytes().to_vec(),
                            },
                            AccountBalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 1_i32.to_be_bytes().to_vec(),
                            },
                        ],
                    }],
                    component_changes: vec![ProtocolComponent {
                        id: "pc_1".to_owned(),
                        tokens: vec![
                            address_from_str(WETH_ADDRESS),
                            address_from_str(USDC_ADDRESS),
                        ],
                        contracts: vec![],
                        static_att: vec![],
                        change: ChangeType::Creation.into(),
                        protocol_type: Some(ProtocolType {
                            name: "pt_1".to_string(),
                            financial_type: 0,
                            attribute_schema: vec![],
                            implementation_type: 0,
                        }),
                    }],
                    balance_changes: vec![
                        BalanceChange {
                            token: address_from_str(USDC_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_1".into(),
                        },
                        BalanceChange {
                            token: address_from_str(WETH_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_1".into(),
                        },
                    ],
                    ..Default::default()
                }],
                storage_changes: vec![],
            },
            2 => BlockChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![TransactionChanges {
                    tx: Some(pb_transactions(2, 1)),
                    contract_changes: vec![
                        ContractChange {
                            address: address_from_str("0000000000000000000000000000000000000002"),
                            balance: 2_i32.to_be_bytes().to_vec(),
                            code: 123_i32.to_be_bytes().to_vec(),
                            slots: vec![ContractSlot {
                                slot: Bytes::from("0x01").into(),
                                value: Bytes::from("0x02").into(),
                                previous_value: Bytes::new().into(),
                            }],
                            change: ChangeType::Creation.into(),
                            token_balances: vec![
                                AccountBalanceChange {
                                    token: address_from_str(USDT_ADDRESS),
                                    balance: 20_i32.to_be_bytes().to_vec(),
                                },
                                AccountBalanceChange {
                                    token: address_from_str(USDC_ADDRESS),
                                    balance: 20_i32.to_be_bytes().to_vec(),
                                },
                            ],
                        },
                        ContractChange {
                            address: address_from_str("0000000000000000000000000000000000000001"),
                            balance: 10_i32.to_be_bytes().to_vec(),
                            code: 123_i32.to_be_bytes().to_vec(),
                            slots: vec![
                                ContractSlot {
                                    slot: Bytes::from("0x01").into(),
                                    value: Bytes::from("0x10").into(),
                                    previous_value: Bytes::new().into(),
                                },
                                ContractSlot {
                                    slot: Bytes::from("0x02").into(),
                                    value: Bytes::from("0x0a").into(),
                                    previous_value: Bytes::new().into(),
                                },
                            ],
                            change: ChangeType::Update.into(),
                            token_balances: vec![AccountBalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 10_i32.to_be_bytes().to_vec(),
                            }],
                        },
                    ],
                    component_changes: vec![ProtocolComponent {
                        id: "pc_2".to_owned(),
                        tokens: vec![
                            address_from_str(USDT_ADDRESS),
                            address_from_str(USDC_ADDRESS),
                        ],
                        contracts: vec![address_from_str(
                            "0000000000000000000000000000000000000002",
                        )],
                        static_att: vec![],
                        change: ChangeType::Creation.into(),
                        protocol_type: Some(ProtocolType {
                            name: "pt_1".to_string(),
                            financial_type: 0,
                            attribute_schema: vec![],
                            implementation_type: 0,
                        }),
                    }],
                    balance_changes: vec![
                        BalanceChange {
                            token: address_from_str(USDC_ADDRESS),
                            balance: 20_i32.to_be_bytes().to_vec(),
                            component_id: "pc_2".into(),
                        },
                        BalanceChange {
                            token: address_from_str(USDT_ADDRESS),
                            balance: 20_i32.to_be_bytes().to_vec(),
                            component_id: "pc_2".into(),
                        },
                        BalanceChange {
                            token: address_from_str(USDC_ADDRESS),
                            balance: 10_i32.to_be_bytes().to_vec(),
                            component_id: "pc_1".into(),
                        },
                    ],
                    ..Default::default()
                }],
                storage_changes: vec![],
            },
            3 => BlockChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![
                    TransactionChanges {
                        tx: Some(pb_transactions(3, 1)),
                        contract_changes: vec![
                            ContractChange {
                                address: address_from_str(
                                    "0000000000000000000000000000000000000001",
                                ),
                                balance: 1_i32.to_be_bytes().to_vec(),
                                code: 123_i32.to_be_bytes().to_vec(),
                                slots: vec![ContractSlot {
                                    slot: Bytes::from("0x01").into(),
                                    value: Bytes::from("0x01").into(),
                                    previous_value: Bytes::new().into(),
                                }],
                                change: ChangeType::Update.into(),
                                token_balances: vec![],
                            },
                            ContractChange {
                                address: address_from_str(
                                    "0000000000000000000000000000000000000002",
                                ),
                                balance: 20_i32.to_be_bytes().to_vec(),
                                code: 123_i32.to_be_bytes().to_vec(),
                                slots: vec![ContractSlot {
                                    slot: Bytes::from("0x02").into(),
                                    value: Bytes::from("0xc8").into(),
                                    previous_value: Bytes::new().into(),
                                }],
                                change: ChangeType::Update.into(),
                                token_balances: vec![AccountBalanceChange {
                                    token: address_from_str(USDC_ADDRESS),
                                    balance: 1_i32.to_be_bytes().to_vec(),
                                }],
                            },
                        ],
                        balance_changes: vec![BalanceChange {
                            token: address_from_str(USDC_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_2".into(),
                        }],
                        ..Default::default()
                    },
                    TransactionChanges {
                        tx: Some(pb_transactions(3, 2)),
                        contract_changes: vec![ContractChange {
                            address: address_from_str("0000000000000000000000000000000000000001"),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            code: 123_i32.to_be_bytes().to_vec(),
                            slots: vec![ContractSlot {
                                slot: Bytes::from("0x01").into(),
                                value: Bytes::from("0x01").into(),
                                previous_value: Bytes::new().into(),
                            }],
                            change: ChangeType::Update.into(),
                            token_balances: vec![AccountBalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 100_i32.to_be_bytes().to_vec(),
                            }],
                        }],
                        balance_changes: vec![
                            BalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 100_i32.to_be_bytes().to_vec(),
                                component_id: "pc_1".into(),
                            },
                            BalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 2_i32.to_be_bytes().to_vec(),
                                component_id: "pc_2".into(),
                            },
                        ],
                        ..Default::default()
                    },
                ],
                storage_changes: vec![],
            },
            4 => BlockChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![TransactionChanges {
                    tx: Some(pb_transactions(4, 1)),
                    contract_changes: vec![ContractChange {
                        address: address_from_str("0000000000000000000000000000000000000001"),
                        balance: 1_i32.to_be_bytes().to_vec(),
                        code: 123_i32.to_be_bytes().to_vec(),
                        slots: vec![ContractSlot {
                            slot: Bytes::from("0x03").into(),
                            value: Bytes::from("0x10").into(),
                            previous_value: Bytes::new().into(),
                        }],
                        change: ChangeType::Update.into(),
                        token_balances: vec![],
                    }],
                    component_changes: vec![ProtocolComponent {
                        id: "pc_3".to_owned(),
                        tokens: vec![address_from_str(DAI_ADDRESS), address_from_str(USDC_ADDRESS)],
                        contracts: vec![address_from_str(
                            "0000000000000000000000000000000000000001",
                        )],
                        static_att: vec![],
                        change: ChangeType::Creation.into(),
                        protocol_type: Some(ProtocolType {
                            name: "pt_1".to_string(),
                            financial_type: 0,
                            attribute_schema: vec![],
                            implementation_type: 0,
                        }),
                    }],
                    ..Default::default()
                }],
                storage_changes: vec![],
            },
            5 => BlockChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![TransactionChanges {
                    tx: Some(pb_transactions(5, 1)),
                    contract_changes: vec![
                        ContractChange {
                            address: address_from_str("0000000000000000000000000000000000000001"),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            code: 123_i32.to_be_bytes().to_vec(),
                            slots: vec![ContractSlot {
                                slot: Bytes::from("0x01").into(),
                                value: Bytes::from("0x10").into(),
                                previous_value: Bytes::new().into(),
                            }],
                            change: ChangeType::Update.into(),
                            token_balances: vec![
                                AccountBalanceChange {
                                    token: address_from_str(USDC_ADDRESS),
                                    balance: 100_i32.to_be_bytes().to_vec(),
                                },
                                AccountBalanceChange {
                                    token: address_from_str(WETH_ADDRESS),
                                    balance: 100_i32.to_be_bytes().to_vec(),
                                },
                            ],
                        },
                        ContractChange {
                            address: address_from_str("0000000000000000000000000000000000000002"),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            code: 123_i32.to_be_bytes().to_vec(),
                            slots: vec![ContractSlot {
                                slot: Bytes::from("0x01").into(),
                                value: Bytes::from("0x10").into(),
                                previous_value: Bytes::new().into(),
                            }],
                            change: ChangeType::Update.into(),
                            token_balances: vec![AccountBalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 1_i32.to_be_bytes().to_vec(),
                            }],
                        },
                    ],
                    balance_changes: vec![
                        BalanceChange {
                            token: address_from_str(USDC_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_1".into(),
                        },
                        BalanceChange {
                            token: address_from_str(USDC_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_3".into(),
                        },
                        BalanceChange {
                            token: address_from_str(WETH_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_1".into(),
                        },
                    ],
                    ..Default::default()
                }],
                storage_changes: vec![],
            },
            _ => panic!("Requested BlockChanges version doesn't exist"),
        }
    }

    pub fn pb_native_block_changes(version: u8) -> BlockChanges {
        match version {
            0 => BlockChanges {
                block: Some(Block {
                    hash: vec![0x0, 0x0, 0x0, 0x0],
                    parent_hash: vec![0x21, 0x22, 0x23, 0x24],
                    number: 1,
                    ts: yesterday_midnight()
                        .and_utc()
                        .timestamp() as u64,
                }),
                changes: vec![
                    TransactionChanges {
                        tx: Some(Transaction {
                            hash: vec![0x0, 0x0, 0x0, 0x0],
                            from: vec![0x0, 0x0, 0x0, 0x0],
                            to: vec![0x0, 0x0, 0x0, 0x0],
                            index: 10,
                        }),
                        entity_changes: vec![
                            EntityChanges {
                                component_id: "State1".to_owned(),
                                attributes: vec![
                                    Attribute {
                                        name: "reserve".to_owned(),
                                        value: Bytes::from(1_000u64)
                                            .lpad(32, 0)
                                            .to_vec(),
                                        change: ChangeType::Update.into(),
                                    },
                                    Attribute {
                                        name: "static_attribute".to_owned(),
                                        value: Bytes::from(1u64).lpad(32, 0).to_vec(),
                                        change: ChangeType::Update.into(),
                                    },
                                ],
                            },
                            EntityChanges {
                                component_id: "State2".to_owned(),
                                attributes: vec![
                                    Attribute {
                                        name: "reserve".to_owned(),
                                        value: Bytes::from(1_000u64)
                                            .lpad(32, 0)
                                            .to_vec(),
                                        change: ChangeType::Update.into(),
                                    },
                                    Attribute {
                                        name: "static_attribute".to_owned(),
                                        value: Bytes::from(1u64).lpad(32, 0).to_vec(),
                                        change: ChangeType::Update.into(),
                                    },
                                ],
                            },
                        ],
                        ..Default::default()
                    },
                    TransactionChanges {
                        tx: Some(Transaction {
                            hash: vec![0x11, 0x12, 0x13, 0x14],
                            from: vec![0x41, 0x42, 0x43, 0x44],
                            to: vec![0x51, 0x52, 0x53, 0x54],
                            index: 11,
                        }),
                        entity_changes: vec![EntityChanges {
                            component_id: "State1".to_owned(),
                            attributes: vec![
                                Attribute {
                                    name: "reserve".to_owned(),
                                    value: Bytes::from(600u64).lpad(32, 0).to_vec(),
                                    change: ChangeType::Update.into(),
                                },
                                Attribute {
                                    name: "new".to_owned(),
                                    value: Bytes::zero(32).to_vec(),
                                    change: ChangeType::Update.into(),
                                },
                            ],
                        }],
                        component_changes: vec![ProtocolComponent {
                            id: "Pool".to_owned(),
                            tokens: vec![
                                address_from_str(DAI_ADDRESS),
                                address_from_str(DAI_ADDRESS),
                            ],
                            contracts: vec![address_from_str(WETH_ADDRESS)],
                            static_att: vec![Attribute {
                                name: "key".to_owned(),
                                value: Bytes::from(600u64).lpad(32, 0).to_vec(),
                                change: ChangeType::Creation.into(),
                            }],
                            change: ChangeType::Creation.into(),
                            protocol_type: Some(ProtocolType {
                                name: "WeightedPool".to_string(),
                                financial_type: 0,
                                attribute_schema: vec![],
                                implementation_type: 0,
                            }),
                        }],
                        balance_changes: vec![BalanceChange {
                            token: address_from_str(DAI_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "Balance1".into(),
                        }],
                        ..Default::default()
                    },
                ],
                storage_changes: vec![],
            },
            1 => BlockChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![TransactionChanges {
                    tx: Some(pb_transactions(1, 1)),
                    entity_changes: vec![EntityChanges {
                        component_id: "pc_1".to_owned(),
                        attributes: vec![
                            Attribute {
                                name: "attr_1".to_owned(),
                                value: Bytes::from(1u64).lpad(32, 0).to_vec(),
                                change: ChangeType::Update.into(),
                            },
                            Attribute {
                                name: "attr_2".to_owned(),
                                value: Bytes::from(2u64).lpad(32, 0).to_vec(),
                                change: ChangeType::Update.into(),
                            },
                        ],
                    }],
                    component_changes: vec![ProtocolComponent {
                        id: "pc_1".to_owned(),
                        tokens: vec![
                            address_from_str(WETH_ADDRESS),
                            address_from_str(USDC_ADDRESS),
                        ],
                        contracts: vec![],
                        static_att: vec![Attribute {
                            name: "st_attr_1".to_owned(),
                            value: Bytes::from(1u64).lpad(32, 0).to_vec(),
                            change: ChangeType::Creation.into(),
                        }],
                        change: ChangeType::Creation.into(),
                        protocol_type: Some(ProtocolType {
                            name: "pt_1".to_string(),
                            financial_type: 0,
                            attribute_schema: vec![],
                            implementation_type: 0,
                        }),
                    }],
                    balance_changes: vec![BalanceChange {
                        token: address_from_str(USDC_ADDRESS),
                        balance: 1_i32.to_be_bytes().to_vec(),
                        component_id: "pc_1".into(),
                    }],
                    ..Default::default()
                }],
                storage_changes: vec![],
            },
            2 => BlockChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![TransactionChanges {
                    tx: Some(pb_transactions(2, 1)),
                    entity_changes: vec![EntityChanges {
                        component_id: "pc_1".to_owned(),
                        attributes: vec![Attribute {
                            name: "attr_1".to_owned(),
                            value: Bytes::from(10u64).lpad(32, 0).to_vec(),
                            change: ChangeType::Update.into(),
                        }],
                    }],
                    component_changes: vec![ProtocolComponent {
                        id: "pc_2".to_owned(),
                        tokens: vec![
                            address_from_str(USDT_ADDRESS),
                            address_from_str(USDC_ADDRESS),
                        ],
                        contracts: vec![],
                        static_att: vec![],
                        change: ChangeType::Creation.into(),
                        protocol_type: Some(ProtocolType {
                            name: "pt_1".to_string(),
                            financial_type: 0,
                            attribute_schema: vec![],
                            implementation_type: 0,
                        }),
                    }],
                    balance_changes: vec![
                        BalanceChange {
                            token: address_from_str(USDC_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_2".into(),
                        },
                        BalanceChange {
                            token: address_from_str(USDT_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_2".into(),
                        },
                        BalanceChange {
                            token: address_from_str(WETH_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_1".into(),
                        },
                    ],
                    ..Default::default()
                }],
                storage_changes: vec![],
            },
            3 => BlockChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![
                    TransactionChanges {
                        tx: Some(pb_transactions(3, 2)),
                        entity_changes: vec![EntityChanges {
                            component_id: "pc_1".to_owned(),
                            attributes: vec![Attribute {
                                name: "attr_1".to_owned(),
                                value: Bytes::from(1_000u64)
                                    .lpad(32, 0)
                                    .to_vec(),
                                change: ChangeType::Update.into(),
                            }],
                        }],
                        balance_changes: vec![BalanceChange {
                            token: address_from_str(USDC_ADDRESS),
                            balance: 3_i32.to_be_bytes().to_vec(),
                            component_id: "pc_2".into(),
                        }],
                        ..Default::default()
                    },
                    TransactionChanges {
                        tx: Some(pb_transactions(3, 1)),
                        entity_changes: vec![EntityChanges {
                            component_id: "pc_1".to_owned(),
                            attributes: vec![Attribute {
                                name: "attr_1".to_owned(),
                                value: Bytes::from(99_999u64)
                                    .lpad(32, 0)
                                    .to_vec(),
                                change: ChangeType::Update.into(),
                            }],
                        }],
                        balance_changes: vec![
                            BalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 99999_i32.to_be_bytes().to_vec(),
                                component_id: "pc_2".into(),
                            },
                            BalanceChange {
                                token: address_from_str(WETH_ADDRESS),
                                balance: 1000_i32.to_be_bytes().to_vec(),
                                component_id: "pc_1".into(),
                            },
                        ],
                        ..Default::default()
                    },
                ],
                storage_changes: vec![],
            },
            4 => BlockChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![
                    TransactionChanges {
                        tx: Some(pb_transactions(4, 1)),
                        entity_changes: vec![
                            EntityChanges {
                                component_id: "pc_1".to_owned(),
                                attributes: vec![Attribute {
                                    name: "attr_1".to_owned(),
                                    value: Bytes::from(10_000u64)
                                        .lpad(32, 0)
                                        .to_vec(),
                                    change: ChangeType::Update.into(),
                                }],
                            },
                            EntityChanges {
                                component_id: "pc_3".to_owned(),
                                attributes: vec![Attribute {
                                    name: "attr_1".to_owned(),
                                    value: Bytes::from(3u64).lpad(32, 0).to_vec(),
                                    change: ChangeType::Update.into(),
                                }],
                            },
                        ],
                        component_changes: vec![ProtocolComponent {
                            id: "pc_3".to_owned(),
                            tokens: vec![
                                address_from_str(DAI_ADDRESS),
                                address_from_str(WETH_ADDRESS),
                            ],
                            contracts: vec![],
                            static_att: vec![],
                            change: ChangeType::Creation.into(),
                            protocol_type: Some(ProtocolType {
                                name: "pt_2".to_string(),
                                financial_type: 0,
                                attribute_schema: vec![],
                                implementation_type: 0,
                            }),
                        }],
                        ..Default::default()
                    },
                    TransactionChanges {
                        tx: Some(pb_transactions(4, 2)),
                        entity_changes: vec![
                            EntityChanges {
                                component_id: "pc_3".to_owned(),
                                attributes: vec![Attribute {
                                    name: "attr_1".to_owned(),
                                    value: Bytes::from(30u64).lpad(32, 0).to_vec(),
                                    change: ChangeType::Update.into(),
                                }],
                            },
                            EntityChanges {
                                component_id: "pc_1".to_owned(),
                                attributes: vec![Attribute {
                                    name: "attr_1".to_owned(),
                                    value: Bytes::from(100_000u64)
                                        .lpad(32, 0)
                                        .to_vec(),
                                    change: ChangeType::Update.into(),
                                }],
                            },
                        ],
                        balance_changes: vec![
                            BalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 3000_i32.to_be_bytes().to_vec(),
                                component_id: "pc_3".into(),
                            },
                            BalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 1000_i32.to_be_bytes().to_vec(),
                                component_id: "pc_1".into(),
                            },
                        ],
                        ..Default::default()
                    },
                ],
                storage_changes: vec![],
            },
            5 => BlockChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![TransactionChanges {
                    tx: Some(pb_transactions(5, 1)),
                    entity_changes: vec![EntityChanges {
                        component_id: "pc_1".to_owned(),
                        attributes: vec![Attribute {
                            name: "attr_2".to_owned(),
                            value: Bytes::from(1_000_000u64)
                                .lpad(32, 0)
                                .to_vec(),
                            change: ChangeType::Update.into(),
                        }],
                    }],
                    component_changes: vec![ProtocolComponent {
                        id: "pc_2".to_owned(),
                        tokens: vec![
                            address_from_str(USDT_ADDRESS),
                            address_from_str(USDC_ADDRESS),
                        ],
                        contracts: vec![],
                        static_att: vec![],
                        change: ChangeType::Deletion.into(),
                        protocol_type: Some(ProtocolType {
                            name: "pt_1".to_string(),
                            financial_type: 0,
                            attribute_schema: vec![],
                            implementation_type: 0,
                        }),
                    }],
                    balance_changes: vec![BalanceChange {
                        token: address_from_str(WETH_ADDRESS),
                        balance: 1000_i32.to_be_bytes().to_vec(),
                        component_id: "pc_1".into(),
                    }],
                    ..Default::default()
                }],
                storage_changes: vec![],
            },
            _ => panic!("Requested unknown version of block entity changes"),
        }
    }

    pub fn pb_transaction_storage_changes(version: u8) -> TransactionStorageChanges {
        match version {
            0 => TransactionStorageChanges {
                tx: Some(pb_transactions(0, 1)),
                storage_changes: vec![
                    StorageChanges {
                        address: address_from_str("0000000000000000000000000000000000000001"),
                        slots: vec![
                            ContractSlot {
                                slot: Bytes::from("0x01").into(),
                                value: Bytes::from("0x01").into(),
                                previous_value: Bytes::new().into(),
                            },
                            ContractSlot {
                                slot: Bytes::from("0x02").into(),
                                value: Bytes::from("0x02").into(),
                                previous_value: Bytes::new().into(),
                            },
                        ],
                        native_balance: None,
                    },
                    StorageChanges {
                        address: address_from_str("0000000000000000000000000000000000000002"),
                        slots: vec![ContractSlot {
                            slot: Bytes::from("0x03").into(),
                            value: Bytes::from("0x03").into(),
                            previous_value: Bytes::new().into(),
                        }],
                        native_balance: Some(Bytes::from(1000u64).to_vec()),
                    },
                ],
            },
            1 => TransactionStorageChanges {
                tx: Some(pb_transactions(1, 2)),
                storage_changes: vec![
                    StorageChanges {
                        address: address_from_str("0000000000000000000000000000000000000001"),
                        slots: vec![ContractSlot {
                            slot: Bytes::from("0x01").into(),
                            value: Bytes::from("0x04").into(),
                            previous_value: Bytes::new().into(),
                        }],
                        native_balance: None,
                    },
                    StorageChanges {
                        address: address_from_str("0000000000000000000000000000000000000002"),
                        slots: vec![ContractSlot {
                            slot: Bytes::from("0x05").into(),
                            value: Bytes::from("0x05").into(),
                            previous_value: Bytes::new().into(),
                        }],
                        native_balance: None,
                    },
                ],
            },
            _ => panic!("Requested TransactionStorageChanges version doesn't exist"),
        }
    }
}
