use std::collections::HashMap;
use std::sync::Arc;

use prost::Message;
use tokio::sync::mpsc;
use tycho_common::models::{Chain, FinancialType};
use tycho_substreams::pb::tycho::evm::v1 as substreams;

use super::*;
use crate::{
    extractor::{
        control::BranchSubscriptionsMap,
        extractor_config::{BootstrapConfig, ExtractorConfig, ProtocolTypeConfig},
        family_dispatch::{FamilyBlockChangesDispatcher, FamilyBranchSpec},
        family_registry::default_family_runtime_registry,
        family_runtime::{
            resolved_family_runtime_from_extractor_configs_for_tests,
            FamilyRuntimeMembershipView, ResolvedFamilyRuntime,
        },
        family_runtime_execution::FamilyRuntimeState,
        family_runtime_metadata::ResolvedSharedFamilyStream,
        family_runtime_resolution::{
            validate_family_runtime_membership, ResolvedFamilyRuntimeContract,
        },
        protocol_cache::ProtocolMemoryCache,
        ExtractionError, Extractor,
    },
    pb::sf::substreams::rpc::v2::BlockScopedData,
    pb::sf::substreams::v1::Clock,
    substreams::stream::SubstreamsStream,
    testing::{family_output_module_for_tests, MockGateway},
};

pub(super) fn uniswap_shared_stream_for_tests(shared_spkg: &str) -> ResolvedSharedFamilyStream {
    default_family_runtime_registry()
        .resolved_shared_stream_for_family(Chain::Ethereum, "uniswap", shared_spkg)
        .expect("registered uniswap shared stream")
}

pub(super) fn family_runtime_state_for_tests(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    dispatcher: FamilyBlockChangesDispatcher,
) -> FamilyRuntimeState {
    let protocol_cache = ProtocolMemoryCache::new(
        Chain::Ethereum,
        chrono::Duration::seconds(60),
        Arc::new(MockGateway::new()),
    );
    let runtime_contract = family_runtime_contract_for_test_extractors(extractors);
    FamilyRuntimeState::new(&runtime_contract, extractors, dispatcher, protocol_cache)
}

fn family_runtime_contract_for_test_extractors(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
) -> ResolvedFamilyRuntimeContract {
    let branch_specs = extractors
        .keys()
        .cloned()
        .map(|protocol_system| FamilyBranchSpec {
            protocol_system,
            protocol_type_names: Default::default(),
        })
        .collect::<Vec<_>>();
    let shared_progress_owner_protocol_system =
        crate::extractor::family_registry::default_family_runtime_registry()
            .shared_progress_owner_protocol_system_for_family("uniswap")
            .map(str::to_string)
            .or_else(|| {
                branch_specs
                    .first()
                    .map(|branch| branch.protocol_system.clone())
            })
            .expect("family runtime test helper requires at least one branch");
    ResolvedFamilyRuntimeContract::new(
        uniswap_shared_stream_for_tests(""),
        branch_specs,
        shared_progress_owner_protocol_system,
    )
}

pub(super) fn make_uniswap_family_branch_specs() -> [FamilyBranchSpec; 2] {
    [
        FamilyBranchSpec {
            protocol_system: "uniswap_v2".to_string(),
            protocol_type_names: std::collections::HashSet::from(["uniswap_v2_pool".to_string()]),
        },
        FamilyBranchSpec {
            protocol_system: "uniswap_v3".to_string(),
            protocol_type_names: std::collections::HashSet::from(["uniswap_v3_pool".to_string()]),
        },
    ]
}

pub(super) fn make_uniswap_family_dispatcher() -> FamilyBlockChangesDispatcher {
    FamilyBlockChangesDispatcher::new(make_uniswap_family_branch_specs())
        .expect("dispatcher builds")
}

pub(super) fn make_uniswap_family_dispatcher_with_component_systems(
    component_systems: HashMap<String, String>,
) -> FamilyBlockChangesDispatcher {
    let mut dispatcher = make_uniswap_family_dispatcher();
    dispatcher.register_component_systems(component_systems);
    dispatcher
}

pub(super) fn make_uniswap_family_dispatcher_with_contract_systems(
    contract_systems: HashMap<Vec<u8>, String>,
) -> FamilyBlockChangesDispatcher {
    let mut dispatcher = make_uniswap_family_dispatcher();
    dispatcher.register_contract_systems(contract_systems);
    dispatcher
}

pub(super) fn make_uniswap_family_runtime_contract() -> ResolvedFamilyRuntimeContract {
    ResolvedFamilyRuntimeContract::new(
        uniswap_shared_stream_for_tests(""),
        make_uniswap_family_branch_specs().into_iter().collect(),
        "uniswap_v2",
    )
}

pub(super) fn family_runner_for_tests(
    extractors: HashMap<String, Arc<dyn Extractor>>,
    substreams: SubstreamsStream,
    subscriptions: BranchSubscriptionsMap,
    dispatcher: FamilyBlockChangesDispatcher,
) -> FamilyExtractorRunner {
    let runtime_state = family_runtime_state_for_tests(&extractors, dispatcher);
    let runtime_contract = family_runtime_contract_for_test_extractors(&extractors);
    FamilyExtractorRunner::new(
        runtime_contract,
        extractors,
        substreams,
        subscriptions,
        mpsc::channel(4).1,
        None,
        false,
        runtime_state,
    )
}

pub(super) fn validate_family_runner_membership(
    family: &impl FamilyRuntimeMembershipView,
    extractor_configs: &[&ExtractorConfig],
) -> Result<(), ExtractionError> {
    validate_family_runtime_membership(family, extractor_configs)
}

/// Builds minimal BlockScopedData for runner message-selection tests.
pub(super) fn make_block_scoped_data(
    is_partial: bool,
    partial_index: Option<u32>,
    is_last_partial: Option<bool>,
) -> BlockScopedData {
    BlockScopedData {
        output: None,
        clock: None,
        cursor: String::new(),
        final_block_height: 0,
        debug_map_outputs: vec![],
        debug_store_outputs: vec![],
        attestation: String::new(),
        is_partial,
        partial_index,
        is_last_partial,
    }
}

pub(super) fn make_family_block_scoped_data() -> BlockScopedData {
    use crate::pb::sf::substreams::rpc::v2::MapModuleOutput;

    let family_changes = substreams::BlockChanges {
        block: Some(substreams::Block {
            number: 42,
            hash: vec![0x01; 32],
            parent_hash: vec![0x02; 32],
            ts: 1_718_000_000,
        }),
        changes: vec![substreams::TransactionChanges {
            tx: Some(substreams::Transaction {
                hash: vec![0xaa; 32],
                from: vec![0x11; 20],
                to: vec![0x22; 20],
                index: 7,
            }),
            contract_changes: vec![],
            entity_changes: vec![],
            component_changes: vec![
                substreams::ProtocolComponent {
                    id: "v2-pool".to_string(),
                    protocol_type: Some(substreams::ProtocolType {
                        name: "uniswap_v2_pool".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                substreams::ProtocolComponent {
                    id: "v3-pool".to_string(),
                    protocol_type: Some(substreams::ProtocolType {
                        name: "uniswap_v3_pool".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ],
            balance_changes: vec![],
            entrypoints: vec![],
            entrypoint_params: vec![],
        }],
        storage_changes: vec![],
    };

    BlockScopedData {
        output: Some(MapModuleOutput {
            name: family_output_module_for_tests("uniswap"),
            map_output: Some(prost_types::Any {
                type_url: "type.googleapis.com/tycho.evm.v1.BlockChanges".to_string(),
                value: family_changes.encode_to_vec(),
            }),
            debug_info: None,
        }),
        clock: Some(Clock { id: "42".to_string(), number: 42, timestamp: None }),
        cursor: "cursor-42".to_string(),
        final_block_height: 42,
        debug_map_outputs: vec![],
        debug_store_outputs: vec![],
        attestation: String::new(),
        is_partial: false,
        partial_index: None,
        is_last_partial: None,
    }
}

pub(super) fn make_uniswap_family_bootstrap_test_configs() -> [ExtractorConfig; 2] {
    [
        ExtractorConfig {
            substreams_params: make_uniswap_member_substreams_params("uniswap_v2"),
            bootstrap: Some(make_uniswap_member_bootstrap_config("uniswap_v2", 42)),
            ..make_uniswap_member_runtime_test_config("uniswap_v2", "uniswap_v2", 42)
        },
        ExtractorConfig {
            substreams_params: make_uniswap_member_substreams_params("uniswap_v3"),
            bootstrap: Some(make_uniswap_member_bootstrap_config("uniswap_v3", 42)),
            ..make_uniswap_member_runtime_test_config("uniswap_v3", "uniswap_v3", 42)
        },
    ]
}

pub(super) fn make_uniswap_family_runtime_test_configs(
    v2_start_block: i64,
    v3_start_block: i64,
) -> [ExtractorConfig; 2] {
    [
        make_uniswap_member_runtime_test_config("uniswap_v2", "uniswap_v2", v2_start_block),
        make_uniswap_member_runtime_test_config("uniswap_v3", "uniswap_v3", v3_start_block),
    ]
}

pub(super) fn make_uniswap_member_runtime_test_config(
    name: &str,
    protocol_system: &str,
    start_block: i64,
) -> ExtractorConfig {
    ExtractorConfig {
        name: name.to_owned(),
        protocol_system: protocol_system.to_string(),
        start_block,
        protocol_types: vec![ProtocolTypeConfig::new(
            match protocol_system {
                "uniswap_v2" => "uniswap_v2_pool",
                "uniswap_v3" => "uniswap_v3_pool",
                other => panic!("unsupported uniswap-family protocol system `{other}`"),
            }
            .to_string(),
            FinancialType::Swap,
        )],
        ..Default::default()
    }
}

pub(super) fn make_uniswap_member_substreams_params(
    protocol_system: &str,
) -> HashMap<String, String> {
    match protocol_system {
        "uniswap_v2" => HashMap::from([("map_pool_events".to_string(), "factory=0x01".to_string())]),
        "uniswap_v3" => HashMap::from([("map_events".to_string(), "factory=0x02".to_string())]),
        other => panic!("unsupported uniswap-family protocol system `{other}`"),
    }
}

pub(super) fn make_uniswap_member_bootstrap_config(
    protocol_system: &str,
    start_block: i64,
) -> BootstrapConfig {
    match protocol_system {
        "uniswap_v2" => BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV2Rpc,
            start_block,
            params: format!(
                "bootstrap_block={start_block}&pool=0x0000000000000000000000000000000000001234"
            ),
        },
        "uniswap_v3" => BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV3Rpc,
            start_block,
            params: format!(
                "bootstrap_block={start_block}&pool=0x0000000000000000000000000000000000005678"
            ),
        },
        other => panic!("unsupported uniswap-family protocol system `{other}`"),
    }
}

pub(super) fn try_resolved_family_runtime_from_configs_for_tests<'a>(
    extractor_configs: &[&'a ExtractorConfig],
    shared_spkg: &str,
) -> Result<ResolvedFamilyRuntime<'a>, ExtractionError> {
    resolved_family_runtime_from_extractor_configs_for_tests(extractor_configs, shared_spkg)
}

pub(super) fn resolved_family_runtime_from_configs_for_tests<'a>(
    extractor_configs: &[&'a ExtractorConfig],
    shared_spkg: &str,
) -> ResolvedFamilyRuntime<'a> {
    try_resolved_family_runtime_from_configs_for_tests(extractor_configs, shared_spkg)
        .expect("resolved family runtime should derive from test configs")
}

pub(super) fn make_family_follow_up_block_scoped_data(
    block_number: u64,
    cursor: &str,
) -> BlockScopedData {
    use crate::pb::sf::substreams::rpc::v2::MapModuleOutput;

    let family_changes = substreams::BlockChanges {
        block: Some(substreams::Block {
            number: block_number,
            hash: vec![0x04; 32],
            parent_hash: vec![0x01; 32],
            ts: 1_718_000_001,
        }),
        changes: vec![substreams::TransactionChanges {
            tx: Some(substreams::Transaction {
                hash: vec![0xbb; 32],
                from: vec![0x11; 20],
                to: vec![0x22; 20],
                index: 8,
            }),
            contract_changes: vec![],
            entity_changes: vec![
                substreams::EntityChanges {
                    component_id: "v2-pool".to_string(),
                    attributes: vec![],
                },
                substreams::EntityChanges {
                    component_id: "v3-pool".to_string(),
                    attributes: vec![],
                },
            ],
            component_changes: vec![],
            balance_changes: vec![],
            entrypoints: vec![],
            entrypoint_params: vec![],
        }],
        storage_changes: vec![],
    };

    BlockScopedData {
        output: Some(MapModuleOutput {
            name: family_output_module_for_tests("uniswap"),
            map_output: Some(prost_types::Any {
                type_url: "type.googleapis.com/tycho.evm.v1.BlockChanges".to_string(),
                value: family_changes.encode_to_vec(),
            }),
            debug_info: None,
        }),
        clock: Some(Clock { id: block_number.to_string(), number: block_number, timestamp: None }),
        cursor: cursor.to_string(),
        final_block_height: block_number,
        debug_map_outputs: vec![],
        debug_store_outputs: vec![],
        attestation: String::new(),
        is_partial: false,
        partial_index: None,
        is_last_partial: None,
    }
}

pub(super) fn make_family_contract_and_storage_follow_up_block_scoped_data(
    block_number: u64,
    cursor: &str,
) -> BlockScopedData {
    use crate::pb::sf::substreams::rpc::v2::MapModuleOutput;

    let family_changes = substreams::BlockChanges {
        block: Some(substreams::Block {
            number: block_number,
            hash: vec![0x05; 32],
            parent_hash: vec![0x04; 32],
            ts: 1_718_000_002,
        }),
        changes: vec![substreams::TransactionChanges {
            tx: Some(substreams::Transaction {
                hash: vec![0xcc; 32],
                from: vec![0x11; 20],
                to: vec![0x22; 20],
                index: 9,
            }),
            contract_changes: vec![substreams::ContractChange {
                address: vec![0x44; 20],
                balance: vec![],
                code: vec![],
                change: 0,
                slots: vec![],
                token_balances: vec![],
            }],
            entity_changes: vec![],
            component_changes: vec![],
            balance_changes: vec![],
            entrypoints: vec![],
            entrypoint_params: vec![],
        }],
        storage_changes: vec![substreams::TransactionStorageChanges {
            tx: Some(substreams::Transaction {
                hash: vec![0xdd; 32],
                from: vec![0x11; 20],
                to: vec![0x22; 20],
                index: 10,
            }),
            storage_changes: vec![substreams::StorageChanges {
                address: vec![0x55; 20],
                slots: vec![substreams::ContractSlot {
                    slot: vec![0x01],
                    value: vec![0x02],
                    previous_value: vec![],
                }],
                native_balance: None,
            }],
        }],
    };

    BlockScopedData {
        output: Some(MapModuleOutput {
            name: family_output_module_for_tests("uniswap"),
            map_output: Some(prost_types::Any {
                type_url: "type.googleapis.com/tycho.evm.v1.BlockChanges".to_string(),
                value: family_changes.encode_to_vec(),
            }),
            debug_info: None,
        }),
        clock: Some(Clock { id: block_number.to_string(), number: block_number, timestamp: None }),
        cursor: cursor.to_string(),
        final_block_height: block_number,
        debug_map_outputs: vec![],
        debug_store_outputs: vec![],
        attestation: String::new(),
        is_partial: false,
        partial_index: None,
        is_last_partial: None,
    }
}
