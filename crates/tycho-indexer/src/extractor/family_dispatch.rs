use std::collections::{HashMap, HashSet};

use tycho_substreams::pb::tycho::evm::v1 as substreams;

use crate::{
    extractor::{
        extractor_config::ExtractorConfig,
        family_dispatch_payloads::{
            decode_family_block_scoped_data_changes, dispatch_block_scoped_data_by_protocol_system,
            referenced_component_ids_from_block_scoped_data,
            referenced_contract_addresses_from_block_scoped_data,
        },
        family_dispatch_registry::{FamilyDispatchRegistry, FamilyDispatcherSeed},
        family_dispatch_splitter::split_family_block_changes,
        family_runtime_resolution::ResolvedFamilyRuntimeContract,
        protocol_cache::ProtocolMemoryCache,
        ExtractionError,
    },
    pb::sf::substreams::rpc::v2::BlockScopedData,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FamilyBranchSpec {
    pub protocol_system: String,
    pub protocol_type_names: HashSet<String>,
}

pub trait ProtocolSystemBranchView {
    fn protocol_system(&self) -> &str;
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FamilyBranchMembership {
    protocol_systems: HashSet<String>,
    protocol_type_to_system: HashMap<String, String>,
}

impl FamilyBranchSpec {
    pub fn from_extractor_config(config: &ExtractorConfig) -> Result<Self, ExtractionError> {
        if config.protocol_types().is_empty() {
            return Err(ExtractionError::Setup(format!(
                "family branch for `{}` requires at least one protocol type",
                config.name()
            )));
        }

        Ok(Self {
            protocol_system: config.protocol_system().to_string(),
            protocol_type_names: config
                .protocol_types()
                .iter()
                .map(|protocol_type| protocol_type.name().to_string())
                .collect(),
        })
    }

    pub fn from_extractor_configs(
        configs: &[&ExtractorConfig],
    ) -> Result<Vec<Self>, ExtractionError> {
        configs
            .iter()
            .map(|config| Self::from_extractor_config(config))
            .collect()
    }

    pub fn protocol_system_set<'a>(
        branches: impl IntoIterator<Item = &'a FamilyBranchSpec>,
    ) -> HashSet<String> {
        collect_branch_protocol_systems(branches)
    }

    fn resolve_membership<'a>(
        branches: impl IntoIterator<Item = &'a FamilyBranchSpec>,
    ) -> Result<FamilyBranchMembership, ExtractionError> {
        let mut protocol_systems = HashSet::new();
        let mut protocol_type_to_system = HashMap::new();

        for branch in branches {
            protocol_systems.insert(branch.protocol_system.clone());
            for protocol_type_name in &branch.protocol_type_names {
                if let Some(existing) = protocol_type_to_system
                    .insert(protocol_type_name.clone(), branch.protocol_system.clone())
                {
                    return Err(ExtractionError::Setup(format!(
                        "protocol type `{protocol_type_name}` is assigned to both `{existing}` and `{}`",
                        branch.protocol_system
                    )));
                }
            }
        }

        Ok(FamilyBranchMembership { protocol_systems, protocol_type_to_system })
    }

    pub async fn dispatcher_seed_from_protocol_cache(
        branches: &[FamilyBranchSpec],
        protocol_cache: &ProtocolMemoryCache,
    ) -> Result<FamilyDispatcherSeed, ExtractionError> {
        let membership = Self::resolve_membership(branches.iter())?;
        let component_systems = protocol_cache
            .component_protocol_systems(&membership.protocol_systems)
            .await;
        let contract_systems = protocol_cache
            .contract_protocol_systems(&membership.protocol_systems)
            .await
            .into_iter()
            .map(|(contract, protocol_system)| (contract.to_vec(), protocol_system))
            .collect();

        Ok(FamilyDispatcherSeed { component_systems, contract_systems })
    }
}

impl ProtocolSystemBranchView for FamilyBranchSpec {
    fn protocol_system(&self) -> &str {
        &self.protocol_system
    }
}

pub fn collect_branch_protocol_systems<'a, T>(
    branches: impl IntoIterator<Item = &'a T>,
) -> HashSet<String>
where
    T: ProtocolSystemBranchView + 'a,
{
    branches
        .into_iter()
        .map(|branch| branch.protocol_system().to_string())
        .collect()
}

#[derive(Clone, Debug, Default)]
pub struct FamilyBlockChangesDispatcher {
    registry: FamilyDispatchRegistry,
}

impl FamilyBlockChangesDispatcher {
    pub fn new_for_runtime_contract(
        contract: &ResolvedFamilyRuntimeContract,
    ) -> Result<Self, ExtractionError> {
        Self::new(contract.branch_specs().iter().cloned())
    }

    pub fn new(
        branches: impl IntoIterator<Item = FamilyBranchSpec>,
    ) -> Result<Self, ExtractionError> {
        let branches = branches.into_iter().collect::<Vec<_>>();
        let membership = FamilyBranchSpec::resolve_membership(branches.iter())?;

        Ok(Self {
            registry: FamilyDispatchRegistry::new(
                membership.protocol_systems,
                membership.protocol_type_to_system,
            ),
        })
    }

    pub fn new_with_seed(
        branches: impl IntoIterator<Item = FamilyBranchSpec>,
        seed: FamilyDispatcherSeed,
    ) -> Result<Self, ExtractionError> {
        let mut dispatcher = Self::new(branches)?;
        dispatcher.register_component_systems(seed.component_systems);
        dispatcher.register_contract_systems(seed.contract_systems);
        Ok(dispatcher)
    }

    pub fn new_with_seed_for_runtime_contract(
        contract: &ResolvedFamilyRuntimeContract,
        seed: FamilyDispatcherSeed,
    ) -> Result<Self, ExtractionError> {
        Self::new_with_seed(contract.branch_specs().iter().cloned(), seed)
    }

    pub async fn from_protocol_cache(
        branches: &[FamilyBranchSpec],
        protocol_cache: &ProtocolMemoryCache,
    ) -> Result<Self, ExtractionError> {
        let seed =
            FamilyBranchSpec::dispatcher_seed_from_protocol_cache(branches, protocol_cache).await;
        Self::new_with_seed(branches.iter().cloned(), seed?)
    }

    pub async fn from_protocol_cache_for_runtime_contract(
        contract: &ResolvedFamilyRuntimeContract,
        protocol_cache: &ProtocolMemoryCache,
    ) -> Result<Self, ExtractionError> {
        let seed = FamilyBranchSpec::dispatcher_seed_from_protocol_cache(
            contract.branch_specs(),
            protocol_cache,
        )
        .await?;
        Self::new_with_seed_for_runtime_contract(contract, seed)
    }

    pub async fn hydrate_from_protocol_cache_by_component_ids(
        &mut self,
        protocol_cache: &ProtocolMemoryCache,
        component_ids: &HashSet<String>,
    ) -> Result<bool, ExtractionError> {
        self.registry
            .hydrate_from_protocol_cache_by_component_ids(protocol_cache, component_ids)
            .await
    }

    pub async fn hydrate_from_protocol_cache_by_contract_addresses(
        &mut self,
        protocol_cache: &ProtocolMemoryCache,
        contract_addresses: &HashSet<Vec<u8>>,
    ) -> Result<bool, ExtractionError> {
        self.registry
            .hydrate_from_protocol_cache_by_contract_addresses(protocol_cache, contract_addresses)
            .await
    }

    pub async fn dispatch_block_scoped_data_with_protocol_cache_fallback(
        &mut self,
        block_scoped_data: BlockScopedData,
        protocol_cache: &ProtocolMemoryCache,
    ) -> Result<HashMap<String, BlockScopedData>, ExtractionError> {
        match self.dispatch_block_scoped_data(block_scoped_data.clone()) {
            Ok(dispatched) => Ok(dispatched),
            Err(err) => {
                if !Self::should_hydrate_from_dispatch_error(&err) {
                    return Err(err);
                }

                let referenced_component_ids =
                    referenced_component_ids_from_block_scoped_data(&block_scoped_data)?;
                let referenced_contract_addresses =
                    referenced_contract_addresses_from_block_scoped_data(&block_scoped_data)?;
                if referenced_component_ids.is_empty() && referenced_contract_addresses.is_empty() {
                    return Err(err);
                }

                let hydrated_components = if referenced_component_ids.is_empty() {
                    false
                } else {
                    self.hydrate_from_protocol_cache_by_component_ids(
                        protocol_cache,
                        &referenced_component_ids,
                    )
                    .await?
                };

                let hydrated_contracts = if referenced_contract_addresses.is_empty() {
                    false
                } else {
                    self.hydrate_from_protocol_cache_by_contract_addresses(
                        protocol_cache,
                        &referenced_contract_addresses,
                    )
                    .await?
                };

                if !(hydrated_components || hydrated_contracts) {
                    return Err(err);
                }

                self.dispatch_block_scoped_data(block_scoped_data)
            }
        }
    }

    fn should_hydrate_from_dispatch_error(err: &ExtractionError) -> bool {
        matches!(
            err,
            ExtractionError::DecodeError(message) if message.contains("unknown component `")
                || message.contains("unable to route contract changes")
                || message.contains("unable to route storage changes")
        )
    }

    pub fn register_component_system(
        &mut self,
        component_id: impl Into<String>,
        protocol_system: impl Into<String>,
    ) {
        self.registry
            .register_component_system(component_id, protocol_system);
    }

    pub fn register_component_systems(
        &mut self,
        component_systems: impl IntoIterator<Item = (String, String)>,
    ) {
        self.registry
            .register_component_systems(component_systems);
    }

    pub fn register_contract_system(
        &mut self,
        contract_address: impl Into<Vec<u8>>,
        protocol_system: impl Into<String>,
    ) {
        self.registry
            .register_contract_system(contract_address, protocol_system);
    }

    pub fn register_contract_systems(
        &mut self,
        contract_systems: impl IntoIterator<Item = (Vec<u8>, String)>,
    ) {
        self.registry
            .register_contract_systems(contract_systems);
    }

    pub fn component_system(&self, component_id: &str) -> Option<&String> {
        self.registry
            .component_system(component_id)
    }

    pub fn contract_system(&self, contract_address: &[u8]) -> Option<&String> {
        self.registry
            .contract_system(contract_address)
    }

    pub fn dispatch_block_changes(
        &mut self,
        msg: substreams::BlockChanges,
    ) -> Result<HashMap<String, substreams::BlockChanges>, ExtractionError> {
        split_family_block_changes(&mut self.registry, msg)
    }

    pub fn dispatch_block_scoped_data(
        &mut self,
        block_scoped_data: BlockScopedData,
    ) -> Result<HashMap<String, BlockScopedData>, ExtractionError> {
        let raw_msg = decode_family_block_scoped_data_changes(&block_scoped_data)?;
        let dispatched = self.dispatch_block_changes(raw_msg)?;
        dispatch_block_scoped_data_by_protocol_system(block_scoped_data, dispatched)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use chrono::Duration;
    use prost::Message;
    use tycho_common::models::{Chain, FinancialType, ImplementationType};
    use tycho_substreams::pb::tycho::evm::v1 as substreams;

    use crate::extractor::{
        extractor_config::{ExtractorConfig, ProtocolTypeConfig},
        family_runtime_resolution::ResolvedFamilyRuntimeContract,
        protocol_cache::{ProtocolDataCache, ProtocolMemoryCache},
    };
    use crate::pb::sf::substreams::{
        rpc::v2::{BlockScopedData, MapModuleOutput},
        v1::Clock,
    };
    use crate::testing::MockGateway;

    use super::{FamilyBlockChangesDispatcher, FamilyBranchSpec, FamilyDispatcherSeed};

    fn branch(protocol_system: &str, protocol_type_name: &str) -> FamilyBranchSpec {
        FamilyBranchSpec {
            protocol_system: protocol_system.to_string(),
            protocol_type_names: HashSet::from([protocol_type_name.to_string()]),
        }
    }

    fn test_block() -> substreams::Block {
        substreams::Block {
            number: 42,
            hash: vec![0x01; 32],
            parent_hash: vec![0x02; 32],
            ts: 1_718_000_000,
        }
    }

    fn test_tx() -> substreams::Transaction {
        substreams::Transaction {
            hash: vec![0xaa; 32],
            from: vec![0x11; 20],
            to: vec![0x22; 20],
            index: 7,
        }
    }

    fn test_storage_change(address: Vec<u8>) -> substreams::StorageChanges {
        substreams::StorageChanges {
            address,
            slots: vec![substreams::ContractSlot {
                slot: vec![0x01],
                value: vec![0x02],
                previous_value: vec![],
            }],
            native_balance: None,
        }
    }

    fn test_contract_change(address: Vec<u8>) -> substreams::ContractChange {
        substreams::ContractChange {
            address,
            balance: vec![],
            code: vec![],
            change: 0,
            slots: vec![],
            token_balances: vec![],
        }
    }

    fn family_block_scoped_data_with_component_balance(
        component_id: &str,
        token: Vec<u8>,
        block_number: u64,
        cursor: &str,
    ) -> BlockScopedData {
        let block = substreams::Block {
            number: block_number,
            hash: vec![0x01; 32],
            parent_hash: vec![0x02; 32],
            ts: 1_718_000_000 + block_number,
        };
        let tx = substreams::Transaction {
            hash: vec![0xaa; 32],
            from: vec![0x11; 20],
            to: vec![0x22; 20],
            index: 0,
        };
        let block_changes = substreams::BlockChanges {
            block: Some(block),
            changes: vec![substreams::TransactionChanges {
                tx: Some(tx),
                contract_changes: vec![],
                entity_changes: vec![],
                component_changes: vec![],
                balance_changes: vec![substreams::BalanceChange {
                    component_id: component_id.as_bytes().to_vec(),
                    token,
                    balance: vec![0x01],
                }],
                entrypoints: vec![],
                entrypoint_params: vec![],
            }],
            storage_changes: vec![],
        };

        BlockScopedData {
            output: Some(MapModuleOutput {
                name: "map_family_protocol_changes".to_string(),
                map_output: Some(prost_types::Any {
                    type_url: "type.googleapis.com/tycho.evm.v1.BlockChanges".to_string(),
                    value: block_changes.encode_to_vec(),
                }),
                debug_info: None,
            }),
            clock: Some(Clock {
                id: block_number.to_string(),
                number: block_number,
                timestamp: None,
            }),
            cursor: cursor.to_string(),
            final_block_height: block_number,
            debug_map_outputs: vec![],
            debug_store_outputs: vec![],
            attestation: "test_attestation".to_string(),
            is_partial: false,
            partial_index: None,
            is_last_partial: None,
        }
    }

    #[test]
    fn derives_family_branch_spec_from_extractor_config() {
        let config = ExtractorConfig::new(
            "uniswap_v2_indexer".to_string(),
            Chain::Ethereum,
            ImplementationType::Vm,
            100,
            42,
            None,
            vec![
                ProtocolTypeConfig::new("uniswap_v2_pool".to_string(), FinancialType::Swap),
                ProtocolTypeConfig::new("uniswap_v2_pair".to_string(), FinancialType::Swap),
            ],
            "test.spkg".to_string(),
            "map_protocol_changes".to_string(),
            vec![],
            0,
            None,
            None,
            HashMap::new(),
            None,
        )
        .with_protocol_system("uniswap_v2");

        let branch = FamilyBranchSpec::from_extractor_config(&config)
            .expect("branch spec derives from extractor config");

        assert_eq!(branch.protocol_system, "uniswap_v2");
        assert_eq!(
            branch.protocol_type_names,
            HashSet::from(["uniswap_v2_pool".to_string(), "uniswap_v2_pair".to_string(),])
        );
    }

    #[test]
    fn derives_family_branch_specs_from_extractor_configs() {
        let v2 = ExtractorConfig::new(
            "uniswap_v2_primary".to_string(),
            Chain::Ethereum,
            ImplementationType::Vm,
            100,
            42,
            None,
            vec![ProtocolTypeConfig::new("uniswap_v2_pool".to_string(), FinancialType::Swap)],
            "test-v2.spkg".to_string(),
            "map_protocol_changes".to_string(),
            vec![],
            0,
            None,
            None,
            HashMap::new(),
            None,
        )
        .with_protocol_system("uniswap_v2");
        let v3 = ExtractorConfig::new(
            "uniswap_v3_primary".to_string(),
            Chain::Ethereum,
            ImplementationType::Vm,
            100,
            42,
            None,
            vec![ProtocolTypeConfig::new("uniswap_v3_pool".to_string(), FinancialType::Swap)],
            "test-v3.spkg".to_string(),
            "map_protocol_changes".to_string(),
            vec![],
            0,
            None,
            None,
            HashMap::new(),
            None,
        )
        .with_protocol_system("uniswap_v3");

        let branches = FamilyBranchSpec::from_extractor_configs(&[&v2, &v3])
            .expect("branch specs derive from extractor config slice");

        assert_eq!(branches.len(), 2);
        assert_eq!(
            FamilyBranchSpec::protocol_system_set(branches.iter()),
            HashSet::from(["uniswap_v2".to_string(), "uniswap_v3".to_string()])
        );
    }

    #[test]
    fn builds_dispatcher_from_runtime_contract() {
        let contract = ResolvedFamilyRuntimeContract::new(
            crate::extractor::family_runtime_metadata::ResolvedSharedFamilyStream {
                shared_stream_name: "uniswap_family".to_string(),
                spkg: String::new(),
                module: String::new(),
                extractor_id: "ethereum:uniswap_family".to_string(),
                durability_scope: "family::uniswap".to_string(),
            },
            vec![
                branch("uniswap_v2", "uniswap_v2_pool"),
                branch("uniswap_v3", "uniswap_v3_pool"),
            ],
            "uniswap_v2",
        );

        let mut dispatcher = FamilyBlockChangesDispatcher::new_for_runtime_contract(&contract)
            .expect("dispatcher builds from runtime contract");

        let input = substreams::BlockChanges {
            block: Some(test_block()),
            changes: vec![substreams::TransactionChanges {
                tx: Some(test_tx()),
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

        let split = dispatcher
            .dispatch_block_changes(input)
            .expect("runtime-contract dispatcher should route both branches");

        assert_eq!(
            split
                .keys()
                .cloned()
                .collect::<HashSet<_>>(),
            HashSet::from(["uniswap_v2".to_string(), "uniswap_v3".to_string()])
        );
    }

    #[test]
    fn splits_family_block_changes_by_protocol_branch() {
        let tx = test_tx();
        let mut dispatcher = FamilyBlockChangesDispatcher::new([
            branch("uniswap_v2", "uniswap_v2_pool"),
            branch("uniswap_v3", "uniswap_v3_pool"),
        ])
        .expect("dispatcher builds");

        let input = substreams::BlockChanges {
            block: Some(test_block()),
            changes: vec![substreams::TransactionChanges {
                tx: Some(tx),
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
                balance_changes: vec![
                    substreams::BalanceChange {
                        component_id: b"v2-pool".to_vec(),
                        token: vec![0x31; 20],
                        balance: vec![0x01],
                    },
                    substreams::BalanceChange {
                        component_id: b"v3-pool".to_vec(),
                        token: vec![0x32; 20],
                        balance: vec![0x02],
                    },
                ],
                entrypoints: vec![],
                entrypoint_params: vec![],
            }],
            storage_changes: vec![],
        };

        let dispatched = dispatcher
            .dispatch_block_changes(input)
            .expect("dispatch succeeds");

        let v2 = dispatched
            .get("uniswap_v2")
            .expect("v2 output");
        let v3 = dispatched
            .get("uniswap_v3")
            .expect("v3 output");

        assert_eq!(v2.changes.len(), 1);
        assert_eq!(v3.changes.len(), 1);
        assert_eq!(v2.changes[0].component_changes.len(), 1);
        assert_eq!(v3.changes[0].component_changes.len(), 1);
        assert_eq!(v2.changes[0].entity_changes[0].component_id, "v2-pool");
        assert_eq!(v3.changes[0].entity_changes[0].component_id, "v3-pool");
        assert_eq!(v2.changes[0].balance_changes.len(), 1);
        assert_eq!(v3.changes[0].balance_changes.len(), 1);
    }

    #[test]
    fn routes_existing_component_updates_after_registration() {
        let mut dispatcher = FamilyBlockChangesDispatcher::new([
            branch("uniswap_v2", "uniswap_v2_pool"),
            branch("uniswap_v3", "uniswap_v3_pool"),
        ])
        .expect("dispatcher builds");
        dispatcher.register_component_system("existing-v3-pool", "uniswap_v3");

        let dispatched = dispatcher
            .dispatch_block_changes(substreams::BlockChanges {
                block: Some(test_block()),
                changes: vec![substreams::TransactionChanges {
                    tx: Some(test_tx()),
                    contract_changes: vec![],
                    entity_changes: vec![substreams::EntityChanges {
                        component_id: "existing-v3-pool".to_string(),
                        attributes: vec![],
                    }],
                    component_changes: vec![],
                    balance_changes: vec![],
                    entrypoints: vec![],
                    entrypoint_params: vec![],
                }],
                storage_changes: vec![],
            })
            .expect("dispatch succeeds");

        assert!(dispatched.contains_key("uniswap_v3"));
        assert_eq!(dispatched["uniswap_v3"].changes.len(), 1);
        assert_eq!(
            dispatched["uniswap_v3"].changes[0].entity_changes[0].component_id,
            "existing-v3-pool"
        );
    }

    #[test]
    fn routes_existing_component_updates_after_bulk_registration() {
        let mut dispatcher = FamilyBlockChangesDispatcher::new([
            branch("uniswap_v2", "uniswap_v2_pool"),
            branch("uniswap_v3", "uniswap_v3_pool"),
        ])
        .expect("dispatcher builds");
        dispatcher.register_component_systems(HashMap::from([(
            "existing-v2-pool".to_string(),
            "uniswap_v2".to_string(),
        )]));

        let dispatched = dispatcher
            .dispatch_block_changes(substreams::BlockChanges {
                block: Some(test_block()),
                changes: vec![substreams::TransactionChanges {
                    tx: Some(test_tx()),
                    contract_changes: vec![],
                    entity_changes: vec![substreams::EntityChanges {
                        component_id: "existing-v2-pool".to_string(),
                        attributes: vec![],
                    }],
                    component_changes: vec![],
                    balance_changes: vec![],
                    entrypoints: vec![],
                    entrypoint_params: vec![],
                }],
                storage_changes: vec![],
            })
            .expect("dispatch succeeds");

        assert!(dispatched.contains_key("uniswap_v2"));
        assert_eq!(
            dispatched["uniswap_v2"].changes[0].entity_changes[0].component_id,
            "existing-v2-pool"
        );
    }

    #[test]
    fn builds_dispatcher_with_preloaded_seed_state() {
        let dispatcher = FamilyBlockChangesDispatcher::new_with_seed(
            [branch("uniswap_v2", "uniswap_v2_pool"), branch("uniswap_v3", "uniswap_v3_pool")],
            FamilyDispatcherSeed {
                component_systems: HashMap::from([(
                    "seeded-v2-pool".to_string(),
                    "uniswap_v2".to_string(),
                )]),
                contract_systems: HashMap::from([(vec![0x77; 20], "uniswap_v3".to_string())]),
            },
        )
        .expect("dispatcher builds with preloaded seed");

        assert_eq!(dispatcher.component_system("seeded-v2-pool"), Some(&"uniswap_v2".to_string()));
        assert_eq!(dispatcher.contract_system(&vec![0x77; 20]), Some(&"uniswap_v3".to_string()));
    }

    #[test]
    fn routes_dynamically_admitted_component_follow_up_updates() {
        let mut dispatcher = FamilyBlockChangesDispatcher::new([
            branch("uniswap_v2", "uniswap_v2_pool"),
            branch("uniswap_v3", "uniswap_v3_pool"),
        ])
        .expect("dispatcher builds");

        dispatcher
            .dispatch_block_changes(substreams::BlockChanges {
                block: Some(test_block()),
                changes: vec![substreams::TransactionChanges {
                    tx: Some(test_tx()),
                    contract_changes: vec![],
                    entity_changes: vec![],
                    component_changes: vec![substreams::ProtocolComponent {
                        id: "new-v3-pool".to_string(),
                        protocol_type: Some(substreams::ProtocolType {
                            name: "uniswap_v3_pool".to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    balance_changes: vec![],
                    entrypoints: vec![],
                    entrypoint_params: vec![],
                }],
                storage_changes: vec![],
            })
            .expect("creation dispatch succeeds");

        let dispatched = dispatcher
            .dispatch_block_changes(substreams::BlockChanges {
                block: Some(test_block()),
                changes: vec![substreams::TransactionChanges {
                    tx: Some(test_tx()),
                    contract_changes: vec![],
                    entity_changes: vec![substreams::EntityChanges {
                        component_id: "new-v3-pool".to_string(),
                        attributes: vec![],
                    }],
                    component_changes: vec![],
                    balance_changes: vec![],
                    entrypoints: vec![],
                    entrypoint_params: vec![],
                }],
                storage_changes: vec![],
            })
            .expect("follow-up update dispatch succeeds");

        assert!(dispatched.contains_key("uniswap_v3"));
        assert_eq!(
            dispatched["uniswap_v3"].changes[0].entity_changes[0].component_id,
            "new-v3-pool"
        );
    }

    #[test]
    fn routes_contract_only_updates_after_bulk_contract_registration() {
        let mut dispatcher = FamilyBlockChangesDispatcher::new([
            branch("uniswap_v2", "uniswap_v2_pool"),
            branch("uniswap_v3", "uniswap_v3_pool"),
        ])
        .expect("dispatcher builds");
        dispatcher
            .register_contract_systems(HashMap::from([(vec![0x44; 20], "uniswap_v2".to_string())]));

        let dispatched = dispatcher
            .dispatch_block_changes(substreams::BlockChanges {
                block: Some(test_block()),
                changes: vec![substreams::TransactionChanges {
                    tx: Some(test_tx()),
                    contract_changes: vec![test_contract_change(vec![0x44; 20])],
                    entity_changes: vec![],
                    component_changes: vec![],
                    balance_changes: vec![],
                    entrypoints: vec![],
                    entrypoint_params: vec![],
                }],
                storage_changes: vec![],
            })
            .expect("dispatch succeeds");

        assert!(dispatched.contains_key("uniswap_v2"));
        assert_eq!(
            dispatched["uniswap_v2"].changes[0]
                .contract_changes
                .len(),
            1
        );
        assert_eq!(dispatched["uniswap_v2"].changes[0].contract_changes[0].address, vec![0x44; 20]);
    }

    #[test]
    fn routes_storage_only_updates_after_bulk_contract_registration() {
        let mut dispatcher = FamilyBlockChangesDispatcher::new([
            branch("uniswap_v2", "uniswap_v2_pool"),
            branch("uniswap_v3", "uniswap_v3_pool"),
        ])
        .expect("dispatcher builds");
        dispatcher
            .register_contract_systems(HashMap::from([(vec![0x55; 20], "uniswap_v3".to_string())]));

        let dispatched = dispatcher
            .dispatch_block_changes(substreams::BlockChanges {
                block: Some(test_block()),
                changes: vec![],
                storage_changes: vec![substreams::TransactionStorageChanges {
                    tx: Some(test_tx()),
                    storage_changes: vec![test_storage_change(vec![0x55; 20])],
                }],
            })
            .expect("dispatch succeeds");

        assert!(dispatched.contains_key("uniswap_v3"));
        assert_eq!(
            dispatched["uniswap_v3"]
                .storage_changes
                .len(),
            1
        );
        assert_eq!(
            dispatched["uniswap_v3"].storage_changes[0].storage_changes[0].address,
            vec![0x55; 20]
        );
    }

    #[test]
    fn routes_dynamically_admitted_component_contract_and_storage_follow_ups() {
        let mut dispatcher = FamilyBlockChangesDispatcher::new([
            branch("uniswap_v2", "uniswap_v2_pool"),
            branch("uniswap_v3", "uniswap_v3_pool"),
        ])
        .expect("dispatcher builds");

        dispatcher
            .dispatch_block_changes(substreams::BlockChanges {
                block: Some(test_block()),
                changes: vec![substreams::TransactionChanges {
                    tx: Some(test_tx()),
                    contract_changes: vec![],
                    entity_changes: vec![],
                    component_changes: vec![substreams::ProtocolComponent {
                        id: "new-v2-pool".to_string(),
                        protocol_type: Some(substreams::ProtocolType {
                            name: "uniswap_v2_pool".to_string(),
                            ..Default::default()
                        }),
                        contracts: vec![vec![0x66; 20]],
                        ..Default::default()
                    }],
                    balance_changes: vec![],
                    entrypoints: vec![],
                    entrypoint_params: vec![],
                }],
                storage_changes: vec![],
            })
            .expect("creation dispatch succeeds");

        let contract_dispatched = dispatcher
            .dispatch_block_changes(substreams::BlockChanges {
                block: Some(test_block()),
                changes: vec![substreams::TransactionChanges {
                    tx: Some(test_tx()),
                    contract_changes: vec![test_contract_change(vec![0x66; 20])],
                    entity_changes: vec![],
                    component_changes: vec![],
                    balance_changes: vec![],
                    entrypoints: vec![],
                    entrypoint_params: vec![],
                }],
                storage_changes: vec![],
            })
            .expect("contract-only follow-up dispatch succeeds");
        assert!(contract_dispatched.contains_key("uniswap_v2"));

        let storage_dispatched = dispatcher
            .dispatch_block_changes(substreams::BlockChanges {
                block: Some(test_block()),
                changes: vec![],
                storage_changes: vec![substreams::TransactionStorageChanges {
                    tx: Some(test_tx()),
                    storage_changes: vec![test_storage_change(vec![0x66; 20])],
                }],
            })
            .expect("storage-only follow-up dispatch succeeds");
        assert!(storage_dispatched.contains_key("uniswap_v2"));
    }

    #[test]
    fn routes_same_block_dynamic_admission_and_follow_up_updates() {
        let mut dispatcher = FamilyBlockChangesDispatcher::new([
            branch("uniswap_v2", "uniswap_v2_pool"),
            branch("uniswap_v3", "uniswap_v3_pool"),
        ])
        .expect("dispatcher builds");

        let tx = test_tx();
        let dispatched = dispatcher
            .dispatch_block_changes(substreams::BlockChanges {
                block: Some(test_block()),
                changes: vec![substreams::TransactionChanges {
                    tx: Some(tx.clone()),
                    contract_changes: vec![test_contract_change(vec![0x67; 20])],
                    entity_changes: vec![substreams::EntityChanges {
                        component_id: "new-v2-pool".to_string(),
                        attributes: vec![],
                    }],
                    component_changes: vec![substreams::ProtocolComponent {
                        id: "new-v2-pool".to_string(),
                        protocol_type: Some(substreams::ProtocolType {
                            name: "uniswap_v2_pool".to_string(),
                            ..Default::default()
                        }),
                        contracts: vec![vec![0x67; 20]],
                        ..Default::default()
                    }],
                    balance_changes: vec![substreams::BalanceChange {
                        component_id: b"new-v2-pool".to_vec(),
                        token: vec![0xa0; 20],
                        balance: vec![0x01],
                    }],
                    entrypoints: vec![],
                    entrypoint_params: vec![],
                }],
                storage_changes: vec![substreams::TransactionStorageChanges {
                    tx: Some(tx),
                    storage_changes: vec![test_storage_change(vec![0x67; 20])],
                }],
            })
            .expect("same-block dynamic admission dispatch succeeds");

        assert_eq!(dispatched.len(), 2);
        assert_eq!(dispatched["uniswap_v2"].changes.len(), 1);
        assert_eq!(
            dispatched["uniswap_v2"]
                .storage_changes
                .len(),
            1
        );
        assert_eq!(dispatched["uniswap_v2"].changes[0].component_changes[0].id, "new-v2-pool");
        assert_eq!(
            dispatched["uniswap_v2"].changes[0].entity_changes[0].component_id,
            "new-v2-pool"
        );
        assert_eq!(
            String::from_utf8(
                dispatched["uniswap_v2"].changes[0].balance_changes[0]
                    .component_id
                    .clone()
            )
            .expect("balance change component id should be utf8"),
            "new-v2-pool"
        );
        assert_eq!(
            dispatched["uniswap_v2"].storage_changes[0].storage_changes[0].address,
            vec![0x67; 20]
        );
        assert!(
            dispatched["uniswap_v3"]
                .changes
                .is_empty(),
            "untouched sibling branch should still receive an empty block"
        );
    }

    #[test]
    fn dispatches_block_scoped_data_into_branch_payloads() {
        let mut dispatcher = FamilyBlockChangesDispatcher::new([
            branch("uniswap_v2", "uniswap_v2_pool"),
            branch("uniswap_v3", "uniswap_v3_pool"),
        ])
        .expect("dispatcher builds");

        let raw_changes = substreams::BlockChanges {
            block: Some(test_block()),
            changes: vec![substreams::TransactionChanges {
                tx: Some(test_tx()),
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

        let dispatched = dispatcher
            .dispatch_block_scoped_data(BlockScopedData {
                output: Some(MapModuleOutput {
                    name: "map_uniswap_family_protocol_changes".to_string(),
                    map_output: Some(prost_types::Any {
                        type_url: "type.googleapis.com/tycho.evm.v1.BlockChanges".to_string(),
                        value: raw_changes.encode_to_vec(),
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
            })
            .expect("dispatch succeeds");

        assert_eq!(dispatched.len(), 2);
        assert_eq!(dispatched["uniswap_v2"].cursor, "cursor-42");
        assert_eq!(dispatched["uniswap_v3"].final_block_height, 42);
        let v2_bytes = &dispatched["uniswap_v2"]
            .output
            .as_ref()
            .expect("output")
            .map_output
            .as_ref()
            .expect("map output")
            .value;
        let v3_bytes = &dispatched["uniswap_v3"]
            .output
            .as_ref()
            .expect("output")
            .map_output
            .as_ref()
            .expect("map output")
            .value;
        let v2_msg = substreams::BlockChanges::decode(v2_bytes.as_slice()).expect("decode v2");
        let v3_msg = substreams::BlockChanges::decode(v3_bytes.as_slice()).expect("decode v3");
        assert_eq!(
            v2_msg.changes[0]
                .component_changes
                .len(),
            1
        );
        assert_eq!(
            v3_msg.changes[0]
                .component_changes
                .len(),
            1
        );
        assert_eq!(v2_msg.changes[0].component_changes[0].id, "v2-pool");
        assert_eq!(v3_msg.changes[0].component_changes[0].id, "v3-pool");
    }

    #[test]
    fn dispatches_empty_branch_block_for_untouched_family_member() {
        let mut dispatcher = FamilyBlockChangesDispatcher::new([
            branch("uniswap_v2", "uniswap_v2_pool"),
            branch("uniswap_v3", "uniswap_v3_pool"),
        ])
        .expect("dispatcher builds");

        let dispatched = dispatcher
            .dispatch_block_changes(substreams::BlockChanges {
                block: Some(test_block()),
                changes: vec![substreams::TransactionChanges {
                    tx: Some(test_tx()),
                    contract_changes: vec![],
                    entity_changes: vec![],
                    component_changes: vec![substreams::ProtocolComponent {
                        id: "v2-pool".to_string(),
                        protocol_type: Some(substreams::ProtocolType {
                            name: "uniswap_v2_pool".to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    balance_changes: vec![],
                    entrypoints: vec![],
                    entrypoint_params: vec![],
                }],
                storage_changes: vec![],
            })
            .expect("dispatch succeeds");

        assert_eq!(dispatched.len(), 2);
        assert_eq!(dispatched["uniswap_v2"].changes.len(), 1);
        assert!(
            dispatched["uniswap_v3"]
                .changes
                .is_empty(),
            "untouched branch should still receive an empty progress block"
        );
        assert_eq!(
            dispatched["uniswap_v3"]
                .block
                .as_ref()
                .expect("branch block")
                .number,
            42
        );
    }

    #[test]
    fn routes_same_block_updates_when_component_registration_arrives_in_later_transaction() {
        let mut dispatcher = FamilyBlockChangesDispatcher::new([
            branch("uniswap_v2", "uniswap_v2_pool"),
            branch("uniswap_v3", "uniswap_v3_pool"),
        ])
        .expect("dispatcher builds");

        let first_tx = substreams::Transaction {
            hash: vec![0xaa; 32],
            from: vec![0x11; 20],
            to: vec![0x22; 20],
            index: 0,
        };
        let second_tx = substreams::Transaction {
            hash: vec![0xbb; 32],
            from: vec![0x11; 20],
            to: vec![0x22; 20],
            index: 1,
        };

        let dispatched = dispatcher
            .dispatch_block_changes(substreams::BlockChanges {
                block: Some(test_block()),
                changes: vec![
                    substreams::TransactionChanges {
                        tx: Some(first_tx),
                        contract_changes: vec![],
                        entity_changes: vec![substreams::EntityChanges {
                            component_id: "late-v3-pool".to_string(),
                            attributes: vec![],
                        }],
                        component_changes: vec![],
                        balance_changes: vec![substreams::BalanceChange {
                            component_id: b"late-v3-pool".to_vec(),
                            token: vec![0xa0; 20],
                            balance: vec![0x01],
                        }],
                        entrypoints: vec![],
                        entrypoint_params: vec![],
                    },
                    substreams::TransactionChanges {
                        tx: Some(second_tx),
                        contract_changes: vec![],
                        entity_changes: vec![],
                        component_changes: vec![substreams::ProtocolComponent {
                            id: "late-v3-pool".to_string(),
                            protocol_type: Some(substreams::ProtocolType {
                                name: "uniswap_v3_pool".to_string(),
                                ..Default::default()
                            }),
                            contracts: vec![vec![0x83; 20]],
                            ..Default::default()
                        }],
                        balance_changes: vec![],
                        entrypoints: vec![],
                        entrypoint_params: vec![],
                    },
                ],
                storage_changes: vec![],
            })
            .expect("same-block out-of-order admission dispatch succeeds");

        assert_eq!(dispatched["uniswap_v3"].changes.len(), 2);
        assert_eq!(
            dispatched["uniswap_v3"].changes[0].entity_changes[0].component_id,
            "late-v3-pool"
        );
        assert_eq!(
            String::from_utf8(
                dispatched["uniswap_v3"].changes[0].balance_changes[0]
                    .component_id
                    .clone()
            )
            .expect("balance change component id should be utf8"),
            "late-v3-pool"
        );
        assert_eq!(dispatched["uniswap_v3"].changes[1].component_changes[0].id, "late-v3-pool");
    }

    #[tokio::test]
    async fn derives_dispatcher_seed_from_protocol_cache() {
        let cache = ProtocolMemoryCache::new(
            Chain::Ethereum,
            Duration::seconds(60),
            std::sync::Arc::new(MockGateway::new()),
        );
        cache
            .add_components(vec![
                tycho_common::models::protocol::ProtocolComponent::new(
                    "seeded-v2-pool",
                    "uniswap_v2",
                    "uniswap_v2_pool",
                    Chain::Ethereum,
                    Vec::new(),
                    vec![tycho_common::Bytes::from(vec![0x81; 20])],
                    HashMap::new(),
                    tycho_common::models::ChangeType::Creation,
                    tycho_common::Bytes::default(),
                    chrono::NaiveDateTime::default(),
                ),
                tycho_common::models::protocol::ProtocolComponent::new(
                    "seeded-v3-pool",
                    "uniswap_v3",
                    "uniswap_v3_pool",
                    Chain::Ethereum,
                    Vec::new(),
                    vec![tycho_common::Bytes::from(vec![0x82; 20])],
                    HashMap::new(),
                    tycho_common::models::ChangeType::Creation,
                    tycho_common::Bytes::default(),
                    chrono::NaiveDateTime::default(),
                ),
            ])
            .await
            .expect("add cached components");

        let branches =
            vec![branch("uniswap_v2", "uniswap_v2_pool"), branch("uniswap_v3", "uniswap_v3_pool")];
        let seed = FamilyBranchSpec::dispatcher_seed_from_protocol_cache(&branches, &cache)
            .await
            .expect("dispatcher seed derives from cache");

        assert_eq!(
            seed.component_systems
                .get("seeded-v2-pool"),
            Some(&"uniswap_v2".to_string())
        );
        assert_eq!(
            seed.component_systems
                .get("seeded-v3-pool"),
            Some(&"uniswap_v3".to_string())
        );
        assert_eq!(
            seed.contract_systems
                .get(&vec![0x81; 20]),
            Some(&"uniswap_v2".to_string())
        );
        assert_eq!(
            seed.contract_systems
                .get(&vec![0x82; 20]),
            Some(&"uniswap_v3".to_string())
        );
    }

    #[tokio::test]
    async fn hydrates_dispatcher_from_protocol_cache_by_component_ids() {
        let cache = ProtocolMemoryCache::new(
            Chain::Ethereum,
            Duration::seconds(60),
            std::sync::Arc::new(MockGateway::new()),
        );
        cache
            .add_components(vec![
                tycho_common::models::protocol::ProtocolComponent::new(
                    "seeded-v2-pool",
                    "uniswap_v2",
                    "uniswap_v2_pool",
                    Chain::Ethereum,
                    Vec::new(),
                    vec![tycho_common::Bytes::from(vec![0x81; 20])],
                    HashMap::new(),
                    tycho_common::models::ChangeType::Creation,
                    tycho_common::Bytes::default(),
                    chrono::NaiveDateTime::default(),
                ),
                tycho_common::models::protocol::ProtocolComponent::new(
                    "other-pool",
                    "curve",
                    "curve_pool",
                    Chain::Ethereum,
                    Vec::new(),
                    vec![tycho_common::Bytes::from(vec![0x91; 20])],
                    HashMap::new(),
                    tycho_common::models::ChangeType::Creation,
                    tycho_common::Bytes::default(),
                    chrono::NaiveDateTime::default(),
                ),
            ])
            .await
            .expect("add cached components");

        let branches = vec![branch("uniswap_v2", "uniswap_v2_pool")];
        let mut dispatcher =
            FamilyBlockChangesDispatcher::new(branches).expect("dispatcher builds");

        let hydrated = dispatcher
            .hydrate_from_protocol_cache_by_component_ids(
                &cache,
                &HashSet::from(["seeded-v2-pool".to_string(), "other-pool".to_string()]),
            )
            .await
            .expect("dispatcher hydrates from cache");

        assert!(hydrated);
        assert_eq!(dispatcher.component_system("seeded-v2-pool"), Some(&"uniswap_v2".to_string()));
        assert_eq!(dispatcher.contract_system(&vec![0x81; 20]), Some(&"uniswap_v2".to_string()));
        assert_eq!(dispatcher.component_system("other-pool"), None);
        assert_eq!(dispatcher.contract_system(&vec![0x91; 20]), None);
    }

    #[tokio::test]
    async fn dispatch_block_scoped_data_with_protocol_cache_fallback_hydrates_unknown_component() {
        let cache = ProtocolMemoryCache::new(
            Chain::Ethereum,
            Duration::seconds(60),
            std::sync::Arc::new(MockGateway::new()),
        );
        cache
            .add_components(vec![tycho_common::models::protocol::ProtocolComponent::new(
                "seeded-v2-pool",
                "uniswap_v2",
                "uniswap_v2_pool",
                Chain::Ethereum,
                Vec::new(),
                vec![tycho_common::Bytes::from(vec![0x81; 20])],
                HashMap::new(),
                tycho_common::models::ChangeType::Creation,
                tycho_common::Bytes::default(),
                chrono::NaiveDateTime::default(),
            )])
            .await
            .expect("add cached component");

        let branches = vec![branch("uniswap_v2", "uniswap_v2_pool")];
        let mut dispatcher =
            FamilyBlockChangesDispatcher::new(branches).expect("dispatcher builds");

        let dispatched = dispatcher
            .dispatch_block_scoped_data_with_protocol_cache_fallback(
                family_block_scoped_data_with_component_balance(
                    "seeded-v2-pool",
                    vec![0x81; 20],
                    43,
                    "cursor-43",
                ),
                &cache,
            )
            .await
            .expect("dispatcher should hydrate from cache and redispatch");

        let output = dispatched["uniswap_v2"]
            .output
            .as_ref()
            .expect("branch output");
        let map_output = output
            .map_output
            .as_ref()
            .expect("branch map output");
        let block_changes = substreams::BlockChanges::decode(map_output.value.as_slice())
            .expect("decode dispatched block changes");

        assert_eq!(block_changes.changes.len(), 1);
        assert_eq!(
            String::from_utf8(
                block_changes.changes[0].balance_changes[0]
                    .component_id
                    .clone()
            )
            .expect("component id should be utf8"),
            "seeded-v2-pool"
        );
    }

    #[tokio::test]
    async fn dispatch_block_scoped_data_with_protocol_cache_fallback_hydrates_contract_only_follow_up(
    ) {
        let cache = ProtocolMemoryCache::new(
            Chain::Ethereum,
            Duration::seconds(60),
            std::sync::Arc::new(MockGateway::new()),
        );
        cache
            .add_components(vec![tycho_common::models::protocol::ProtocolComponent::new(
                "seeded-v2-pool",
                "uniswap_v2",
                "uniswap_v2_pool",
                Chain::Ethereum,
                Vec::new(),
                vec![tycho_common::Bytes::from(vec![0x81; 20])],
                HashMap::new(),
                tycho_common::models::ChangeType::Creation,
                tycho_common::Bytes::default(),
                chrono::NaiveDateTime::default(),
            )])
            .await
            .expect("add cached component");

        let branches = vec![branch("uniswap_v2", "uniswap_v2_pool")];
        let mut dispatcher =
            FamilyBlockChangesDispatcher::new(branches).expect("dispatcher builds");

        let block_changes = substreams::BlockChanges {
            block: Some(test_block()),
            changes: vec![substreams::TransactionChanges {
                tx: Some(test_tx()),
                contract_changes: vec![test_contract_change(vec![0x81; 20])],
                entity_changes: vec![],
                component_changes: vec![],
                balance_changes: vec![],
                entrypoints: vec![],
                entrypoint_params: vec![],
            }],
            storage_changes: vec![substreams::TransactionStorageChanges {
                tx: Some(test_tx()),
                storage_changes: vec![test_storage_change(vec![0x81; 20])],
            }],
        };

        let dispatched = dispatcher
            .dispatch_block_scoped_data_with_protocol_cache_fallback(
                BlockScopedData {
                    output: Some(MapModuleOutput {
                        name: "map_family_protocol_changes".to_string(),
                        map_output: Some(prost_types::Any {
                            type_url: "type.googleapis.com/tycho.evm.v1.BlockChanges".to_string(),
                            value: block_changes.encode_to_vec(),
                        }),
                        debug_info: None,
                    }),
                    clock: Some(Clock { id: "43".to_string(), number: 43, timestamp: None }),
                    cursor: "cursor-43".to_string(),
                    final_block_height: 43,
                    debug_map_outputs: vec![],
                    debug_store_outputs: vec![],
                    attestation: "test_attestation".to_string(),
                    is_partial: false,
                    partial_index: None,
                    is_last_partial: None,
                },
                &cache,
            )
            .await
            .expect("dispatcher should hydrate contract ownership from cache and redispatch");

        let output = dispatched["uniswap_v2"]
            .output
            .as_ref()
            .expect("branch output");
        let map_output = output
            .map_output
            .as_ref()
            .expect("branch map output");
        let block_changes = substreams::BlockChanges::decode(map_output.value.as_slice())
            .expect("decode dispatched block changes");

        assert_eq!(block_changes.changes.len(), 1);
        assert_eq!(
            block_changes.changes[0]
                .contract_changes
                .len(),
            1
        );
        assert_eq!(block_changes.changes[0].contract_changes[0].address, vec![0x81; 20]);
        assert_eq!(block_changes.storage_changes.len(), 1);
        assert_eq!(block_changes.storage_changes[0].storage_changes[0].address, vec![0x81; 20]);
    }
}
