use std::collections::{HashMap, HashSet};

use prost::Message;
use tycho_substreams::pb::tycho::evm::v1 as substreams;

use crate::{
    extractor::{protocol_cache::ProtocolMemoryCache, runner::ExtractorConfig, ExtractionError},
    pb::sf::substreams::rpc::v2::{BlockScopedData, MapModuleOutput},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FamilyBranchSpec {
    pub protocol_system: String,
    pub protocol_type_names: HashSet<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FamilyDispatcherSeed {
    pub component_systems: HashMap<String, String>,
    pub contract_systems: HashMap<Vec<u8>, String>,
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
        branches
            .into_iter()
            .map(|branch| branch.protocol_system.clone())
            .collect()
    }

    pub async fn dispatcher_seed_from_protocol_cache(
        branches: &[FamilyBranchSpec],
        protocol_cache: &ProtocolMemoryCache,
    ) -> FamilyDispatcherSeed {
        let protocol_systems = Self::protocol_system_set(branches.iter());
        let component_systems = protocol_cache
            .component_protocol_systems(&protocol_systems)
            .await;
        let contract_systems = protocol_cache
            .contract_protocol_systems(&protocol_systems)
            .await
            .into_iter()
            .map(|(contract, protocol_system)| (contract.to_vec(), protocol_system))
            .collect();

        FamilyDispatcherSeed {
            component_systems,
            contract_systems,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct FamilyBlockChangesDispatcher {
    branch_protocol_systems: HashSet<String>,
    protocol_type_to_system: HashMap<String, String>,
    component_to_system: HashMap<String, String>,
    contract_to_system: HashMap<Vec<u8>, String>,
}

impl FamilyBlockChangesDispatcher {
    pub fn new(
        branches: impl IntoIterator<Item = FamilyBranchSpec>,
    ) -> Result<Self, ExtractionError> {
        let mut branch_protocol_systems = HashSet::new();
        let mut protocol_type_to_system = HashMap::new();

        for branch in branches {
            branch_protocol_systems.insert(branch.protocol_system.clone());
            for protocol_type_name in branch.protocol_type_names {
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

        Ok(Self {
            branch_protocol_systems,
            protocol_type_to_system,
            component_to_system: HashMap::new(),
            contract_to_system: HashMap::new(),
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

    pub fn register_component_system(
        &mut self,
        component_id: impl Into<String>,
        protocol_system: impl Into<String>,
    ) {
        self.component_to_system
            .insert(component_id.into(), protocol_system.into());
    }

    pub fn register_component_systems(
        &mut self,
        component_systems: impl IntoIterator<Item = (String, String)>,
    ) {
        self.component_to_system
            .extend(component_systems);
    }

    pub fn register_contract_system(
        &mut self,
        contract_address: impl Into<Vec<u8>>,
        protocol_system: impl Into<String>,
    ) {
        self.contract_to_system
            .insert(contract_address.into(), protocol_system.into());
    }

    pub fn register_contract_systems(
        &mut self,
        contract_systems: impl IntoIterator<Item = (Vec<u8>, String)>,
    ) {
        self.contract_to_system
            .extend(contract_systems);
    }

    pub fn dispatch_block_changes(
        &mut self,
        msg: substreams::BlockChanges,
    ) -> Result<HashMap<String, substreams::BlockChanges>, ExtractionError> {
        let block = msg.block.clone();
        let mut txs_by_system: HashMap<String, Vec<substreams::TransactionChanges>> =
            HashMap::new();
        let mut tx_systems_by_hash: HashMap<Vec<u8>, HashSet<String>> = HashMap::new();

        for tx_changes in msg.changes {
            let tx = tx_changes.tx.clone().ok_or_else(|| {
                ExtractionError::DecodeError("TransactionChanges misses a transaction".to_string())
            })?;
            let tx_hash = tx.hash.clone();
            let (split_txs, touched_systems) = self.dispatch_transaction_changes(tx_changes)?;

            for (protocol_system, split_tx) in split_txs {
                txs_by_system
                    .entry(protocol_system.clone())
                    .or_default()
                    .push(split_tx);
                tx_systems_by_hash
                    .entry(tx_hash.clone())
                    .or_default()
                    .insert(protocol_system);
            }

            if touched_systems.is_empty() {
                tx_systems_by_hash
                    .entry(tx_hash)
                    .or_default();
            }
        }

        let mut storage_by_system: HashMap<String, Vec<substreams::TransactionStorageChanges>> =
            HashMap::new();
        for storage_changes in msg.storage_changes {
            let tx = storage_changes
                .tx
                .clone()
                .ok_or_else(|| {
                    ExtractionError::DecodeError(
                        "TransactionStorageChanges misses a transaction".to_string(),
                    )
                })?;
            let systems = tx_systems_by_hash
                .get(&tx.hash)
                .cloned()
                .unwrap_or_default();

            match systems.len() {
                0 => {
                    let inferred_systems = self.resolve_storage_systems(&storage_changes)?;
                    match inferred_systems.len() {
                        0 => {
                            return Err(ExtractionError::DecodeError(format!(
                                "unable to route storage changes for tx 0x{}: no protocol branch matched",
                                hex::encode(tx.hash)
                            )));
                        }
                        1 => {
                            let protocol_system = inferred_systems
                                .into_iter()
                                .next()
                                .expect("one system");
                            storage_by_system
                                .entry(protocol_system)
                                .or_default()
                                .push(storage_changes);
                        }
                        _ => {
                            return Err(ExtractionError::DecodeError(format!(
                                "unable to route storage changes for tx 0x{}: multiple protocol branches matched",
                                hex::encode(tx.hash)
                            )));
                        }
                    }
                }
                1 => {
                    let protocol_system = systems
                        .into_iter()
                        .next()
                        .expect("one system");
                    storage_by_system
                        .entry(protocol_system)
                        .or_default()
                        .push(storage_changes);
                }
                _ => {
                    return Err(ExtractionError::DecodeError(format!(
                        "unable to route storage changes for tx 0x{}: multiple protocol branches matched",
                        hex::encode(tx.hash)
                    )));
                }
            }
        }

        let mut dispatched = HashMap::new();
        let mut all_systems = self.branch_protocol_systems.clone();
        all_systems.extend(
            txs_by_system
                .keys()
                .cloned(),
        );
        all_systems.extend(storage_by_system.keys().cloned());

        for protocol_system in all_systems {
            dispatched.insert(
                protocol_system.clone(),
                substreams::BlockChanges {
                    block: block.clone(),
                    changes: txs_by_system
                        .remove(&protocol_system)
                        .unwrap_or_default(),
                    storage_changes: storage_by_system
                        .remove(&protocol_system)
                        .unwrap_or_default(),
                },
            );
        }

        Ok(dispatched)
    }

    pub fn dispatch_block_scoped_data(
        &mut self,
        block_scoped_data: BlockScopedData,
    ) -> Result<HashMap<String, BlockScopedData>, ExtractionError> {
        let output = block_scoped_data
            .output
            .clone()
            .ok_or_else(|| {
                ExtractionError::DecodeError("Missing output in block scoped data".to_string())
            })?;
        let map_output = output
            .map_output
            .clone()
            .ok_or_else(|| {
                ExtractionError::DecodeError(
                    "Missing map_output in block scoped data's output".to_string(),
                )
            })?;

        if !map_output
            .type_url
            .ends_with("BlockChanges")
        {
            return Err(ExtractionError::DecodeError(format!(
                "family dispatcher only supports BlockChanges outputs, got {}",
                map_output.type_url
            )));
        }

        let raw_msg = substreams::BlockChanges::decode(map_output.value.as_slice())?;
        let dispatched = self.dispatch_block_changes(raw_msg)?;

        Ok(dispatched
            .into_iter()
            .map(|(protocol_system, branch_changes)| {
                let mut branch_bsd = block_scoped_data.clone();
                branch_bsd.output = Some(MapModuleOutput {
                    name: output.name.clone(),
                    map_output: Some(prost_types::Any {
                        type_url: map_output.type_url.clone(),
                        value: branch_changes.encode_to_vec(),
                    }),
                    debug_info: output.debug_info.clone(),
                });
                (protocol_system, branch_bsd)
            })
            .collect())
    }

    fn dispatch_transaction_changes(
        &mut self,
        tx_changes: substreams::TransactionChanges,
    ) -> Result<(HashMap<String, substreams::TransactionChanges>, HashSet<String>), ExtractionError>
    {
        let tx = tx_changes.tx.clone().ok_or_else(|| {
            ExtractionError::DecodeError("TransactionChanges misses a transaction".to_string())
        })?;
        let mut split_txs: HashMap<String, substreams::TransactionChanges> = HashMap::new();
        let mut touched_systems = HashSet::new();

        for component_change in tx_changes.component_changes {
            let protocol_type_name = component_change
                .protocol_type
                .as_ref()
                .map(|protocol_type| protocol_type.name.clone())
                .ok_or_else(|| {
                    ExtractionError::DecodeError(format!(
                        "component `{}` is missing protocol_type",
                        component_change.id
                    ))
                })?;
            let protocol_system = self
                .protocol_type_to_system
                .get(&protocol_type_name)
                .cloned()
                .ok_or_else(|| {
                    ExtractionError::DecodeError(format!(
                        "unknown protocol type `{protocol_type_name}` while routing component `{}`",
                        component_change.id
                    ))
                })?;

            self.component_to_system
                .insert(component_change.id.clone(), protocol_system.clone());
            for contract in &component_change.contracts {
                self.contract_to_system
                    .insert(contract.clone(), protocol_system.clone());
            }
            touched_systems.insert(protocol_system.clone());
            split_txs
                .entry(protocol_system)
                .or_insert_with(|| empty_transaction_changes(&tx))
                .component_changes
                .push(component_change);
        }

        for entity_change in tx_changes.entity_changes {
            let protocol_system = self.resolve_component_system(&entity_change.component_id)?;
            touched_systems.insert(protocol_system.clone());
            split_txs
                .entry(protocol_system)
                .or_insert_with(|| empty_transaction_changes(&tx))
                .entity_changes
                .push(entity_change);
        }

        for balance_change in tx_changes.balance_changes {
            let component_id =
                String::from_utf8(balance_change.component_id.clone()).map_err(|err| {
                    ExtractionError::DecodeError(format!(
                        "balance change component id is not utf8: {err}"
                    ))
                })?;
            let protocol_system = self.resolve_component_system(&component_id)?;
            touched_systems.insert(protocol_system.clone());
            split_txs
                .entry(protocol_system)
                .or_insert_with(|| empty_transaction_changes(&tx))
                .balance_changes
                .push(balance_change);
        }

        for entrypoint in tx_changes.entrypoints {
            let protocol_system = self.resolve_component_system(&entrypoint.component_id)?;
            touched_systems.insert(protocol_system.clone());
            split_txs
                .entry(protocol_system)
                .or_insert_with(|| empty_transaction_changes(&tx))
                .entrypoints
                .push(entrypoint);
        }

        for entrypoint_params in tx_changes.entrypoint_params {
            let component_id = entrypoint_params
                .component_id
                .clone()
                .ok_or_else(|| {
                    ExtractionError::DecodeError(
                        "Entrypoint params should have a component id".to_owned(),
                    )
                })?;
            let protocol_system = self.resolve_component_system(&component_id)?;
            touched_systems.insert(protocol_system.clone());
            split_txs
                .entry(protocol_system)
                .or_insert_with(|| empty_transaction_changes(&tx))
                .entrypoint_params
                .push(entrypoint_params);
        }

        if !tx_changes.contract_changes.is_empty() {
            let contract_systems = if touched_systems.is_empty() {
                tx_changes
                    .contract_changes
                    .iter()
                    .filter_map(|change| self.contract_to_system.get(&change.address).cloned())
                    .collect::<HashSet<_>>()
            } else {
                touched_systems.clone()
            };

            match contract_systems.len() {
                0 => {
                    return Err(ExtractionError::DecodeError(format!(
                        "unable to route contract changes for tx 0x{}: no protocol branch matched",
                        hex::encode(tx.hash.clone())
                    )));
                }
                1 => {
                    let protocol_system = contract_systems
                        .iter()
                        .next()
                        .cloned()
                        .expect("one system");
                    split_txs
                        .entry(protocol_system)
                        .or_insert_with(|| empty_transaction_changes(&tx))
                        .contract_changes
                        .extend(tx_changes.contract_changes);
                }
                _ => {
                    return Err(ExtractionError::DecodeError(format!(
                        "unable to route contract changes for tx 0x{}: multiple protocol branches matched",
                        hex::encode(tx.hash.clone())
                    )));
                }
            }
        }

        Ok((split_txs, touched_systems))
    }

    fn resolve_component_system(&self, component_id: &str) -> Result<String, ExtractionError> {
        self.component_to_system
            .get(component_id)
            .cloned()
            .ok_or_else(|| {
                ExtractionError::DecodeError(format!(
                    "unknown component `{component_id}` while routing family block changes"
                ))
            })
    }

    fn resolve_storage_systems(
        &self,
        storage_changes: &substreams::TransactionStorageChanges,
    ) -> Result<HashSet<String>, ExtractionError> {
        Ok(storage_changes
            .storage_changes
            .iter()
            .filter_map(|change| self.contract_to_system.get(&change.address).cloned())
            .collect())
    }
}

fn empty_transaction_changes(tx: &substreams::Transaction) -> substreams::TransactionChanges {
    substreams::TransactionChanges {
        tx: Some(tx.clone()),
        contract_changes: vec![],
        entity_changes: vec![],
        component_changes: vec![],
        balance_changes: vec![],
        entrypoints: vec![],
        entrypoint_params: vec![],
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use tycho_common::models::{Chain, FinancialType, ImplementationType};
    use chrono::Duration;
    use prost::Message;
    use tycho_substreams::pb::tycho::evm::v1 as substreams;

    use crate::extractor::{
        protocol_cache::{ProtocolDataCache, ProtocolMemoryCache},
        runner::{ExtractorConfig, ProtocolTypeConfig},
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
            HashSet::from([
                "uniswap_v2_pool".to_string(),
                "uniswap_v2_pair".to_string(),
            ])
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
            [
                branch("uniswap_v2", "uniswap_v2_pool"),
                branch("uniswap_v3", "uniswap_v3_pool"),
            ],
            FamilyDispatcherSeed {
                component_systems: HashMap::from([(
                    "seeded-v2-pool".to_string(),
                    "uniswap_v2".to_string(),
                )]),
                contract_systems: HashMap::from([(
                    vec![0x77; 20],
                    "uniswap_v3".to_string(),
                )]),
            },
        )
        .expect("dispatcher builds with preloaded seed");

        assert_eq!(
            dispatcher.component_to_system.get("seeded-v2-pool"),
            Some(&"uniswap_v2".to_string())
        );
        assert_eq!(
            dispatcher.contract_to_system.get(&vec![0x77; 20]),
            Some(&"uniswap_v3".to_string())
        );
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
        dispatcher.register_contract_systems(HashMap::from([(
            vec![0x44; 20],
            "uniswap_v2".to_string(),
        )]));

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
        assert_eq!(dispatched["uniswap_v2"].changes[0].contract_changes.len(), 1);
        assert_eq!(
            dispatched["uniswap_v2"].changes[0].contract_changes[0].address,
            vec![0x44; 20]
        );
    }

    #[test]
    fn routes_storage_only_updates_after_bulk_contract_registration() {
        let mut dispatcher = FamilyBlockChangesDispatcher::new([
            branch("uniswap_v2", "uniswap_v2_pool"),
            branch("uniswap_v3", "uniswap_v3_pool"),
        ])
        .expect("dispatcher builds");
        dispatcher.register_contract_systems(HashMap::from([(
            vec![0x55; 20],
            "uniswap_v3".to_string(),
        )]));

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
        assert_eq!(dispatched["uniswap_v3"].storage_changes.len(), 1);
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
            dispatched["uniswap_v3"].changes.is_empty(),
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

    #[tokio::test]
    async fn derives_dispatcher_seed_from_protocol_cache() {
        let cache = ProtocolMemoryCache::new(
            Chain::Ethereum,
            Duration::seconds(60),
            std::sync::Arc::new(MockGateway::new()),
        );
        cache.add_components(vec![
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

        let branches = vec![
            branch("uniswap_v2", "uniswap_v2_pool"),
            branch("uniswap_v3", "uniswap_v3_pool"),
        ];
        let seed = FamilyBranchSpec::dispatcher_seed_from_protocol_cache(&branches, &cache).await;

        assert_eq!(
            seed.component_systems.get("seeded-v2-pool"),
            Some(&"uniswap_v2".to_string())
        );
        assert_eq!(
            seed.component_systems.get("seeded-v3-pool"),
            Some(&"uniswap_v3".to_string())
        );
        assert_eq!(
            seed.contract_systems.get(&vec![0x81; 20]),
            Some(&"uniswap_v2".to_string())
        );
        assert_eq!(
            seed.contract_systems.get(&vec![0x82; 20]),
            Some(&"uniswap_v3".to_string())
        );
    }
}
