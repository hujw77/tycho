use std::collections::{HashMap, HashSet};

use tycho_substreams::pb::tycho::evm::v1 as substreams;

use crate::extractor::{family_dispatch_registry::FamilyDispatchRegistry, ExtractionError};

pub(crate) fn split_family_block_changes(
    registry: &mut FamilyDispatchRegistry,
    msg: substreams::BlockChanges,
) -> Result<HashMap<String, substreams::BlockChanges>, ExtractionError> {
    let block = msg.block.clone();
    pre_register_block_components(registry, &msg)?;
    let mut txs_by_system: HashMap<String, Vec<substreams::TransactionChanges>> = HashMap::new();
    let mut tx_systems_by_hash: HashMap<Vec<u8>, HashSet<String>> = HashMap::new();

    for tx_changes in msg.changes {
        let tx = tx_changes.tx.clone().ok_or_else(|| {
            ExtractionError::DecodeError("TransactionChanges misses a transaction".to_string())
        })?;
        let tx_hash = tx.hash.clone();
        let (split_txs, touched_systems) = dispatch_transaction_changes(registry, tx_changes)?;

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
                let inferred_systems = registry.resolve_storage_systems(&storage_changes);
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
    let mut all_systems = registry
        .branch_protocol_systems()
        .clone();
    all_systems.extend(txs_by_system.keys().cloned());
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

fn pre_register_block_components(
    registry: &mut FamilyDispatchRegistry,
    msg: &substreams::BlockChanges,
) -> Result<(), ExtractionError> {
    for tx_changes in &msg.changes {
        for component_change in &tx_changes.component_changes {
            registry.admit_component_change(component_change)?;
        }
    }

    Ok(())
}

fn dispatch_transaction_changes(
    registry: &mut FamilyDispatchRegistry,
    tx_changes: substreams::TransactionChanges,
) -> Result<(HashMap<String, substreams::TransactionChanges>, HashSet<String>), ExtractionError> {
    let tx = tx_changes.tx.clone().ok_or_else(|| {
        ExtractionError::DecodeError("TransactionChanges misses a transaction".to_string())
    })?;
    let mut split_txs: HashMap<String, substreams::TransactionChanges> = HashMap::new();
    let mut touched_systems = HashSet::new();

    for component_change in tx_changes.component_changes {
        let protocol_system = registry.admit_component_change(&component_change)?;
        touched_systems.insert(protocol_system.clone());
        split_txs
            .entry(protocol_system)
            .or_insert_with(|| empty_transaction_changes(&tx))
            .component_changes
            .push(component_change);
    }

    for entity_change in tx_changes.entity_changes {
        let protocol_system = registry.resolve_component_system(&entity_change.component_id)?;
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
        let protocol_system = registry.resolve_component_system(&component_id)?;
        touched_systems.insert(protocol_system.clone());
        split_txs
            .entry(protocol_system)
            .or_insert_with(|| empty_transaction_changes(&tx))
            .balance_changes
            .push(balance_change);
    }

    for entrypoint in tx_changes.entrypoints {
        let protocol_system = registry.resolve_component_system(&entrypoint.component_id)?;
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
        let protocol_system = registry.resolve_component_system(&component_id)?;
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
                .filter_map(|change| {
                    registry
                        .contract_system(&change.address)
                        .cloned()
                })
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
