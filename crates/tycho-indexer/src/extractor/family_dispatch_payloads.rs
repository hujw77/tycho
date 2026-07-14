use std::collections::{HashMap, HashSet};

use prost::Message;
use tycho_substreams::pb::tycho::evm::v1 as substreams;

use crate::{
    extractor::ExtractionError,
    pb::sf::substreams::rpc::v2::{BlockScopedData, MapModuleOutput},
};

fn decode_family_block_scoped_data_payload(
    block_scoped_data: &BlockScopedData,
) -> Result<(MapModuleOutput, prost_types::Any, substreams::BlockChanges), ExtractionError> {
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
    Ok((output, map_output, raw_msg))
}

pub fn dispatch_block_scoped_data_by_protocol_system(
    block_scoped_data: BlockScopedData,
    dispatched: HashMap<String, substreams::BlockChanges>,
) -> Result<HashMap<String, BlockScopedData>, ExtractionError> {
    let (output, map_output, _) = decode_family_block_scoped_data_payload(&block_scoped_data)?;

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

pub fn decode_family_block_scoped_data_changes(
    block_scoped_data: &BlockScopedData,
) -> Result<substreams::BlockChanges, ExtractionError> {
    let (_, _, block_changes) = decode_family_block_scoped_data_payload(block_scoped_data)?;
    Ok(block_changes)
}

pub fn referenced_component_ids_from_block_changes(
    msg: &substreams::BlockChanges,
) -> Result<HashSet<String>, ExtractionError> {
    let mut component_ids = HashSet::new();

    for tx_changes in &msg.changes {
        for component_change in &tx_changes.component_changes {
            component_ids.insert(component_change.id.clone());
        }
        for entity_change in &tx_changes.entity_changes {
            component_ids.insert(entity_change.component_id.clone());
        }
        for balance_change in &tx_changes.balance_changes {
            let component_id =
                String::from_utf8(balance_change.component_id.clone()).map_err(|err| {
                    ExtractionError::DecodeError(format!(
                        "balance change component id is not utf8: {err}"
                    ))
                })?;
            component_ids.insert(component_id);
        }
        for entrypoint in &tx_changes.entrypoints {
            component_ids.insert(entrypoint.component_id.clone());
        }
        for entrypoint_params in &tx_changes.entrypoint_params {
            if let Some(component_id) = &entrypoint_params.component_id {
                component_ids.insert(component_id.clone());
            }
        }
    }

    Ok(component_ids)
}

pub fn referenced_component_ids_from_block_scoped_data(
    block_scoped_data: &BlockScopedData,
) -> Result<HashSet<String>, ExtractionError> {
    let raw_msg = decode_family_block_scoped_data_changes(block_scoped_data)?;
    referenced_component_ids_from_block_changes(&raw_msg)
}

pub fn referenced_contract_addresses_from_block_changes(
    msg: &substreams::BlockChanges,
) -> HashSet<Vec<u8>> {
    let mut contract_addresses = HashSet::new();

    for tx_changes in &msg.changes {
        for component_change in &tx_changes.component_changes {
            for contract in &component_change.contracts {
                contract_addresses.insert(contract.clone());
            }
        }
        for contract_change in &tx_changes.contract_changes {
            contract_addresses.insert(contract_change.address.clone());
        }
    }

    for storage_changes in &msg.storage_changes {
        for storage_change in &storage_changes.storage_changes {
            contract_addresses.insert(storage_change.address.clone());
        }
    }

    contract_addresses
}

pub fn referenced_contract_addresses_from_block_scoped_data(
    block_scoped_data: &BlockScopedData,
) -> Result<HashSet<Vec<u8>>, ExtractionError> {
    let raw_msg = decode_family_block_scoped_data_changes(block_scoped_data)?;
    Ok(referenced_contract_addresses_from_block_changes(&raw_msg))
}
