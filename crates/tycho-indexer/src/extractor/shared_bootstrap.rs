use std::collections::{HashMap, HashSet};

use tycho_common::{
    models::{blockchain::{TracedEntryPoint, TxWithChanges}, Chain},
    Bytes,
};
use tycho_ethereum::rpc::EthereumRpcClient;

use crate::extractor::{
    family_runtime::{default_family_runtime_registry, FamilyRuntimeRegistry},
    models::{BlockChanges, TxWithContractChanges},
    runner::{BootstrapConfig, BootstrapStrategy, ExtractorConfig},
    ExtractionError,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SharedBootstrapParams {
    pub bootstrap_block: u64,
    pub pools: Vec<Bytes>,
}

pub fn parse_pool_list_bootstrap_params(
    params: &str,
) -> Result<SharedBootstrapParams, ExtractionError> {
    let mut bootstrap_block = None;
    let mut pools = Vec::new();

    for pair in params.split('&').filter(|part| !part.is_empty()) {
        let Some((key, value)) = pair.split_once('=') else {
            return Err(ExtractionError::Setup(format!("invalid bootstrap param `{pair}`")));
        };

        match key {
            "bootstrap_block" => {
                bootstrap_block = Some(value.parse::<u64>().map_err(|err| {
                    ExtractionError::Setup(format!("parse bootstrap_block: {err}"))
                })?);
            }
            "pool" => pools.push(Bytes::from(value)),
            "pools" => {
                for pool in value.split(',').filter(|pool| !pool.is_empty()) {
                    pools.push(Bytes::from(pool));
                }
            }
            _ => return Err(ExtractionError::Setup(format!("unknown bootstrap param `{key}`"))),
        }
    }

    let bootstrap_block = bootstrap_block
        .ok_or_else(|| ExtractionError::Setup("missing `bootstrap_block`".to_string()))?;
    if pools.is_empty() {
        return Err(ExtractionError::Setup("missing `pool` or `pools`".to_string()));
    }

    Ok(SharedBootstrapParams { bootstrap_block, pools })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapBranchDescriptor {
    pub extractor_name: String,
    pub protocol_system: String,
    pub chain: Chain,
    pub strategy: BootstrapStrategy,
    pub params: SharedBootstrapParams,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SharedBootstrapPlan {
    pub family_name: Option<String>,
    pub bootstrap_block: u64,
    pub branches: Vec<BootstrapBranchDescriptor>,
}

impl SharedBootstrapPlan {
    pub fn for_extractor_config(
        config: &ExtractorConfig,
        bootstrap: &BootstrapConfig,
    ) -> Result<Self, ExtractionError> {
        Self::for_extractor_config_with_registry(
            config,
            bootstrap,
            default_family_runtime_registry(),
        )
    }

    pub fn for_extractor_config_with_registry(
        config: &ExtractorConfig,
        bootstrap: &BootstrapConfig,
        registry: FamilyRuntimeRegistry<'_>,
    ) -> Result<Self, ExtractionError> {
        Self::for_extractor_configs_with_registry([(config, bootstrap)], registry)
    }

    pub fn for_extractor_configs<'a>(
        configs: impl IntoIterator<Item = (&'a ExtractorConfig, &'a BootstrapConfig)>,
    ) -> Result<Self, ExtractionError> {
        Self::for_extractor_configs_with_registry(configs, default_family_runtime_registry())
    }

    pub fn for_extractor_configs_with_registry<'a>(
        configs: impl IntoIterator<Item = (&'a ExtractorConfig, &'a BootstrapConfig)>,
        registry: FamilyRuntimeRegistry<'_>,
    ) -> Result<Self, ExtractionError> {
        let configs = configs.into_iter().collect::<Vec<_>>();
        registry.validate()?;
        let family_name = registry.resolve_shared_bootstrap_plan_family_name(&configs)?;

        let mut branches = Vec::new();
        let mut bootstrap_block = None;

        for (config, bootstrap) in configs {
            let params = parse_and_validate_bootstrap_params(config, bootstrap, registry)?;

            if let Some(expected_block) = bootstrap_block {
                if expected_block != params.bootstrap_block {
                    return Err(ExtractionError::Setup(format!(
                        "shared bootstrap plan requires one bootstrap_block, found {} and {}",
                        expected_block, params.bootstrap_block
                    )));
                }
            } else {
                bootstrap_block = Some(params.bootstrap_block);
            }

            branches.push(BootstrapBranchDescriptor {
                extractor_name: config.name().to_owned(),
                protocol_system: config.protocol_system().to_owned(),
                chain: config.chain(),
                strategy: bootstrap.strategy.clone(),
                params,
            });
        }

        Ok(Self {
            family_name,
            bootstrap_block: bootstrap_block.ok_or_else(|| {
                ExtractionError::Setup("shared bootstrap plan contained no extractors".to_string())
            })?,
            branches,
        })
    }
}

pub async fn materialize_branch_block(
    rpc: &EthereumRpcClient,
    branch: &BootstrapBranchDescriptor,
) -> Result<BlockChanges, ExtractionError> {
    materialize_branch_block_with_registry(rpc, branch, default_family_runtime_registry()).await
}

pub async fn materialize_branch_block_with_registry(
    rpc: &EthereumRpcClient,
    branch: &BootstrapBranchDescriptor,
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<BlockChanges, ExtractionError> {
    registry.materialize_shared_bootstrap_branch(rpc, branch)?.await
}

pub async fn materialize_plan_block(
    rpc: &EthereumRpcClient,
    plan: &SharedBootstrapPlan,
) -> Result<BlockChanges, ExtractionError> {
    materialize_plan_block_with_registry(rpc, plan, default_family_runtime_registry()).await
}

pub async fn materialize_plan_block_with_registry(
    rpc: &EthereumRpcClient,
    plan: &SharedBootstrapPlan,
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<BlockChanges, ExtractionError> {
    if let Some(family_name) = plan.family_name.as_deref() {
        return registry
            .materialize_shared_bootstrap_plan(family_name, rpc, plan)?
            .await;
    }

    let mut merged = None;
    for branch in &plan.branches {
        let branch_changes = registry
            .materialize_shared_bootstrap_branch(rpc, branch)?
            .await?;
        merged = Some(match merged {
            Some(existing) => merge_family_block_changes(existing, branch_changes)?,
            None => branch_changes,
        });
    }
    merged.ok_or_else(|| {
        ExtractionError::Setup("shared bootstrap plan contained no branches".to_string())
    })
}

pub(crate) async fn materialize_plan_by_registered_branches(
    rpc: &EthereumRpcClient,
    plan: &SharedBootstrapPlan,
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<BlockChanges, ExtractionError> {
    let family_name = plan.family_name.as_deref().ok_or_else(|| {
        ExtractionError::Setup(
            "shared family bootstrap plan is missing family identity during materialization"
                .to_string(),
        )
    })?;
    let mut merged = None;

    for branch in &plan.branches {
        let member = registry.require_shared_bootstrap_member_for_family(
            family_name,
            &branch.protocol_system,
            "shared bootstrap plan for family",
        )?;
        let materialize = member
            .shared_bootstrap
            .expect("validated shared bootstrap member must have runtime")
            .materialize_branch;
        let branch_changes = materialize(rpc, branch).await?;
        merged = Some(match merged {
            Some(existing) => merge_family_block_changes(existing, branch_changes)?,
            None => branch_changes,
        });
    }

    merged.ok_or_else(|| {
        ExtractionError::Setup("shared bootstrap plan contained no branches".to_string())
    })
}

pub fn split_plan_block_by_protocol_system(
    block_changes: BlockChanges,
) -> Result<HashMap<String, BlockChanges>, ExtractionError> {
    let extractor_name = block_changes
        .extractor_name()
        .to_string();
    let chain = block_changes.chain();
    let block = block_changes.block.clone();
    let finalized_block_height = block_changes.finalized_block_height;
    let revert = block_changes.revert;
    let all_new_tokens = block_changes.new_tokens.clone();
    let (_global_component_to_system, global_account_to_system, global_entrypoint_to_system) =
        collect_block_protocol_ownership(&block_changes.txs_with_update)?;

    let mut txs_by_system: HashMap<String, Vec<TxWithChanges>> = HashMap::new();
    let mut contract_changes_by_system: HashMap<String, Vec<TxWithContractChanges>> = HashMap::new();
    let mut trace_results_by_system: HashMap<String, Vec<TracedEntryPoint>> = HashMap::new();
    let mut tokens_by_system: HashMap<String, HashSet<Bytes>> = HashMap::new();

    for tx_with_changes in block_changes.txs_with_update {
        let split = split_tx_with_changes_by_protocol_system(tx_with_changes)?;
        for (protocol_system, split_tx) in split {
            let touched_tokens = split_tx
                .balance_changes
                .values()
                .flat_map(|balances| balances.keys().cloned())
                .collect::<HashSet<_>>();
            tokens_by_system
                .entry(protocol_system.clone())
                .or_default()
                .extend(touched_tokens);
            txs_by_system
                .entry(protocol_system)
                .or_default()
                .push(split_tx);
        }
    }

    for tx_with_contract_changes in block_changes.block_contract_changes {
        let split = split_contract_changes_by_protocol_system(
            tx_with_contract_changes,
            &global_account_to_system,
        )?;
        for (protocol_system, split_tx) in split {
            contract_changes_by_system
                .entry(protocol_system)
                .or_default()
                .push(split_tx);
        }
    }

    for traced_entrypoint in block_changes.trace_results {
        let protocol_system = resolve_trace_result_protocol_system(
            &traced_entrypoint,
            &global_account_to_system,
            &global_entrypoint_to_system,
        )?;
        trace_results_by_system
            .entry(protocol_system)
            .or_default()
            .push(traced_entrypoint);
    }

    let protocol_systems = txs_by_system
        .keys()
        .chain(contract_changes_by_system.keys())
        .chain(trace_results_by_system.keys())
        .cloned()
        .collect::<HashSet<_>>();

    let mut split_blocks = HashMap::new();
    for protocol_system in protocol_systems {
        let mut txs_with_update = txs_by_system.remove(&protocol_system).unwrap_or_default();
        txs_with_update.sort_by_key(|tx| tx.tx.index);
        let mut block_contract_changes = contract_changes_by_system
            .remove(&protocol_system)
            .unwrap_or_default();
        block_contract_changes.sort_by_key(|tx| tx.tx.index);
        let trace_results = trace_results_by_system
            .remove(&protocol_system)
            .unwrap_or_default();
        let branch_tokens = tokens_by_system.get(&protocol_system);

        let mut branch_block = BlockChanges::new(
            extractor_name.clone(),
            chain,
            block.clone(),
            finalized_block_height,
            revert,
            txs_with_update,
            block_contract_changes,
        );
        branch_block.new_tokens = all_new_tokens
            .iter()
            .filter(|(token, _)| branch_tokens.is_some_and(|tokens| tokens.contains(*token)))
            .map(|(token, metadata)| (token.clone(), metadata.clone()))
            .collect();
        branch_block.trace_results = trace_results;

        split_blocks.insert(protocol_system, branch_block);
    }

    Ok(split_blocks)
}

fn collect_block_protocol_ownership(
    txs_with_update: &[TxWithChanges],
) -> Result<
    (
        HashMap<String, String>,
        HashMap<Bytes, String>,
        HashMap<String, String>,
    ),
    ExtractionError,
> {
    let mut component_to_system = HashMap::new();
    let mut account_to_system = HashMap::new();
    let mut entrypoint_to_system = HashMap::new();

    for tx_with_changes in txs_with_update {
        for (component_id, component) in &tx_with_changes.protocol_components {
            let protocol_system = component.protocol_system.clone();
            component_to_system.insert(component_id.clone(), protocol_system.clone());
            for contract_address in &component.contract_addresses {
                match account_to_system.entry(contract_address.clone()) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(protocol_system.clone());
                    }
                    std::collections::hash_map::Entry::Occupied(existing) => {
                        if existing.get() != &protocol_system {
                            return Err(ExtractionError::Setup(format!(
                                "shared bootstrap splitter found account `{contract_address:#x}` owned by multiple protocol systems `{}` and `{protocol_system}`",
                                existing.get()
                            )));
                        }
                    }
                }
            }
        }

        for (entrypoint_id, params) in &tx_with_changes.entrypoint_params {
            let touched_systems = params
                .iter()
                .map(|(_, component_id)| {
                    component_to_system
                        .get(component_id)
                        .cloned()
                        .ok_or_else(|| {
                            ExtractionError::Setup(format!(
                                "shared bootstrap splitter could not resolve entrypoint params component `{component_id}`"
                            ))
                        })
                })
                .collect::<Result<HashSet<_>, _>>()?;
            if touched_systems.len() > 1 {
                return Err(ExtractionError::Setup(format!(
                    "shared bootstrap splitter found multi-system entrypoint params for `{entrypoint_id}`"
                )));
            }
            if let Some(protocol_system) = touched_systems.into_iter().next() {
                entrypoint_to_system.insert(entrypoint_id.clone(), protocol_system);
            }
        }
    }

    Ok((component_to_system, account_to_system, entrypoint_to_system))
}

fn split_contract_changes_by_protocol_system(
    tx_with_contract_changes: TxWithContractChanges,
    account_to_system: &HashMap<Bytes, String>,
) -> Result<HashMap<String, TxWithContractChanges>, ExtractionError> {
    let mut split = HashMap::new();

    for (account, contract_changes) in tx_with_contract_changes.contract_changes {
        let protocol_system = account_to_system.get(&account).cloned().ok_or_else(|| {
            ExtractionError::Setup(format!(
                "shared bootstrap splitter could not resolve block contract changes account `{account:#x}`"
            ))
        })?;
        split
            .entry(protocol_system)
            .or_insert_with(|| TxWithContractChanges {
                tx: tx_with_contract_changes.tx.clone(),
                ..Default::default()
            })
            .contract_changes
            .insert(account, contract_changes);
    }

    Ok(split)
}

fn resolve_trace_result_protocol_system(
    traced_entrypoint: &TracedEntryPoint,
    account_to_system: &HashMap<Bytes, String>,
    entrypoint_to_system: &HashMap<String, String>,
) -> Result<String, ExtractionError> {
    let target_account = &traced_entrypoint.entry_point_with_params.entry_point.target;
    if let Some(protocol_system) = account_to_system.get(target_account) {
        return Ok(protocol_system.clone());
    }

    let entrypoint_id = traced_entrypoint.entry_point_id();
    entrypoint_to_system.get(&entrypoint_id).cloned().ok_or_else(|| {
        ExtractionError::Setup(format!(
            "shared bootstrap splitter could not resolve trace result entrypoint `{entrypoint_id}` targeting `{target_account:#x}`"
        ))
    })
}

fn split_tx_with_changes_by_protocol_system(
    tx_with_changes: TxWithChanges,
) -> Result<HashMap<String, TxWithChanges>, ExtractionError> {
    let mut component_to_system = HashMap::new();
    let mut account_to_system = HashMap::new();
    let mut split: HashMap<String, TxWithChanges> = HashMap::new();

    for (component_id, component) in tx_with_changes.protocol_components {
        let protocol_system = component.protocol_system.clone();
        component_to_system.insert(component_id.clone(), protocol_system.clone());
        for contract_address in &component.contract_addresses {
            match account_to_system.entry(contract_address.clone()) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(protocol_system.clone());
                }
                std::collections::hash_map::Entry::Occupied(existing) => {
                    if existing.get() != &protocol_system {
                        return Err(ExtractionError::Setup(format!(
                            "shared bootstrap splitter found account `{contract_address:#x}` owned by multiple protocol systems `{}` and `{protocol_system}`",
                            existing.get()
                        )));
                    }
                }
            }
        }
        split
            .entry(protocol_system)
            .or_insert_with(|| TxWithChanges {
                tx: tx_with_changes.tx.clone(),
                ..Default::default()
            })
            .protocol_components
            .insert(component_id, component);
    }

    for (component_id, state_delta) in tx_with_changes.state_updates {
        let protocol_system = component_to_system.get(&component_id).cloned().ok_or_else(|| {
            ExtractionError::Setup(format!(
                "shared bootstrap splitter could not resolve state update component `{component_id}`"
            ))
        })?;
        split
            .entry(protocol_system)
            .or_insert_with(|| TxWithChanges {
                tx: tx_with_changes.tx.clone(),
                ..Default::default()
            })
            .state_updates
            .insert(component_id, state_delta);
    }

    for (component_id, balances) in tx_with_changes.balance_changes {
        let protocol_system = component_to_system.get(&component_id).cloned().ok_or_else(|| {
            ExtractionError::Setup(format!(
                "shared bootstrap splitter could not resolve balance change component `{component_id}`"
            ))
        })?;
        split
            .entry(protocol_system)
            .or_insert_with(|| TxWithChanges {
                tx: tx_with_changes.tx.clone(),
                ..Default::default()
            })
            .balance_changes
            .insert(component_id, balances);
    }

    for (account, delta) in tx_with_changes.account_deltas {
        let protocol_system = account_to_system.get(&account).cloned().ok_or_else(|| {
            ExtractionError::Setup(format!(
                "shared bootstrap splitter could not resolve account delta account `{account:#x}`"
            ))
        })?;
        split
            .entry(protocol_system)
            .or_insert_with(|| TxWithChanges {
                tx: tx_with_changes.tx.clone(),
                ..Default::default()
            })
            .account_deltas
            .insert(account, delta);
    }

    for (account, balances) in tx_with_changes.account_balance_changes {
        let protocol_system = account_to_system.get(&account).cloned().ok_or_else(|| {
            ExtractionError::Setup(format!(
                "shared bootstrap splitter could not resolve account balance account `{account:#x}`"
            ))
        })?;
        split
            .entry(protocol_system)
            .or_insert_with(|| TxWithChanges {
                tx: tx_with_changes.tx.clone(),
                ..Default::default()
            })
            .account_balance_changes
            .insert(account, balances);
    }

    for (component_id, entrypoints) in tx_with_changes.entrypoints {
        let protocol_system = component_to_system
            .get(&component_id)
            .cloned()
            .ok_or_else(|| {
                ExtractionError::Setup(format!(
                "shared bootstrap splitter could not resolve entrypoint component `{component_id}`"
            ))
            })?;
        split
            .entry(protocol_system)
            .or_insert_with(|| TxWithChanges {
                tx: tx_with_changes.tx.clone(),
                ..Default::default()
            })
            .entrypoints
            .insert(component_id, entrypoints);
    }

    for (entrypoint_id, params) in tx_with_changes.entrypoint_params {
        let touched_systems = params
            .iter()
            .map(|(_, component_id)| {
                component_to_system
                    .get(component_id)
                    .cloned()
                    .ok_or_else(|| {
                        ExtractionError::Setup(format!(
                            "shared bootstrap splitter could not resolve entrypoint params component `{component_id}`"
                        ))
                    })
            })
            .collect::<Result<HashSet<_>, _>>()?;
        if touched_systems.len() > 1 {
            return Err(ExtractionError::Setup(format!(
                "shared bootstrap splitter found multi-system entrypoint params for `{entrypoint_id}`"
            )));
        }
        if let Some(protocol_system) = touched_systems.into_iter().next() {
            split
                .entry(protocol_system)
                .or_insert_with(|| TxWithChanges {
                    tx: tx_with_changes.tx.clone(),
                    ..Default::default()
                })
                .entrypoint_params
                .insert(entrypoint_id, params);
        }
    }

    Ok(split)
}

pub(crate) fn merge_family_block_changes(
    mut existing: BlockChanges,
    incoming: BlockChanges,
) -> Result<BlockChanges, ExtractionError> {
    for (token, metadata) in incoming.new_tokens {
        existing
            .new_tokens
            .entry(token)
            .or_insert(metadata);
    }

    for tx_with_changes in incoming.txs_with_update {
        if let Some(existing_tx) = existing
            .txs_with_update
            .iter_mut()
            .find(|current| current.tx.hash == tx_with_changes.tx.hash)
        {
            existing_tx.merge(tx_with_changes)?;
        } else {
            existing
                .txs_with_update
                .push(tx_with_changes);
        }
    }
    existing
        .txs_with_update
        .sort_by_key(|tx| tx.tx.index);

    existing
        .block_contract_changes
        .extend(incoming.block_contract_changes);
    existing
        .trace_results
        .extend(incoming.trace_results);

    Ok(existing)
}

fn parse_and_validate_bootstrap_params(
    config: &ExtractorConfig,
    bootstrap: &BootstrapConfig,
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<SharedBootstrapParams, ExtractionError> {
    let parsed_params = registry.parse_shared_bootstrap_params(
        config.protocol_system(),
        bootstrap.strategy,
        &bootstrap.params,
    )?;
    let bootstrap_start_block = u64::try_from(bootstrap.start_block).map_err(|_| {
        ExtractionError::Setup(format!(
            "bootstrap start_block must be non-negative for extractor `{}`",
            config.name()
        ))
    })?;
    let runtime_start_block = u64::try_from(config.start_block()).map_err(|_| {
        ExtractionError::Setup(format!(
            "runtime start_block must be non-negative for extractor `{}`",
            config.name()
        ))
    })?;

    if parsed_params.bootstrap_block != bootstrap_start_block {
        return Err(ExtractionError::Setup(format!(
            "bootstrap_block {} does not match bootstrap start_block {} for extractor `{}`",
            parsed_params.bootstrap_block,
            bootstrap.start_block,
            config.name()
        )));
    }

    if runtime_start_block != bootstrap_start_block {
        return Err(ExtractionError::Setup(format!(
            "runtime start_block {} does not match bootstrap start_block {} for extractor `{}`",
            config.start_block(),
            bootstrap.start_block,
            config.name()
        )));
    }

    Ok(SharedBootstrapParams {
        bootstrap_block: parsed_params.bootstrap_block,
        pools: parsed_params.pools,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use tycho_common::{
        models::{
            blockchain::{
                Block, EntryPoint, EntryPointWithTracingParams, RPCTracerParams,
                TracedEntryPoint, TracingParams, TracingResult, Transaction, TxWithChanges,
            },
            contract::{AccountBalance, AccountDelta, ContractChanges},
            protocol::{ComponentBalance, ProtocolComponent, ProtocolComponentStateDelta},
            token::Token,
            Chain, ChangeType, ImplementationType,
        },
        Bytes,
    };
    use tycho_ethereum::rpc::EthereumRpcClient;

    use crate::extractor::{
        family_registry::{
            shared_bootstrap_member_runtime, shared_family_member_spec,
            shared_family_runtime_spec,
        },
        family_runtime::{
            default_family_runtime_registry, FamilyMemberSpec, FamilyRuntimeRegistry,
            FamilyRuntimeSpec, SharedBootstrapParamsParser,
        },
        models::{BlockChanges, TxWithContractChanges},
        runner::{BootstrapConfig, BootstrapStrategy, ExtractorConfig},
        ExtractionError,
    };

    use super::{
        merge_family_block_changes, split_plan_block_by_protocol_system,
        BootstrapBranchDescriptor, SharedBootstrapPlan, SharedBootstrapParams,
    };

    fn test_extractor_config() -> ExtractorConfig {
        ExtractorConfig::new(
            "uniswap_v3".to_owned(),
            Chain::Ethereum,
            ImplementationType::Vm,
            10,
            42,
            None,
            vec![],
            "test.spkg".to_owned(),
            "map_protocol_changes".to_owned(),
            vec![],
            0,
            None,
            None,
            Default::default(),
            None,
        )
    }

    #[test]
    fn builds_shared_bootstrap_plan_for_single_extractor() {
        let config = test_extractor_config();
        let bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV3Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000001234".to_owned(),
        };

        let plan =
            SharedBootstrapPlan::for_extractor_config(&config, &bootstrap).expect("plan builds");

        assert_eq!(plan.family_name, Some("uniswap".to_string()));
        assert_eq!(plan.bootstrap_block, 42);
        assert_eq!(plan.branches.len(), 1);
        assert_eq!(plan.branches[0].extractor_name, "uniswap_v3");
        assert_eq!(plan.branches[0].protocol_system, "uniswap_v3");
        assert_eq!(plan.branches[0].chain, Chain::Ethereum);
        assert_eq!(plan.branches[0].strategy, BootstrapStrategy::UniswapV3Rpc);
        assert_eq!(plan.branches[0].params.bootstrap_block, 42);
        assert_eq!(plan.branches[0].params.pools.len(), 1);
    }

    #[test]
    fn builds_shared_bootstrap_plan_for_multiple_extractors() {
        let v3_config = test_extractor_config();
        let v2_config = ExtractorConfig::new(
            "uniswap_v2".to_owned(),
            Chain::Ethereum,
            ImplementationType::Vm,
            10,
            42,
            None,
            vec![],
            "test-v2.spkg".to_owned(),
            "map_pool_events".to_owned(),
            vec![],
            0,
            None,
            None,
            Default::default(),
            None,
        );
        let v3_bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV3Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000001234".to_owned(),
        };
        let v2_bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV2Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000005678".to_owned(),
        };

        let plan = SharedBootstrapPlan::for_extractor_configs([
            (&v2_config, &v2_bootstrap),
            (&v3_config, &v3_bootstrap),
        ])
        .expect("family plan builds");

        assert_eq!(plan.family_name, Some("uniswap".to_string()));
        assert_eq!(plan.bootstrap_block, 42);
        assert_eq!(plan.branches.len(), 2);
        assert_eq!(plan.branches[0].protocol_system, "uniswap_v2");
        assert_eq!(plan.branches[1].protocol_system, "uniswap_v3");
    }

    #[test]
    fn rejects_family_plan_with_mismatched_bootstrap_blocks() {
        let v3_config = test_extractor_config();
        let v2_config = ExtractorConfig::new(
            "uniswap_v2".to_owned(),
            Chain::Ethereum,
            ImplementationType::Vm,
            10,
            43,
            None,
            vec![],
            "test-v2.spkg".to_owned(),
            "map_pool_events".to_owned(),
            vec![],
            0,
            None,
            None,
            Default::default(),
            None,
        );
        let v3_bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV3Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000001234".to_owned(),
        };
        let v2_bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV2Rpc,
            start_block: 43,
            params: "bootstrap_block=43&pool=0x0000000000000000000000000000000000005678".to_owned(),
        };

        let err = SharedBootstrapPlan::for_extractor_configs([
            (&v2_config, &v2_bootstrap),
            (&v3_config, &v3_bootstrap),
        ])
        .expect_err("family plan should reject mismatched blocks");

        assert!(err
            .to_string()
            .contains("shared bootstrap plan requires one bootstrap_block"));
    }

    #[test]
    fn rejects_shared_bootstrap_plan_with_mismatched_chains() {
        let eth_config = test_extractor_config();
        let base_config = ExtractorConfig::new(
            "uniswap_v2_base".to_owned(),
            Chain::Base,
            ImplementationType::Vm,
            10,
            42,
            None,
            vec![],
            "test-v2.spkg".to_owned(),
            "map_pool_events".to_owned(),
            vec![],
            0,
            None,
            None,
            Default::default(),
            None,
        )
        .with_protocol_system("uniswap_v2");
        let eth_bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV3Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000001234".to_owned(),
        };
        let base_bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV2Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000005678".to_owned(),
        };

        let err = SharedBootstrapPlan::for_extractor_configs([
            (&eth_config, &eth_bootstrap),
            (&base_config, &base_bootstrap),
        ])
        .expect_err("mixed chains should fail");

        assert!(err
            .to_string()
            .contains("shared bootstrap plan requires one chain"));
    }

    #[test]
    fn rejects_shared_bootstrap_plan_with_mismatched_families() {
        let v2_config = ExtractorConfig::new(
            "uniswap_v2".to_owned(),
            Chain::Ethereum,
            ImplementationType::Vm,
            10,
            42,
            None,
            vec![],
            "test-v2.spkg".to_owned(),
            "map_pool_events".to_owned(),
            vec![],
            0,
            None,
            None,
            Default::default(),
            None,
        )
        .with_protocol_system("uniswap_v2")
        .with_family_runtime(Some(crate::extractor::runner::FamilyRuntimeConfig {
                family: "uniswap".to_string(),
                ..Default::default()
            }));
        let future_config = ExtractorConfig::new(
            "future_v1".to_owned(),
            Chain::Ethereum,
            ImplementationType::Vm,
            10,
            42,
            None,
            vec![],
            "future.spkg".to_owned(),
            "map_protocol_changes".to_owned(),
            vec![],
            0,
            None,
            None,
            Default::default(),
            None,
        )
        .with_protocol_system("future_v1")
        .with_family_runtime(Some(crate::extractor::runner::FamilyRuntimeConfig {
                family: "future_swap".to_string(),
                ..Default::default()
            }));
        let v2_bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV2Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000005678".to_owned(),
        };
        let future_bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV3Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000001234".to_owned(),
        };

        let err = SharedBootstrapPlan::for_extractor_configs([
            (&v2_config, &v2_bootstrap),
            (&future_config, &future_bootstrap),
        ])
        .expect_err("mixed family runtimes should fail");

        assert!(err
            .to_string()
            .contains("shared bootstrap plan requires one family runtime"));
    }

    #[test]
    fn rejects_shared_bootstrap_plan_with_duplicate_protocol_systems() {
        let first_v2 = ExtractorConfig::new(
            "uniswap_v2_a".to_owned(),
            Chain::Ethereum,
            ImplementationType::Vm,
            10,
            42,
            None,
            vec![],
            "test-v2.spkg".to_owned(),
            "map_pool_events".to_owned(),
            vec![],
            0,
            None,
            None,
            Default::default(),
            None,
        )
        .with_protocol_system("uniswap_v2");
        let second_v2 = ExtractorConfig::new(
            "uniswap_v2_b".to_owned(),
            Chain::Ethereum,
            ImplementationType::Vm,
            10,
            42,
            None,
            vec![],
            "test-v2-b.spkg".to_owned(),
            "map_pool_events".to_owned(),
            vec![],
            0,
            None,
            None,
            Default::default(),
            None,
        )
        .with_protocol_system("uniswap_v2");
        let first_bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV2Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000005678".to_owned(),
        };
        let second_bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV2Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000009999".to_owned(),
        };

        let err = SharedBootstrapPlan::for_extractor_configs([
            (&first_v2, &first_bootstrap),
            (&second_v2, &second_bootstrap),
        ])
        .expect_err("duplicate protocol systems should fail");

        assert!(err
            .to_string()
            .contains("shared bootstrap plan received duplicate protocol system `uniswap_v2`"));
    }

    #[test]
    fn rejects_shared_bootstrap_plan_with_partial_family_runtime_membership() {
        let v2_config = ExtractorConfig::new(
            "uniswap_v2".to_owned(),
            Chain::Ethereum,
            ImplementationType::Vm,
            10,
            42,
            None,
            vec![],
            "test-v2.spkg".to_owned(),
            "map_pool_events".to_owned(),
            vec![],
            0,
            None,
            None,
            Default::default(),
            None,
        )
        .with_protocol_system("uniswap_v2")
        .with_family_runtime(Some(crate::extractor::runner::FamilyRuntimeConfig {
            family: "uniswap".to_string(),
            ..Default::default()
        }));
        let v3_config = test_extractor_config();
        let v2_bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV2Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000005678".to_owned(),
        };
        let v3_bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV3Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000001234".to_owned(),
        };

        let err = SharedBootstrapPlan::for_extractor_configs([
            (&v2_config, &v2_bootstrap),
            (&v3_config, &v3_bootstrap),
        ])
        .expect_err("partial family runtime membership should fail");

        assert!(err
            .to_string()
            .contains("shared bootstrap plan for multiple extractors requires either a family runtime on every config or on none of them"));
    }

    #[test]
    fn rejects_shared_bootstrap_plan_with_mismatched_inferred_families() {
        const FUTURE_FAMILY: FamilyRuntimeSpec = shared_family_runtime_spec(
            "future_swap",
            &[shared_family_member_spec(
                "future_v1",
                &["futurev1"],
                Some(shared_bootstrap_member_runtime(
                    BootstrapStrategy::UniswapV2Rpc,
                    SharedBootstrapParamsParser::Custom(parse_future_params),
                    materialize_future_branch,
                )),
            )],
            "map_future_swap_family_protocol_changes",
            None,
        );
        let registry = test_registry_with_future_family(FUTURE_FAMILY);

        let future_config = ExtractorConfig::new(
            "future_v1".to_owned(),
            Chain::Ethereum,
            ImplementationType::Vm,
            10,
            99,
            None,
            vec![],
            "future.spkg".to_owned(),
            "map_protocol_changes".to_owned(),
            vec![],
            0,
            None,
            None,
            Default::default(),
            None,
        )
        .with_protocol_system("future_v1");
        let future_bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV2Rpc,
            start_block: 99,
            params: "bootstrap_block=99&pool=0x0000000000000000000000000000000000000011".to_owned(),
        };
        let uniswap_config = test_extractor_config();
        let uniswap_bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV3Rpc,
            start_block: 99,
            params: "bootstrap_block=99&pool=0x0000000000000000000000000000000000001234".to_owned(),
        };

        let err = SharedBootstrapPlan::for_extractor_configs_with_registry(
            [(&future_config, &future_bootstrap), (&uniswap_config, &uniswap_bootstrap)],
            registry,
        )
        .expect_err("mixed inferred families should fail");

        assert!(err
            .to_string()
            .contains("shared bootstrap plan requires one inferred family runtime"));
    }

    #[test]
    fn rejects_shared_bootstrap_plan_with_invalid_custom_registry() {
        const INVALID_FUTURE_FAMILY: FamilyRuntimeSpec = FamilyRuntimeSpec {
            family_name: "future_swap",
            members: &[FamilyMemberSpec {
                protocol_system: "future_v1",
                shared_route_protocols: &["futurev1"],
                shared_bootstrap: None,
            }],
            output_module: "map_future_swap_family_protocol_changes",
            shared_bootstrap_runtime: None,
        };
        let registry = FamilyRuntimeRegistry::new(&[INVALID_FUTURE_FAMILY]);
        let future_config = ExtractorConfig::new(
            "future_v1".to_owned(),
            Chain::Ethereum,
            ImplementationType::Vm,
            10,
            99,
            None,
            vec![],
            "future.spkg".to_owned(),
            "map_protocol_changes".to_owned(),
            vec![],
            0,
            None,
            None,
            Default::default(),
            None,
        )
        .with_protocol_system("future_v1");
        let future_bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV2Rpc,
            start_block: 99,
            params: "bootstrap_block=99&pool=0x0000000000000000000000000000000000000011".to_owned(),
        };

        let err = SharedBootstrapPlan::for_extractor_configs_with_registry(
            [(&future_config, &future_bootstrap)],
            registry,
        )
        .expect_err("invalid custom registry should fail before plan construction");

        assert!(err
            .to_string()
            .contains("does not declare a shared bootstrap strategy"));
    }

    fn parse_future_params(params: &str) -> Result<SharedBootstrapParams, ExtractionError> {
        let pool = params
            .split("pool=")
            .nth(1)
            .ok_or_else(|| ExtractionError::Setup("missing pool".to_string()))?;
        Ok(SharedBootstrapParams {
            bootstrap_block: 99,
            pools: vec![Bytes::from(pool)],
        })
    }

    fn materialize_future_branch<'a>(
        _: &'a EthereumRpcClient,
        _: &'a BootstrapBranchDescriptor,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<BlockChanges, ExtractionError>> + Send + 'a>,
    > {
        Box::pin(async {
            Err(ExtractionError::Setup(
                "test-only future family materialization should not run".to_string(),
            ))
        })
    }

    fn test_registry_with_future_family(
        future_family: FamilyRuntimeSpec,
    ) -> FamilyRuntimeRegistry<'static> {
        let mut specs = default_family_runtime_registry().specs().to_vec();
        specs.push(future_family);
        FamilyRuntimeRegistry::new(Box::leak(specs.into_boxed_slice()))
    }

    #[test]
    fn builds_shared_bootstrap_plan_for_future_family_with_custom_registry() {
        const FUTURE_FAMILY: FamilyRuntimeSpec = shared_family_runtime_spec(
            "future_swap",
            &[
                shared_family_member_spec(
                    "future_v1",
                    &["futurev1"],
                    Some(shared_bootstrap_member_runtime(
                        BootstrapStrategy::UniswapV2Rpc,
                        SharedBootstrapParamsParser::Custom(parse_future_params),
                        materialize_future_branch,
                    )),
                ),
                shared_family_member_spec(
                    "future_v2",
                    &["futurev2"],
                    Some(shared_bootstrap_member_runtime(
                        BootstrapStrategy::UniswapV2Rpc,
                        SharedBootstrapParamsParser::Custom(parse_future_params),
                        materialize_future_branch,
                    )),
                ),
            ],
            "map_future_swap_family_protocol_changes",
            None,
        );
        let registry = test_registry_with_future_family(FUTURE_FAMILY);

        let v1_config = ExtractorConfig::new(
            "future_v1".to_owned(),
            Chain::Ethereum,
            ImplementationType::Vm,
            10,
            99,
            None,
            vec![],
            "future.spkg".to_owned(),
            "map_protocol_changes".to_owned(),
            vec![],
            0,
            None,
            None,
            Default::default(),
            None,
        );
        let v2_config = ExtractorConfig::new(
            "future_v2".to_owned(),
            Chain::Ethereum,
            ImplementationType::Vm,
            10,
            99,
            None,
            vec![],
            "future.spkg".to_owned(),
            "map_protocol_changes".to_owned(),
            vec![],
            0,
            None,
            None,
            Default::default(),
            None,
        );
        let v1_bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV2Rpc,
            start_block: 99,
            params: "bootstrap_block=99&pool=0x0000000000000000000000000000000000000011".to_owned(),
        };
        let v2_bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV2Rpc,
            start_block: 99,
            params: "bootstrap_block=99&pool=0x0000000000000000000000000000000000000022".to_owned(),
        };

        let plan = SharedBootstrapPlan::for_extractor_configs_with_registry(
            [(&v1_config, &v1_bootstrap), (&v2_config, &v2_bootstrap)],
            registry,
        )
        .expect("future family plan builds from custom registry");

        assert_eq!(plan.family_name, Some("future_swap".to_string()));
        assert_eq!(plan.bootstrap_block, 99);
        assert_eq!(plan.branches.len(), 2);
        assert_eq!(plan.branches[0].protocol_system, "future_v1");
        assert_eq!(plan.branches[1].protocol_system, "future_v2");
        assert_eq!(
            plan.branches[0].params.pools,
            vec![Bytes::from("0x0000000000000000000000000000000000000011")]
        );
        assert_eq!(
            plan.branches[1].params.pools,
            vec![Bytes::from("0x0000000000000000000000000000000000000022")]
        );
    }

    #[test]
    fn merges_family_bootstrap_block_changes_by_transaction_hash() {
        let block = Block {
            number: 42,
            hash: Bytes::from(vec![0x01; 32]),
            parent_hash: Bytes::from(vec![0x02; 32]),
            chain: Chain::Ethereum,
            ts: chrono::DateTime::from_timestamp(1_718_000_000, 0)
                .expect("timestamp")
                .naive_utc(),
        };
        let tx = Transaction {
            hash: Bytes::from(vec![0xaa; 32]),
            block_hash: block.hash.clone(),
            from: Bytes::from(vec![0x11; 20]),
            to: None,
            index: 7,
        };
        let v2 = BlockChanges::new(
            "uniswap_v2".to_owned(),
            Chain::Ethereum,
            block.clone(),
            42,
            false,
            vec![TxWithChanges {
                tx: tx.clone(),
                protocol_components: HashMap::from([(
                    "v2-pool".to_string(),
                    ProtocolComponent { id: "v2-pool".to_string(), ..Default::default() },
                )]),
                ..Default::default()
            }],
            vec![],
        );
        let v3 = BlockChanges::new(
            "uniswap_v3".to_owned(),
            Chain::Ethereum,
            block,
            42,
            false,
            vec![TxWithChanges {
                tx,
                protocol_components: HashMap::from([(
                    "v3-pool".to_string(),
                    ProtocolComponent { id: "v3-pool".to_string(), ..Default::default() },
                )]),
                ..Default::default()
            }],
            vec![],
        );

        let merged = merge_family_block_changes(v2, v3).expect("merge succeeds");

        assert_eq!(merged.txs_with_update.len(), 1);
        assert_eq!(
            merged.txs_with_update[0]
                .protocol_components
                .len(),
            2
        );
        assert!(merged.txs_with_update[0]
            .protocol_components
            .contains_key("v2-pool"));
        assert!(merged.txs_with_update[0]
            .protocol_components
            .contains_key("v3-pool"));
    }

    #[test]
    fn splits_merged_family_bootstrap_block_by_protocol_system() {
        let block = Block {
            number: 42,
            hash: Bytes::from(vec![0x01; 32]),
            parent_hash: Bytes::from(vec![0x02; 32]),
            chain: Chain::Ethereum,
            ts: chrono::DateTime::from_timestamp(1_718_000_000, 0)
                .expect("timestamp")
                .naive_utc(),
        };
        let tx = Transaction {
            hash: Bytes::from(vec![0xaa; 32]),
            block_hash: block.hash.clone(),
            from: Bytes::from(vec![0x11; 20]),
            to: None,
            index: 7,
        };
        let v2_contract = Bytes::from(vec![0x41; 20]);
        let v3_contract = Bytes::from(vec![0x51; 20]);
        let v2_entrypoint_id = "v2-entrypoint".to_string();
        let v3_entrypoint_id = "v3-entrypoint".to_string();

        let mut merged = BlockChanges::new(
            "uniswap_family".to_owned(),
            Chain::Ethereum,
            block,
            42,
            false,
            vec![TxWithChanges {
                tx: tx.clone(),
                protocol_components: HashMap::from([
                    (
                        "v2-pool".to_string(),
                        ProtocolComponent {
                            id: "v2-pool".to_string(),
                            protocol_system: "uniswap_v2".to_string(),
                            contract_addresses: vec![v2_contract.clone()],
                            ..Default::default()
                        },
                    ),
                    (
                        "v3-pool".to_string(),
                        ProtocolComponent {
                            id: "v3-pool".to_string(),
                            protocol_system: "uniswap_v3".to_string(),
                            contract_addresses: vec![v3_contract.clone()],
                            ..Default::default()
                        },
                    ),
                ]),
                state_updates: HashMap::from([
                    (
                        "v2-pool".to_string(),
                        ProtocolComponentStateDelta::new("v2-pool", HashMap::new(), HashSet::new()),
                    ),
                    (
                        "v3-pool".to_string(),
                        ProtocolComponentStateDelta::new("v3-pool", HashMap::new(), HashSet::new()),
                    ),
                ]),
                balance_changes: HashMap::from([
                    (
                        "v2-pool".to_string(),
                        HashMap::from([(
                            Bytes::from(vec![0x21; 20]),
                            ComponentBalance::new(
                                Bytes::from(vec![0x21; 20]),
                                Bytes::from(vec![0x01]),
                                1.0,
                                tx.hash.clone(),
                                "v2-pool",
                            ),
                        )]),
                    ),
                    (
                        "v3-pool".to_string(),
                        HashMap::from([(
                            Bytes::from(vec![0x31; 20]),
                            ComponentBalance::new(
                                Bytes::from(vec![0x31; 20]),
                                Bytes::from(vec![0x02]),
                                2.0,
                                tx.hash.clone(),
                                "v3-pool",
                            ),
                        )]),
                    ),
                ]),
                account_deltas: HashMap::from([
                    (
                        v2_contract.clone(),
                        AccountDelta::new(
                            Chain::Ethereum,
                            v2_contract.clone(),
                            HashMap::new(),
                            Some(Bytes::from(vec![0x0a])),
                            None,
                            ChangeType::Update,
                        ),
                    ),
                    (
                        v3_contract.clone(),
                        AccountDelta::new(
                            Chain::Ethereum,
                            v3_contract.clone(),
                            HashMap::new(),
                            Some(Bytes::from(vec![0x0b])),
                            None,
                            ChangeType::Update,
                        ),
                    ),
                ]),
                account_balance_changes: HashMap::from([
                    (
                        v2_contract.clone(),
                        HashMap::from([(
                            Bytes::from(vec![0x21; 20]),
                            AccountBalance {
                                token: Bytes::from(vec![0x21; 20]),
                                balance: Bytes::from(vec![0x03]),
                                modify_tx: tx.hash.clone(),
                                account: v2_contract.clone(),
                            },
                        )]),
                    ),
                    (
                        v3_contract.clone(),
                        HashMap::from([(
                            Bytes::from(vec![0x31; 20]),
                            AccountBalance {
                                token: Bytes::from(vec![0x31; 20]),
                                balance: Bytes::from(vec![0x04]),
                                modify_tx: tx.hash.clone(),
                                account: v3_contract.clone(),
                            },
                        )]),
                    ),
                ]),
                entrypoint_params: HashMap::from([
                    (
                        v2_entrypoint_id.clone(),
                        HashSet::from([(
                            TracingParams::RPCTracer(RPCTracerParams::new(
                                Some(Bytes::from(vec![0x12; 20])),
                                Bytes::from(vec![0xde, 0xad]),
                            )),
                            "v2-pool".to_string(),
                        )]),
                    ),
                    (
                        v3_entrypoint_id.clone(),
                        HashSet::from([(
                            TracingParams::RPCTracer(RPCTracerParams::new(
                                Some(Bytes::from(vec![0x13; 20])),
                                Bytes::from(vec![0xbe, 0xef]),
                            )),
                            "v3-pool".to_string(),
                        )]),
                    ),
                ]),
                ..Default::default()
            }],
            vec![TxWithContractChanges {
                tx: tx.clone(),
                contract_changes: HashMap::from([
                    (
                        v2_contract.clone(),
                        ContractChanges::new(
                            v2_contract.clone(),
                            HashMap::from([(
                                Bytes::from(vec![0x01; 32]),
                                tycho_common::models::contract::ContractStorageChange::initial(
                                    Bytes::from(vec![0xaa]),
                                ),
                            )]),
                            Some(Bytes::from(vec![0x05])),
                        ),
                    ),
                    (
                        v3_contract.clone(),
                        ContractChanges::new(
                            v3_contract.clone(),
                            HashMap::from([(
                                Bytes::from(vec![0x02; 32]),
                                tycho_common::models::contract::ContractStorageChange::initial(
                                    Bytes::from(vec![0xbb]),
                                ),
                            )]),
                            Some(Bytes::from(vec![0x06])),
                        ),
                    ),
                ]),
            }],
        );
        merged.new_tokens = HashMap::from([
            (
                Bytes::from(vec![0x21; 20]),
                Token::new(
                    &Bytes::from(vec![0x21; 20]),
                    "TKA",
                    18,
                    0,
                    &[Some(2300)],
                    Chain::Ethereum,
                    100,
                ),
            ),
            (
                Bytes::from(vec![0x31; 20]),
                Token::new(
                    &Bytes::from(vec![0x31; 20]),
                    "TKB",
                    18,
                    0,
                    &[Some(2300)],
                    Chain::Ethereum,
                    100,
                ),
            ),
        ]);
        merged.trace_results = vec![
            TracedEntryPoint::new(
                EntryPointWithTracingParams::new(
                    EntryPoint::new(
                        v2_entrypoint_id.clone(),
                        v2_contract.clone(),
                        "sync()".to_string(),
                    ),
                    TracingParams::RPCTracer(RPCTracerParams::new(
                        Some(Bytes::from(vec![0x12; 20])),
                        Bytes::from(vec![0xde, 0xad]),
                    )),
                ),
                Bytes::from(vec![0x91; 32]),
                TracingResult::new(HashSet::new(), HashMap::new()),
            ),
            TracedEntryPoint::new(
                EntryPointWithTracingParams::new(
                    EntryPoint::new(
                        v3_entrypoint_id.clone(),
                        v3_contract.clone(),
                        "swap()".to_string(),
                    ),
                    TracingParams::RPCTracer(RPCTracerParams::new(
                        Some(Bytes::from(vec![0x13; 20])),
                        Bytes::from(vec![0xbe, 0xef]),
                    )),
                ),
                Bytes::from(vec![0x92; 32]),
                TracingResult::new(HashSet::new(), HashMap::new()),
            ),
        ];

        let split = split_plan_block_by_protocol_system(merged).expect("split succeeds");

        assert_eq!(split.len(), 2);
        assert_eq!(
            split["uniswap_v2"].txs_with_update[0]
                .protocol_components
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["v2-pool".to_string()]
        );
        assert_eq!(
            split["uniswap_v3"].txs_with_update[0]
                .protocol_components
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["v3-pool".to_string()]
        );
        assert_eq!(split["uniswap_v2"].new_tokens.len(), 1);
        assert_eq!(split["uniswap_v3"].new_tokens.len(), 1);
        assert_eq!(split["uniswap_v2"].block_contract_changes.len(), 1);
        assert_eq!(split["uniswap_v3"].block_contract_changes.len(), 1);
        assert_eq!(
            split["uniswap_v2"].block_contract_changes[0]
                .contract_changes
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec![v2_contract.clone()]
        );
        assert_eq!(
            split["uniswap_v3"].trace_results[0].entry_point_id(),
            v3_entrypoint_id
        );
        assert_eq!(split["uniswap_v2"].txs_with_update[0].account_deltas.len(), 1);
        assert_eq!(
            split["uniswap_v2"].txs_with_update[0]
                .account_deltas
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec![v2_contract]
        );
        assert_eq!(
            split["uniswap_v3"].txs_with_update[0]
                .account_balance_changes
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec![v3_contract.clone()]
        );
        assert_eq!(
            split["uniswap_v2"].trace_results[0].entry_point_id(),
            v2_entrypoint_id
        );
    }
}
