use std::collections::{HashMap, HashSet};

use tycho_substreams::pb::tycho::evm::v1 as substreams;

use crate::extractor::{protocol_cache::ProtocolMemoryCache, ExtractionError};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FamilyDispatcherSeed {
    pub component_systems: HashMap<String, String>,
    pub contract_systems: HashMap<Vec<u8>, String>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct FamilyDispatchRegistry {
    branch_protocol_systems: HashSet<String>,
    protocol_type_to_system: HashMap<String, String>,
    component_to_system: HashMap<String, String>,
    contract_to_system: HashMap<Vec<u8>, String>,
}

impl FamilyDispatchRegistry {
    pub(crate) fn new(
        branch_protocol_systems: HashSet<String>,
        protocol_type_to_system: HashMap<String, String>,
    ) -> Self {
        Self {
            branch_protocol_systems,
            protocol_type_to_system,
            component_to_system: HashMap::new(),
            contract_to_system: HashMap::new(),
        }
    }

    pub(crate) fn branch_protocol_systems(&self) -> &HashSet<String> {
        &self.branch_protocol_systems
    }

    pub(crate) fn component_system(&self, component_id: &str) -> Option<&String> {
        self.component_to_system
            .get(component_id)
    }

    pub(crate) fn contract_system(&self, contract_address: &[u8]) -> Option<&String> {
        self.contract_to_system
            .get(contract_address)
    }

    pub(crate) fn register_component_system(
        &mut self,
        component_id: impl Into<String>,
        protocol_system: impl Into<String>,
    ) {
        self.component_to_system
            .insert(component_id.into(), protocol_system.into());
    }

    pub(crate) fn register_component_systems(
        &mut self,
        component_systems: impl IntoIterator<Item = (String, String)>,
    ) {
        self.component_to_system
            .extend(component_systems);
    }

    pub(crate) fn register_contract_system(
        &mut self,
        contract_address: impl Into<Vec<u8>>,
        protocol_system: impl Into<String>,
    ) {
        self.contract_to_system
            .insert(contract_address.into(), protocol_system.into());
    }

    pub(crate) fn register_contract_systems(
        &mut self,
        contract_systems: impl IntoIterator<Item = (Vec<u8>, String)>,
    ) {
        self.contract_to_system
            .extend(contract_systems);
    }

    pub(crate) fn admit_component_change(
        &mut self,
        component_change: &substreams::ProtocolComponent,
    ) -> Result<String, ExtractionError> {
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

        Ok(protocol_system)
    }

    pub(crate) fn resolve_component_system(
        &self,
        component_id: &str,
    ) -> Result<String, ExtractionError> {
        self.component_to_system
            .get(component_id)
            .cloned()
            .ok_or_else(|| {
                ExtractionError::DecodeError(format!(
                    "unknown component `{component_id}` while routing family block changes"
                ))
            })
    }

    pub(crate) fn resolve_storage_systems(
        &self,
        storage_changes: &substreams::TransactionStorageChanges,
    ) -> HashSet<String> {
        storage_changes
            .storage_changes
            .iter()
            .filter_map(|change| {
                self.contract_to_system
                    .get(&change.address)
                    .cloned()
            })
            .collect()
    }

    pub(crate) async fn hydrate_from_protocol_cache_by_component_ids(
        &mut self,
        protocol_cache: &ProtocolMemoryCache,
        component_ids: &HashSet<String>,
    ) -> Result<bool, ExtractionError> {
        let components = protocol_cache
            .ensure_protocol_components_by_id(component_ids)
            .await?;

        let mut hydrated_components = HashMap::new();
        let mut hydrated_contracts = HashMap::new();
        for component in components.into_values() {
            if !self
                .branch_protocol_systems
                .contains(&component.protocol_system)
            {
                continue;
            }
            hydrated_components.insert(component.id.clone(), component.protocol_system.clone());
            for contract in component.contract_addresses {
                hydrated_contracts.insert(contract.to_vec(), component.protocol_system.clone());
            }
        }

        if hydrated_components.is_empty() && hydrated_contracts.is_empty() {
            return Ok(false);
        }

        self.register_component_systems(hydrated_components);
        self.register_contract_systems(hydrated_contracts);
        Ok(true)
    }

    pub(crate) async fn hydrate_from_protocol_cache_by_contract_addresses(
        &mut self,
        protocol_cache: &ProtocolMemoryCache,
        contract_addresses: &HashSet<Vec<u8>>,
    ) -> Result<bool, ExtractionError> {
        let components = protocol_cache
            .cached_protocol_components_by_contract_addresses(
                contract_addresses,
                &self.branch_protocol_systems,
            )
            .await;

        let mut hydrated_components = HashMap::new();
        let mut hydrated_contracts = HashMap::new();
        for component in components.into_values() {
            if !self
                .branch_protocol_systems
                .contains(&component.protocol_system)
            {
                continue;
            }
            hydrated_components.insert(component.id.clone(), component.protocol_system.clone());
            for contract in component.contract_addresses {
                hydrated_contracts.insert(contract.to_vec(), component.protocol_system.clone());
            }
        }

        if hydrated_components.is_empty() && hydrated_contracts.is_empty() {
            return Ok(false);
        }

        self.register_component_systems(hydrated_components);
        self.register_contract_systems(hydrated_contracts);
        Ok(true)
    }
}
