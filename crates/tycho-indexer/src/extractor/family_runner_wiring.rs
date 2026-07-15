use std::{collections::HashMap, sync::Arc};

use tokio::sync::{mpsc::Sender, Mutex};
use tycho_common::models::ExtractorIdentity;

use crate::extractor::{
    control::{BranchSubscriptionsMap, ControlMessage, ExtractorHandle},
    family_runtime_planning::ResolvedFamilyRuntimeContract,
    shared_bootstrap::MaterializedBootstrapCommitTarget,
    ExtractionError,
    Extractor,
};

pub(crate) struct FamilyBranchRuntimeWiring {
    pub(crate) extractors: HashMap<String, Arc<dyn Extractor>>,
    pub(crate) subscriptions: BranchSubscriptionsMap,
    pub(crate) handles: Vec<ExtractorHandle>,
}

#[derive(Clone)]
pub(crate) struct FamilyBootstrapCommitWiring {
    branch_targets: Vec<MaterializedBootstrapCommitTarget>,
    completion_extractor: Arc<dyn Extractor>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct FamilyBranchSubscriptionIndex {
    keys: HashMap<String, String>,
}

impl FamilyBranchRuntimeWiring {
    pub(crate) fn from_extractors(
        extractors: HashMap<String, Arc<dyn Extractor>>,
        control_tx: &Sender<ControlMessage>,
    ) -> Self {
        let subscriptions = extractors
            .keys()
            .map(|protocol_system| (protocol_system.clone(), Arc::new(Mutex::new(HashMap::new()))))
            .collect::<BranchSubscriptionsMap>();
        let handles = extractors
            .values()
            .map(|extractor| ExtractorHandle::new(extractor.get_id(), control_tx.clone()))
            .collect::<Vec<_>>();
        Self { extractors, subscriptions, handles }
    }
}

impl FamilyBootstrapCommitWiring {
    pub(crate) fn from_runtime_contract(
        runtime_contract: &ResolvedFamilyRuntimeContract,
        extractors: &HashMap<String, Arc<dyn Extractor>>,
    ) -> Result<Self, ExtractionError> {
        let mut branch_targets = Vec::with_capacity(runtime_contract.branch_specs().len());
        let mut completion_extractor = None;

        for branch in runtime_contract.branch_specs() {
            let extractor = extractors.get(&branch.protocol_system).ok_or_else(|| {
                ExtractionError::Setup(format!(
                    "missing family bootstrap extractor for {}",
                    branch.protocol_system
                ))
            })?;
            branch_targets.push(MaterializedBootstrapCommitTarget::protocol_system_branch(
                branch.protocol_system.clone(),
                extractor.clone(),
            ));
            if completion_extractor.is_none() {
                completion_extractor = Some(extractor.clone());
            }
        }

        let Some(completion_extractor) = completion_extractor else {
            return Err(ExtractionError::Setup(
                "shared bootstrap plan contained no family branch extractors".to_string(),
            ));
        };

        Ok(Self { branch_targets, completion_extractor })
    }

    pub(crate) fn branch_targets(&self) -> Vec<MaterializedBootstrapCommitTarget> {
        self.branch_targets.clone()
    }

    pub(crate) fn completion_extractor(&self) -> Arc<dyn Extractor> {
        self.completion_extractor.clone()
    }
}

#[cfg(test)]
pub(crate) fn extractors_by_protocol_system(
    extractors: Vec<(String, Arc<dyn Extractor>)>,
) -> HashMap<String, Arc<dyn Extractor>> {
    extractors.into_iter().collect()
}

impl FamilyBranchSubscriptionIndex {
    pub(crate) fn from_branch_protocol_systems(
        branch_protocol_systems: impl IntoIterator<Item = impl Into<String>>,
        extractors: &HashMap<String, Arc<dyn Extractor>>,
    ) -> Self {
        let mut keys = HashMap::new();

        for protocol_system in branch_protocol_systems {
            let protocol_system = protocol_system.into();
            keys.insert(protocol_system.clone(), protocol_system);
        }

        for (protocol_system, extractor) in extractors {
            keys.insert(extractor.get_id().name.clone(), protocol_system.clone());
        }

        Self { keys }
    }

    #[cfg(test)]
    pub(crate) fn get(&self, key: &str) -> Option<&str> {
        self.keys.get(key).map(String::as_str)
    }

    pub(crate) fn resolve_or_learn(
        &mut self,
        extractor_id: &ExtractorIdentity,
        extractors: &HashMap<String, Arc<dyn Extractor>>,
    ) -> Option<String> {
        if let Some(existing) = self.keys.get(&extractor_id.name) {
            return Some(existing.clone());
        }

        let resolved = extractors
            .iter()
            .find_map(|(protocol_system, extractor)| {
                (extractor.get_id().name == extractor_id.name).then(|| protocol_system.clone())
            });
        if let Some(protocol_system) = &resolved {
            self.keys
                .insert(extractor_id.name.clone(), protocol_system.clone());
        }
        resolved
    }
}
