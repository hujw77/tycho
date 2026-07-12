use std::{collections::HashMap, sync::Arc};

use tokio::sync::{
    mpsc::Sender,
    Mutex,
};
use tycho_common::models::ExtractorIdentity;

use crate::extractor::{
    runner::{BranchSubscriptionsMap, ControlMessage, ExtractorBuilder, ExtractorHandle},
    Extractor,
};

pub(crate) struct FamilyBranchRuntimeWiring {
    pub(crate) extractors: HashMap<String, Arc<dyn Extractor>>,
    pub(crate) subscriptions: BranchSubscriptionsMap,
    pub(crate) handles: Vec<ExtractorHandle>,
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

pub(crate) fn extractors_by_protocol_system(
    built_builders: Vec<ExtractorBuilder>,
) -> HashMap<String, Arc<dyn Extractor>> {
    built_builders
        .into_iter()
        .map(ExtractorBuilder::into_protocol_system_and_extractor)
        .collect()
}

impl FamilyBranchSubscriptionIndex {
    pub(crate) fn from_extractors(
        extractors: &HashMap<String, Arc<dyn Extractor>>,
    ) -> Self {
        let mut keys = HashMap::new();

        for (protocol_system, extractor) in extractors {
            keys.insert(protocol_system.clone(), protocol_system.clone());
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
