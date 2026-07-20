use std::{collections::HashMap, sync::Arc};

use tycho_common::models::ExtractorIdentity;

use crate::extractor::{
    control::{
        build_runtime_control_wiring, new_branch_subscriptions_map, BranchSubscriptionsMap,
        RuntimeControlWiring,
    },
    family_runtime_resolution::ResolvedFamilyRuntimeContract,
    shared_bootstrap::MaterializedBootstrapCommitTarget,
    ExtractionError,
    Extractor,
};
#[cfg(test)]
use crate::extractor::{
    family_registry::FamilyRuntimeRegistry,
    shared_bootstrap::{
        resolve_shared_progress_owner_protocol_system_for_plan, SharedBootstrapPlan,
    },
};

pub(crate) struct FamilyBranchRuntimeWiring {
    pub(crate) extractors: HashMap<String, Arc<dyn Extractor>>,
    pub(crate) subscriptions: BranchSubscriptionsMap,
    pub(crate) control: RuntimeControlWiring,
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
    pub(crate) fn from_extractors(extractors: HashMap<String, Arc<dyn Extractor>>) -> Self {
        let control = build_runtime_control_wiring(
            extractors.values().map(|extractor| extractor.get_id()),
        );
        let subscriptions = new_branch_subscriptions_map(extractors.keys().cloned());
        Self { extractors, subscriptions, control }
    }
}

impl FamilyBootstrapCommitWiring {
    #[cfg(test)]
    pub(crate) fn from_shared_bootstrap_plan_with_registry(
        plan: &SharedBootstrapPlan,
        registry: FamilyRuntimeRegistry<'_>,
        extractors: &HashMap<String, Arc<dyn Extractor>>,
    ) -> Result<Self, ExtractionError> {
        let shared_progress_owner_protocol_system =
            resolve_shared_progress_owner_protocol_system_for_plan(plan, registry)?;

        Self::from_branch_protocol_systems(
            &shared_progress_owner_protocol_system,
            plan.branches.iter().map(|branch| branch.protocol_system.as_str()),
            extractors,
        )
    }

    pub(crate) fn from_branch_protocol_systems(
        shared_progress_owner_protocol_system: &str,
        branch_protocol_systems: impl IntoIterator<Item = impl AsRef<str>>,
        extractors: &HashMap<String, Arc<dyn Extractor>>,
    ) -> Result<Self, ExtractionError> {
        let completion_extractor = extractors
            .get(shared_progress_owner_protocol_system)
            .cloned()
            .ok_or_else(|| {
                ExtractionError::Setup(format!(
                    "missing family bootstrap completion extractor for shared progress owner {}",
                    shared_progress_owner_protocol_system
                ))
            })?;

        let mut branch_targets = Vec::new();
        for protocol_system in branch_protocol_systems {
            let protocol_system = protocol_system.as_ref();
            let extractor = extractors.get(protocol_system).ok_or_else(|| {
                ExtractionError::Setup(format!(
                    "missing family bootstrap extractor for {}",
                    protocol_system
                ))
            })?;
            branch_targets.push(MaterializedBootstrapCommitTarget::protocol_system_branch(
                protocol_system.to_string(),
                extractor.clone(),
            ));
        }

        Ok(Self { branch_targets, completion_extractor })
    }

    pub(crate) fn from_runtime_contract(
        runtime_contract: &ResolvedFamilyRuntimeContract,
        extractors: &HashMap<String, Arc<dyn Extractor>>,
    ) -> Result<Self, ExtractionError> {
        Self::from_branch_protocol_systems(
            runtime_contract.shared_progress_owner_protocol_system(),
            runtime_contract.branch_protocol_systems(),
            extractors,
        )
    }

    pub(crate) fn branch_targets(&self) -> Vec<MaterializedBootstrapCommitTarget> {
        self.branch_targets.clone()
    }

    pub(crate) fn completion_extractor(&self) -> Arc<dyn Extractor> {
        self.completion_extractor.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extractor::{
        family_dispatch::FamilyBranchSpec,
        family_runtime_metadata::ResolvedSharedFamilyStream,
        MockExtractor,
    };
    use tycho_common::models::{Chain, ExtractorIdentity};

    #[test]
    fn bootstrap_commit_wiring_uses_shared_progress_owner_as_completion_extractor() {
        let mut v2 = MockExtractor::new();
        v2.expect_get_id()
            .returning(|| ExtractorIdentity::new(Chain::Ethereum, "uniswap_v2"));
        let mut v3 = MockExtractor::new();
        v3.expect_get_id()
            .returning(|| ExtractorIdentity::new(Chain::Ethereum, "uniswap_v3"));

        let extractors = HashMap::from([
            ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
            ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
        ]);
        let runtime_contract = ResolvedFamilyRuntimeContract::new(
            ResolvedSharedFamilyStream {
                shared_stream_name: "uniswap_family".to_string(),
                spkg: String::new(),
                module: String::new(),
                extractor_id: "ethereum:uniswap_family".to_string(),
                durability_scope: "family::uniswap".to_string(),
            },
            vec![
                FamilyBranchSpec {
                    protocol_system: "uniswap_v2".to_string(),
                    protocol_type_names: Default::default(),
                },
                FamilyBranchSpec {
                    protocol_system: "uniswap_v3".to_string(),
                    protocol_type_names: Default::default(),
                },
            ],
            "uniswap_v3",
        );

        let wiring = FamilyBootstrapCommitWiring::from_runtime_contract(
            &runtime_contract,
            &extractors,
        )
        .expect("family bootstrap wiring should build");

        assert_eq!(wiring.completion_extractor().get_id().name, "uniswap_v3");
    }

    #[test]
    fn bootstrap_commit_wiring_builds_from_branch_protocol_systems() {
        let mut v2 = MockExtractor::new();
        v2.expect_get_id()
            .returning(|| ExtractorIdentity::new(Chain::Ethereum, "uniswap_v2"));
        let mut v3 = MockExtractor::new();
        v3.expect_get_id()
            .returning(|| ExtractorIdentity::new(Chain::Ethereum, "uniswap_v3"));

        let extractors = HashMap::from([
            ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
            ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
        ]);

        let wiring = FamilyBootstrapCommitWiring::from_branch_protocol_systems(
            "uniswap_v3",
            ["uniswap_v2", "uniswap_v3"],
            &extractors,
        )
        .expect("bootstrap wiring should build from branch protocol systems");

        assert_eq!(wiring.branch_targets().len(), 2);
        assert_eq!(wiring.completion_extractor().get_id().name, "uniswap_v3");
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

    pub(crate) fn resolve(
        &self,
        extractor_id: &ExtractorIdentity,
    ) -> Option<String> {
        self.keys.get(&extractor_id.name).cloned()
    }
}
