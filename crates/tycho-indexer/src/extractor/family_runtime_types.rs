use std::collections::{HashMap, HashSet};

use tycho_common::models::Chain;

use crate::extractor::{
    extractor_config::ExtractorConfig,
    family_bootstrap_registry::{ResolvedSharedBootstrapExecution, ResolvedSharedBootstrapRuntime},
    family_dispatch::FamilyBranchSpec,
    family_registry::FamilyRuntimeRegistry,
    family_runtime_detection::detect_family_runtimes_with_registry,
    family_runtime_metadata::{ResolvedSharedFamilyStream, SharedStreamTarget},
    family_runtime_resolution::{
        resolve_family_runtime_plan_with_registry, ResolvedFamilyExecutionConfig,
        ResolvedFamilyRuntimeContract, ResolvedFamilySharedRuntime,
    },
    protocol_message_registry::ProtocolSystemAuxiliaryRuntimeHooks,
    runtime_target_planning::{
        ResolvedRuntimeTarget, ResolvedRuntimeTargets, ResolvedStandaloneRuntime,
    },
    shared_bootstrap::SharedBootstrapPlan,
    ExtractionError,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DetectedFamilyRuntime {
    family_name: String,
    chain: Chain,
    member_protocol_systems: Vec<String>,
    shared_target: SharedStreamTarget,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FamilyRuntimeMembership {
    pub family_name: String,
    pub chain: Chain,
    pub member_protocol_systems: Vec<String>,
}

pub trait FamilyRuntimeMembershipView {
    fn family_name(&self) -> &str;
    fn chain(&self) -> Chain;
    fn member_protocol_systems(&self) -> &[String];
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FamilyRuntimeBuildPlan {
    pub families: Vec<DetectedFamilyRuntime>,
    pub standalone_protocol_systems: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct ResolvedFamilyRuntime<'a> {
    pub family: FamilyRuntimeMembership,
    pub extractor_configs: Vec<&'a ExtractorConfig>,
    pub(crate) shared_runtime: ResolvedFamilySharedRuntime,
    pub(crate) execution: ResolvedFamilyExecutionConfig,
}

#[derive(Clone, Debug)]
pub struct ResolvedFamilyRuntimePlan<'a> {
    pub families: Vec<ResolvedFamilyRuntime<'a>>,
    pub standalone_extractors: Vec<ResolvedStandaloneRuntime<'a>>,
}

impl<'a> FamilyRuntimeRegistry<'a> {
    pub fn detected_family_runtime(
        &self,
        family_name: &str,
        chain: Chain,
        shared_spkg: impl Into<String>,
    ) -> Result<DetectedFamilyRuntime, ExtractionError> {
        let spec = self.require_family_spec(family_name, "family runtime")?;
        let shared_stream =
            self.resolved_shared_stream_for_family(chain, family_name, shared_spkg)?;
        Ok(DetectedFamilyRuntime {
            family_name: spec.family_name().to_string(),
            chain,
            member_protocol_systems: spec
                .members()
                .iter()
                .map(|member| member.protocol_system.to_string())
                .collect(),
            shared_target: SharedStreamTarget {
                spkg: shared_stream.spkg,
                module: shared_stream.module,
            },
        })
    }

    pub fn detect_family_runtimes(
        &self,
        extractors: &HashMap<String, ExtractorConfig>,
    ) -> Result<Vec<DetectedFamilyRuntime>, ExtractionError> {
        detect_family_runtimes_with_registry(extractors, *self)
    }

    pub fn build_family_runtime_plan(
        &self,
        extractors: &HashMap<String, ExtractorConfig>,
    ) -> Result<FamilyRuntimeBuildPlan, ExtractionError> {
        let families = self.detect_family_runtimes(extractors)?;
        Ok(FamilyRuntimeBuildPlan::from_detected_families(extractors, families))
    }

    pub fn build_resolved_family_runtime_plan<'b>(
        &self,
        extractors: &'b HashMap<String, ExtractorConfig>,
    ) -> Result<ResolvedFamilyRuntimePlan<'b>, ExtractionError> {
        let runtime_plan = self.build_family_runtime_plan(extractors)?;
        resolve_family_runtime_plan_with_registry(extractors, runtime_plan, *self)
    }

    pub fn resolve_runtime_targets<'b>(
        &self,
        extractors: &'b HashMap<String, ExtractorConfig>,
    ) -> Result<ResolvedRuntimeTargets<'b>, ExtractionError> {
        let resolved = self.build_resolved_family_runtime_plan(extractors)?;
        let mut targets = resolved
            .families
            .into_iter()
            .map(ResolvedRuntimeTarget::Family)
            .collect::<Vec<_>>();
        targets.extend(
            resolved
                .standalone_extractors
                .into_iter()
                .map(ResolvedRuntimeTarget::Standalone),
        );
        Ok(ResolvedRuntimeTargets::new(targets))
    }
}

impl DetectedFamilyRuntime {
    pub fn family_name(&self) -> &str {
        &self.family_name
    }

    pub fn chain(&self) -> Chain {
        self.chain
    }

    pub fn member_protocol_systems(&self) -> &[String] {
        &self.member_protocol_systems
    }

    pub fn runtime_membership(&self) -> FamilyRuntimeMembership {
        FamilyRuntimeMembership {
            family_name: self.family_name().to_string(),
            chain: self.chain(),
            member_protocol_systems: self.member_protocol_systems().to_vec(),
        }
    }

    pub fn shared_spkg(&self) -> &str {
        &self.shared_target.spkg
    }

    pub fn output_module(&self) -> &str {
        &self.shared_target.module
    }

    pub fn resolved_shared_stream_with_registry(
        &self,
        registry: FamilyRuntimeRegistry<'_>,
    ) -> Result<ResolvedSharedFamilyStream, ExtractionError> {
        registry.resolved_shared_stream_for_family(
            self.chain(),
            self.family_name(),
            self.shared_spkg(),
        )
    }
}

impl FamilyRuntimeBuildPlan {
    pub fn from_detected_families(
        extractors: &HashMap<String, ExtractorConfig>,
        families: Vec<DetectedFamilyRuntime>,
    ) -> Self {
        let handled = families
            .iter()
            .flat_map(|family| family.member_protocol_systems().iter().cloned())
            .collect::<HashSet<_>>();
        let mut standalone_protocol_systems = extractors
            .values()
            .map(|config| config.protocol_system().to_string())
            .filter(|name| !handled.contains(name))
            .collect::<Vec<_>>();
        standalone_protocol_systems.sort();
        standalone_protocol_systems.dedup();

        Self { families, standalone_protocol_systems }
    }
}

impl FamilyRuntimeMembershipView for DetectedFamilyRuntime {
    fn family_name(&self) -> &str {
        DetectedFamilyRuntime::family_name(self)
    }

    fn chain(&self) -> Chain {
        DetectedFamilyRuntime::chain(self)
    }

    fn member_protocol_systems(&self) -> &[String] {
        DetectedFamilyRuntime::member_protocol_systems(self)
    }
}

impl FamilyRuntimeMembershipView for FamilyRuntimeMembership {
    fn family_name(&self) -> &str {
        &self.family_name
    }

    fn chain(&self) -> Chain {
        self.chain
    }

    fn member_protocol_systems(&self) -> &[String] {
        &self.member_protocol_systems
    }
}

impl<'a> ResolvedFamilyRuntime<'a> {
    pub fn family_name(&self) -> &str {
        &self.family.family_name
    }

    pub fn chain(&self) -> Chain {
        self.family.chain
    }

    pub fn member_protocol_systems(&self) -> &[String] {
        &self.family.member_protocol_systems
    }

    pub fn configured_start_block(&self) -> i64 {
        self.shared_runtime.configured_start_block
    }

    pub fn stop_block(&self) -> u64 {
        self.shared_runtime.stop_block()
    }

    pub fn merged_substreams_params(&self) -> &HashMap<String, String> {
        self.shared_runtime.merged_substreams_params().as_map()
    }

    pub fn shared_spkg(&self) -> &str {
        &self.runtime_contract().resolved_shared_stream().spkg
    }

    pub fn shared_extractor_id(&self) -> &str {
        self.runtime_contract().shared_extractor_id()
    }

    pub fn output_module(&self) -> &str {
        &self.runtime_contract().resolved_shared_stream().module
    }

    pub fn durability_scope(&self) -> &str {
        self.runtime_contract().durability_scope()
    }

    pub fn shared_stream_name(&self) -> &str {
        self.runtime_contract().shared_stream_name()
    }

    pub fn branch_specs(&self) -> &[FamilyBranchSpec] {
        self.execution.runtime_contract.branch_specs()
    }

    pub fn shared_bootstrap_plan(&self) -> Option<&SharedBootstrapPlan> {
        self.shared_runtime
            .shared_bootstrap_runtime
            .as_ref()
            .map(|runtime| &runtime.plan)
    }

    pub fn shared_bootstrap_runtime(&self) -> Option<&ResolvedSharedBootstrapRuntime> {
        self.shared_runtime.shared_bootstrap_runtime.as_ref()
    }

    pub fn shared_bootstrap_execution(&self) -> &ResolvedSharedBootstrapExecution {
        &self
            .shared_runtime
            .shared_bootstrap_runtime
            .as_ref()
            .expect("shared bootstrap execution requires precomputed shared bootstrap runtime")
            .execution
    }

    #[cfg(test)]
    pub(crate) fn shared_bootstrap_runtime_mut(
        &mut self,
    ) -> Option<&mut ResolvedSharedBootstrapRuntime> {
        self.shared_runtime.shared_bootstrap_runtime.as_mut()
    }

    pub fn runtime_contract(&self) -> &ResolvedFamilyRuntimeContract {
        &self.execution.runtime_contract
    }

    pub(crate) fn shared_progress_owner_protocol_system(&self) -> &str {
        self.execution
            .runtime_contract
            .shared_progress_owner_protocol_system()
    }

    pub(crate) fn auxiliary_runtime_hooks_by_protocol_system(
        &self,
    ) -> &HashMap<String, ProtocolSystemAuxiliaryRuntimeHooks> {
        &self.execution.auxiliary_runtime_hooks_by_protocol_system
    }
}
