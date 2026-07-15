#[cfg(test)]
use std::any::Any;

use tokio::task::JoinHandle;

use crate::extractor::ExtractionError;

pub use crate::extractor::family_runtime_execution::FamilyExtractorRunner;
pub use crate::extractor::single_runtime_execution::ExtractorRunner;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManagedRunnerKind {
    Single,
    Family,
}

pub(crate) trait ManagedRuntime: Send {
    fn run(self: Box<Self>) -> JoinHandle<Result<(), ExtractionError>>;

    #[allow(dead_code)]
    fn kind(&self) -> ManagedRunnerKind;

    #[cfg(test)]
    fn as_any(&self) -> &dyn Any;

    #[cfg(test)]
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

impl ManagedRuntime for ExtractorRunner {
    fn run(self: Box<Self>) -> JoinHandle<Result<(), ExtractionError>> {
        (*self).run()
    }

    fn kind(&self) -> ManagedRunnerKind {
        ManagedRunnerKind::Single
    }

    #[cfg(test)]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[cfg(test)]
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl ManagedRuntime for FamilyExtractorRunner {
    fn run(self: Box<Self>) -> JoinHandle<Result<(), ExtractionError>> {
        (*self).run()
    }

    fn kind(&self) -> ManagedRunnerKind {
        ManagedRunnerKind::Family
    }

    #[cfg(test)]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[cfg(test)]
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

pub struct ManagedRunner {
    runner: Box<dyn ManagedRuntime>,
}

impl ManagedRunner {
    pub(crate) fn new<T>(runner: T) -> Self
    where
        T: ManagedRuntime + 'static,
    {
        Self { runner: Box::new(runner) }
    }

    pub fn run(self) -> JoinHandle<Result<(), ExtractionError>> {
        self.runner.run()
    }

    #[allow(dead_code)]
    pub fn kind(&self) -> ManagedRunnerKind {
        self.runner.kind()
    }

    #[cfg(test)]
    pub(crate) fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.runner.as_any().downcast_ref::<T>()
    }

    #[cfg(test)]
    pub(crate) fn into_typed<T: 'static>(self) -> T {
        *self
            .runner
            .into_any()
            .downcast::<T>()
            .expect("managed runner should contain the requested concrete runtime type")
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::{HashMap, HashSet},
        sync::Arc,
    };

    use super::*;
    use crate::substreams::stream::{BlockResponse, SubstreamsStream};
    use crate::{
        extractor::{
            extractor_config::{
                BootstrapConfig, BootstrapStrategy, DCIType, ExtractorConfig, ProtocolTypeConfig,
            },
            family_bootstrap_registry::ResolvedSharedBootstrapBranchRuntime,
            family_dispatch::{FamilyBlockChangesDispatcher, FamilyBranchSpec},
            family_lifecycle::{
                apply_family_bootstrap_plan, family_bootstrap_already_completed,
                resolve_family_stream_position, run_family_bootstrap_if_needed,
                ResolvedFamilyStreamPosition,
            },
            family_managed_startup::PreparedFamilyRunnerStartup,
            family_runner_wiring::{
                extractors_by_protocol_system, FamilyBranchRuntimeWiring,
                FamilyBranchSubscriptionIndex,
            },
            family_runtime_execution::FamilyRuntimeState,
            family_runtime_metadata::FamilyRuntimeConfig,
            managed_stream_startup::build_test_single_runner,
            protocol_cache::ProtocolDataCache,
            shared_bootstrap::SharedBootstrapPlan,
            validate_shared_progress_consistency, MockExtractor,
        },
        pb::sf::substreams::v1::Clock,
        testing::{
            family_output_module_for_tests, family_shared_extractor_id_for_tests,
            family_shared_stream_name_for_tests, MockGateway,
        },
    };
    use chrono::NaiveDateTime;
    use futures03::stream;
    use prost::Message;
    use tokio::sync::{mpsc, Mutex};
    use tracing::{error, info};
    use tycho_common::{
        models::{
            blockchain::Block, blockchain::BlockAggregatedChanges, protocol::ProtocolComponent,
            token::Token, Chain, ChangeType, ExtractorIdentity, FinancialType, ImplementationType,
        },
        storage::WithTotal,
        Bytes,
    };
    use tycho_substreams::pb::tycho::evm::v1 as substreams;

    #[path = "support.rs"]
    mod support;
    use support::*;

    #[path = "runner_family_tests.rs"]
    mod family_runner_tests;

    #[path = "runner_family_runtime_wiring_tests.rs"]
    mod family_runner_runtime_wiring_tests;

    #[path = "runner_family_lifecycle_tests.rs"]
    mod family_runner_lifecycle_tests;

    #[path = "runner_family_planning_tests.rs"]
    mod family_runner_planning_tests;

    #[path = "runner_family_bootstrap_tests.rs"]
    mod family_runner_bootstrap_tests;

    #[path = "runner_family_runtime_metadata_tests.rs"]
    mod family_runner_runtime_metadata_tests;

    #[path = "runner_single_runtime_tests.rs"]
    mod single_runtime_tests;

    #[test]
    fn test_extractor_config_without_dci_plugin() {
        let yaml = r#"
name: uniswap_v2
chain: ethereum
implementation_type: Custom
sync_batch_size: 1000
start_block: 10008300
protocol_types:
  - name: uniswap_v2_pool
    financial_type: Swap
spkg: substreams/ethereum-uniswap-v2/ethereum-uniswap-v2-v0.3.0.spkg
module_name: map_pool_events
"#;

        let config: ExtractorConfig =
            serde_yaml::from_str(yaml).expect("Failed to deserialize YAML");

        // Verify basic fields
        assert_eq!(config.name, "uniswap_v2");

        // Verify DCI plugin is None (optional field)
        assert!(config.dci_plugin.is_none());
    }

    #[test]
    fn test_dci_extractor_config() {
        let yaml = r#"
name: uniswap_v3
chain: ethereum
implementation_type: Custom
sync_batch_size: 1000
start_block: 12369621
protocol_types:
  - name: uniswap_v3_pool
    financial_type: Swap
spkg: substreams/ethereum-uniswap-v3/ethereum-uniswap-v3-logs-only-0.1.1.spkg
module_name: map_protocol_changes
dci_plugin:
  type: rpc
"#;

        let config: ExtractorConfig =
            serde_yaml::from_str(yaml).expect("Failed to deserialize YAML");

        // Verify basic fields
        assert_eq!(config.name, "uniswap_v3");

        // Verify DCI plugin is RPC
        assert!(
            matches!(config.dci_plugin, Some(DCIType::RPC)),
            "Expected RPC DCI plugin but got {:?}",
            config.dci_plugin
        );
    }

    #[test]
    fn test_uniswap_v4_hooks_dci_extractor_config() {
        let yaml = r#"
name: uniswap_v4
chain: ethereum
implementation_type: Custom
sync_batch_size: 1000
start_block: 21688329
protocol_types:
  - name: uniswap_v4_pool
    financial_type: Swap
spkg: substreams/ethereum-uniswap-v4/ethereum-uniswap-v4-v0.2.1.spkg
module_name: map_protocol_changes
dci_plugin:
  type: uniswap_v4_hooks
  router_address: "0x2e234DAe75C793f67A35089C9d99245E1C58470b"
  pool_manager_address: "0x000000000004444c5dc75cB358380D2e3dE08A90"
"#;

        let config: ExtractorConfig =
            serde_yaml::from_str(yaml).expect("Failed to deserialize YAML");

        // Verify basic fields
        assert_eq!(config.name, "uniswap_v4");
        assert_eq!(config.chain, Chain::Ethereum);
        assert_eq!(config.sync_batch_size, 1000);
        assert_eq!(config.start_block, 21688329);

        // Verify protocol types
        assert_eq!(config.protocol_types.len(), 1);
        assert_eq!(config.protocol_types[0].name, "uniswap_v4_pool");

        // Verify DCI plugin configuration
        let dci_plugin = config
            .dci_plugin
            .expect("Expected dci_plugin to be set");
        match dci_plugin {
            DCIType::UniswapV4Hooks { pool_manager_address } => {
                assert_eq!(pool_manager_address, "0x000000000004444c5dc75cB358380D2e3dE08A90");
            }
            _ => {
                panic!("Expected UniswapV4Hooks DCI plugin but got RPC");
            }
        }
    }

    #[tokio::test]
    async fn test_extractor_runner_builder_fresh_start_no_db_state() {
        // No DB state: get_last_processed_block returns None, so the stream
        // starts from the config start_block with no cursor.
        let mut mock_extractor = MockExtractor::new();
        mock_extractor
            .expect_get_last_processed_block()
            .returning(|| None);
        mock_extractor
            .expect_get_cursor()
            .returning(String::new);
        mock_extractor
            .expect_get_completed_bootstrap_block()
            .returning(|| Ok(None));
        mock_extractor
            .expect_flush()
            .returning(|| Ok(()));
        mock_extractor
            .expect_get_id()
            .returning(ExtractorIdentity::default);

        // Build the ExtractorRunnerBuilder
        let extractor = Arc::new(mock_extractor);
        let config = ExtractorConfig {
            name: "test_module".to_owned(),
            implementation_type: ImplementationType::Vm,
            protocol_types: vec![ProtocolTypeConfig {
                name: "test_module_pool".to_owned(),
                financial_type: FinancialType::Swap,
            }],
            spkg: "./test/spkg/substreams-ethereum-quickstart-v1.0.0.spkg".to_owned(),
            module_name: "test_module".to_owned(),
            ..Default::default()
        };

        // Run the builder
        let (runner, _handle) = build_test_single_runner(
            &config,
            extractor,
            "https://mainnet.eth.streamingfast.io",
            None,
            "test_token",
            false,
            false,
            None,
        )
        .await
        .unwrap();

        // Wait for the handle to complete
        match runner.run().await {
            Ok(_) => {
                info!("ExtractorRunnerBuilder completed successfully");
            }
            Err(err) => {
                error!(error = %err, "ExtractorRunnerBuilder failed");
                panic!("ExtractorRunnerBuilder failed");
            }
        }
    }

    #[tokio::test]
    async fn test_start_block_no_db_state() {
        use crate::substreams::mock::start_mock_substreams;

        let (captured, addr) = start_mock_substreams().await;

        let mut mock_extractor = MockExtractor::new();
        mock_extractor
            .expect_get_last_processed_block()
            .returning(|| None);
        mock_extractor
            .expect_get_cursor()
            .returning(String::new);
        mock_extractor
            .expect_get_completed_bootstrap_block()
            .returning(|| Ok(None));
        mock_extractor
            .expect_flush()
            .returning(|| Ok(()));
        mock_extractor
            .expect_get_id()
            .returning(ExtractorIdentity::default);

        let extractor = Arc::new(mock_extractor);
        let config = ExtractorConfig {
            name: "test_module".to_owned(),
            implementation_type: ImplementationType::Vm,
            protocol_types: vec![ProtocolTypeConfig {
                name: "test_module_pool".to_owned(),
                financial_type: FinancialType::Swap,
            }],
            spkg: "./test/spkg/substreams-ethereum-quickstart-v1.0.0.spkg".to_owned(),
            module_name: "test_module".to_owned(),
            start_block: 42,
            substreams_params: HashMap::from([(
                "test_module".to_owned(),
                "bootstrap_block=42&pool=0x1234".to_owned(),
            )]),
            ..Default::default()
        };

        let (runner, _handle) = build_test_single_runner(
            &config,
            extractor,
            &format!("http://{addr}"),
            None,
            "test_token",
            false,
            false,
            None,
        )
        .await
        .unwrap();
        let handle = runner.run();
        handle.await.unwrap().unwrap();

        let requests = captured.lock().unwrap();
        assert_eq!(requests.len(), 1, "expected exactly one gRPC request");
        assert_eq!(requests[0].start_block_num, 42);
        assert!(requests[0].start_cursor.is_empty(), "fresh start should have no cursor");
        assert_eq!(
            requests[0].params.get("test_module"),
            Some(&"bootstrap_block=42&pool=0x1234".to_owned())
        );
    }

    #[tokio::test]
    async fn test_start_block_with_db_state() {
        use chrono::NaiveDateTime;
        use tycho_common::models::blockchain::Block;

        use crate::substreams::mock::start_mock_substreams;

        let (captured, addr) = start_mock_substreams().await;

        let mut mock_extractor = MockExtractor::new();
        mock_extractor
            .expect_get_last_processed_block()
            .returning(|| {
                Some(Block::new(
                    1000,
                    Chain::Ethereum,
                    vec![0x01].into(),
                    vec![0x00].into(),
                    NaiveDateTime::default(),
                ))
            });
        mock_extractor
            .expect_get_cursor()
            .returning(String::new);
        mock_extractor
            .expect_get_completed_bootstrap_block()
            .returning(|| Ok(None));
        mock_extractor
            .expect_flush()
            .returning(|| Ok(()));
        mock_extractor
            .expect_get_id()
            .returning(ExtractorIdentity::default);

        let extractor = Arc::new(mock_extractor);
        let config = ExtractorConfig {
            name: "test_module".to_owned(),
            implementation_type: ImplementationType::Vm,
            protocol_types: vec![ProtocolTypeConfig {
                name: "test_module_pool".to_owned(),
                financial_type: FinancialType::Swap,
            }],
            spkg: "./test/spkg/substreams-ethereum-quickstart-v1.0.0.spkg".to_owned(),
            module_name: "test_module".to_owned(),
            start_block: 500,
            ..Default::default()
        };

        let (runner, _handle) = build_test_single_runner(
            &config,
            extractor,
            &format!("http://{addr}"),
            None,
            "test_token",
            false,
            false,
            None,
        )
        .await
        .unwrap();
        let handle = runner.run();
        handle.await.unwrap().unwrap();

        let requests = captured.lock().unwrap();
        assert_eq!(requests.len(), 1, "expected exactly one gRPC request");
        assert_eq!(
            requests[0].start_block_num, 1001,
            "should use last_committed + 1, not config's start_block"
        );
        assert!(requests[0].start_cursor.is_empty(), "fresh start should have no cursor");
    }

    #[tokio::test]
    async fn test_skip_bootstrap_when_completed_state_exists() {
        use crate::substreams::mock::start_mock_substreams;

        let (captured, addr) = start_mock_substreams().await;

        let mut mock_extractor = MockExtractor::new();
        mock_extractor
            .expect_get_last_processed_block()
            .returning(|| None);
        mock_extractor
            .expect_get_cursor()
            .returning(String::new);
        mock_extractor
            .expect_get_completed_bootstrap_block()
            .returning(|| Ok(Some(42)));
        mock_extractor
            .expect_flush()
            .returning(|| Ok(()));
        mock_extractor
            .expect_get_id()
            .returning(ExtractorIdentity::default);

        let extractor = Arc::new(mock_extractor);
        let config = ExtractorConfig {
            name: "uniswap_v3".to_owned(),
            implementation_type: ImplementationType::Custom,
            protocol_types: vec![ProtocolTypeConfig {
                name: "uniswap_v3_pool".to_owned(),
                financial_type: FinancialType::Swap,
            }],
            spkg: "./test/spkg/substreams-ethereum-quickstart-v1.0.0.spkg".to_owned(),
            module_name: "map_protocol_changes".to_owned(),
            start_block: 42,
            bootstrap: Some(BootstrapConfig {
                strategy: BootstrapStrategy::UniswapV3Rpc,
                start_block: 42,
                params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000001234"
                    .to_owned(),
            }),
            ..Default::default()
        };

        let (runner, _handle) = build_test_single_runner(
            &config,
            extractor,
            &format!("http://{addr}"),
            None,
            "test_token",
            false,
            false,
            None,
        )
        .await
        .unwrap();
        let handle = runner.run();
        handle.await.unwrap().unwrap();

        let requests = captured.lock().unwrap();
        assert_eq!(requests.len(), 1, "expected exactly one gRPC request");
        assert_eq!(
            requests[0].start_block_num, 43,
            "should start from bootstrap block + 1 when bootstrap is already completed"
        );
        assert!(requests[0].start_cursor.is_empty(), "fresh start should have no cursor");
    }

    #[tokio::test]
    async fn test_extractor_runner_flushes_on_stream_end() {
        let mut extractor = MockExtractor::new();
        extractor
            .expect_get_id()
            .return_const(ExtractorIdentity::default());
        extractor
            .expect_flush()
            .once()
            .returning(|| Ok(()));

        let runner = ExtractorRunner::new(
            Arc::new(extractor),
            SubstreamsStream::from_stream(Box::pin(stream::iter(vec![Ok(BlockResponse::Ended)]))),
            Arc::new(Mutex::new(HashMap::new())),
            mpsc::channel(4).1,
            None,
            false,
        );

        runner.run().await.unwrap().unwrap();
    }

    #[test]
    fn test_validate_bootstrap_config_accepts_matching_runtime_blocks() {
        let config = ExtractorConfig {
            name: "uniswap_v3".to_owned(),
            start_block: 42,
            ..Default::default()
        };
        let bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV3Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000001234".to_owned(),
        };

        let plan = SharedBootstrapPlan::for_extractor_config(&config, &bootstrap)
            .expect("matching bootstrap config should validate");

        assert_eq!(plan.bootstrap_block, 42);
        assert_eq!(plan.branches.len(), 1);
    }

    #[test]
    fn test_validate_bootstrap_config_rejects_runtime_block_mismatch() {
        let config = ExtractorConfig {
            name: "uniswap_v3".to_owned(),
            start_block: 43,
            ..Default::default()
        };
        let bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV3Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000001234".to_owned(),
        };

        let err = SharedBootstrapPlan::for_extractor_config(&config, &bootstrap)
            .expect_err("mismatched start blocks must fail");

        assert!(err
            .to_string()
            .contains("runtime start_block"));
    }
}
