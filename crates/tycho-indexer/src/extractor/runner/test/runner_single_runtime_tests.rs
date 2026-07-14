use std::sync::Arc;

use super::support::make_block_scoped_data;
use super::*;
use crate::{
    extractor::{Extractor, ExtractorMsg},
    pb::sf::substreams::rpc::v2::BlockScopedData,
};
use tycho_common::models::blockchain::BlockAggregatedChanges;

fn one_msg() -> ExtractorMsg {
    Arc::new(BlockAggregatedChanges::default())
}

#[tokio::test]
async fn test_process_block_data_partial_blocks_disabled() {
    let data = make_block_scoped_data(false, None, None);
    let mut mock = MockExtractor::new();
    mock.expect_handle_tick_scoped_data()
        .once()
        .returning(|inp: BlockScopedData| {
            assert!(!inp.is_partial, "data must be sent as full block");
            Ok(Some(one_msg()))
        });
    let extractor: Arc<dyn Extractor> = Arc::new(mock);

    let msgs = ExtractorRunner::process_block_data(extractor.as_ref(), &data, false)
        .await
        .unwrap();
    assert_eq!(msgs.len(), 1);
}

#[tokio::test]
async fn test_process_block_data_final_partial() {
    let data = make_block_scoped_data(true, Some(2), Some(true));
    let mut mock = MockExtractor::new();
    mock.expect_handle_tick_scoped_data()
        .once()
        .returning(|inp: BlockScopedData| {
            assert_eq!(inp.partial_index, Some(2));
            assert_eq!(inp.is_last_partial, Some(true));
            Ok(Some(one_msg()))
        });
    mock.expect_collect_and_process_full_block()
        .once()
        .returning(|_cursor: String, _final_block_height: u64, _clock: Option<Clock>| {
            Ok(Some(one_msg()))
        });
    let extractor: Arc<dyn Extractor> = Arc::new(mock);

    let msgs = ExtractorRunner::process_block_data(extractor.as_ref(), &data, true)
        .await
        .unwrap();
    assert_eq!(msgs.len(), 2);
}

#[tokio::test]
async fn test_process_block_data_full_block() {
    let data = make_block_scoped_data(false, None, None);
    let mut mock = MockExtractor::new();
    mock.expect_handle_tick_scoped_data()
        .once()
        .returning(|inp: BlockScopedData| {
            assert!(!inp.is_partial, "data is sent as full block");
            Ok(Some(one_msg()))
        });
    let extractor: Arc<dyn Extractor> = Arc::new(mock);

    let msgs = ExtractorRunner::process_block_data(extractor.as_ref(), &data, true)
        .await
        .unwrap();
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].partial_block_index, Some(0));
    assert!(msgs[1].partial_block_index.is_none());
}

#[tokio::test]
async fn test_process_block_data_middle_partial() {
    let data = make_block_scoped_data(true, Some(1), Some(false));
    let mut mock = MockExtractor::new();
    mock.expect_handle_tick_scoped_data()
        .once()
        .returning(|inp: BlockScopedData| {
            assert_eq!(inp.partial_index, Some(1));
            assert_eq!(inp.is_last_partial, Some(false));
            Ok(Some(one_msg()))
        });
    let extractor: Arc<dyn Extractor> = Arc::new(mock);

    let msgs = ExtractorRunner::process_block_data(extractor.as_ref(), &data, true)
        .await
        .unwrap();
    assert_eq!(msgs.len(), 1);
}
