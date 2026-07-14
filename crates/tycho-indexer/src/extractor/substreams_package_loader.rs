use std::{path::Path, sync::Arc};

use anyhow::{format_err, Context};
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;
use prost::Message;

use crate::{
    extractor::ExtractionError, pb::sf::substreams::v1::Package, substreams::SubstreamsEndpoint,
};

async fn download_file_from_s3(
    bucket: &str,
    key: &str,
    download_path: &Path,
) -> anyhow::Result<()> {
    tracing::info!("Downloading file from s3: {}/{} to {:?}", bucket, key, download_path);

    let region_provider = RegionProviderChain::default_provider().or_else("eu-central-1");

    let config = aws_config::from_env()
        .region(region_provider)
        .load()
        .await;

    let client = Client::new(&config);

    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    let data = resp.body.collect().await.unwrap();

    if let Some(parent) = download_path.parent() {
        std::fs::create_dir_all(parent)
            .context(format!("Failed to create directories for {parent:?}"))?;
    }

    std::fs::write(download_path, data.into_bytes()).unwrap();

    Ok(())
}

async fn ensure_spkg_path(s3_bucket: Option<&str>, spkg_path: &str) -> Result<(), ExtractionError> {
    if Path::new(spkg_path).exists() {
        return Ok(());
    }

    download_file_from_s3(
        s3_bucket.ok_or_else(|| {
            ExtractionError::Setup(format!("Missing spkg and s3 bucket config for {spkg_path}"))
        })?,
        spkg_path,
        Path::new(spkg_path),
    )
    .await
    .map_err(|e| ExtractionError::Setup(format!("Failed to download {spkg_path} from s3. {e}")))?;

    Ok(())
}

async fn read_spkg(spkg_path: &str) -> Result<Package, ExtractionError> {
    let content = std::fs::read(spkg_path)
        .context(format_err!("read package from file '{spkg_path}'"))
        .map_err(|err| ExtractionError::SubstreamsError(err.to_string()))?;
    Package::decode(content.as_ref())
        .context("decode command")
        .map_err(|err| ExtractionError::SubstreamsError(err.to_string()))
}

pub struct LoadedSubstreamsPackage {
    pub spkg: Package,
    pub endpoint: Arc<SubstreamsEndpoint>,
}

pub async fn load_substreams_package(
    s3_bucket: Option<&str>,
    spkg_path: &str,
    endpoint_url: &str,
    token: Option<String>,
) -> Result<LoadedSubstreamsPackage, ExtractionError> {
    ensure_spkg_path(s3_bucket, spkg_path).await?;
    let spkg = read_spkg(spkg_path).await?;
    let endpoint = Arc::new(
        SubstreamsEndpoint::new(endpoint_url, token)
            .await
            .map_err(|err| ExtractionError::SubstreamsError(err.to_string()))?,
    );

    Ok(LoadedSubstreamsPackage { spkg, endpoint })
}
