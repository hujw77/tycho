use anyhow::{Ok, Result};
use substreams_ethereum::Abigen;

fn main() -> Result<(), anyhow::Error> {
    Abigen::new("Executor", "abi/Executor.json")?
        .generate()?
        .write_to_file("src/abi/executor.rs")?;
    Abigen::new("Factory", "abi/Factory.json")?
        .generate()?
        .write_to_file("src/abi/factory.rs")?;
    Abigen::new("Pool", "abi/Pool.json")?
        .generate()?
        .write_to_file("src/abi/pool.rs")?;
    Abigen::new("TickSnapshotLens", "abi/TickSnapshotLens.json")?
        .generate()?
        .write_to_file("src/abi/tick_snapshot_lens.rs")?;
    Ok(())
}
