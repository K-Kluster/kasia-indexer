use crate::headers::block_compact_headers::BlockCompactHeaderPartition;
use crate::headers::daa_index::DaaIndexPartition;
use crate::metadata::MetadataPartition;
use anyhow::{Result, anyhow};
use tracing::{error, info};
use zerocopy::{IntoBytes, TryFromBytes};

mod v0;

pub const CURRENT_VERSION: u32 = 1;

pub fn apply_migrations(
    metadata_partition: &MetadataPartition,
    keyspace: &fjall::TxKeyspace,
) -> Result<()> {
    let version = metadata_partition.db_version()?;
    info!("Database version: {}", version);
    if version < CURRENT_VERSION {
        info!("Applying migrations to database version: {}", version);
        let daa_index_partition = DaaIndexPartition::new(keyspace)?;
        let compact_header_partition = BlockCompactHeaderPartition::new(keyspace)?;
        migrate_v0_v1(
            &daa_index_partition,
            &compact_header_partition,
            metadata_partition,
        )
        .inspect_err(|err| error!("Migration failed: {}", err))?;
        info!(
            "Applying migrations finished, database version: {}",
            CURRENT_VERSION,
        );
    } else {
        info!("Database version is up to date: {}", version);
    }

    Ok(())
}

fn migrate_v0_v1(
    daa_index_partition: &DaaIndexPartition,
    compact_header_partition: &BlockCompactHeaderPartition,
    metadata_partition: &MetadataPartition,
) -> Result<()> {
    use std::sync::mpsc;
    info!("Start migrating v0 to v1");
    let (sender, receiver) = mpsc::channel();
    let daa_index_partition_reader = daa_index_partition.clone();
    // Spawn a thread to read keys and send batches
    let reader_handle = std::thread::spawn(move || -> Result<()> {
        let keys_iter = daa_index_partition_reader.0.inner().keys();
        let mut batch = Vec::with_capacity(512);

        for key_result in keys_iter {
            let key = key_result.map_err(|_| anyhow!("Daa key reading"))?;
            let old_key = v0::DaaIndexKeyV0::try_read_from_bytes(key.as_ref())
                .map_err(|_| anyhow!("Daa key v0 deserialization"))?;

            batch.push(old_key);

            if batch.len() == 512 {
                sender
                    .send(batch)
                    .map_err(|_| anyhow!("Channel send error"))?;
                batch = Vec::with_capacity(512);
            }
        }

        // Send the last partial batch
        if !batch.is_empty() {
            sender
                .send(batch)
                .map_err(|_| anyhow!("Channel send error"))?;
        }

        Ok(())
    });

    // Process batches in parallel as they arrive using Rayon's scope
    rayon::scope(|s| {
        for batch in receiver {
            s.spawn(move |_| {
                for old_key in &batch {
                    // Delete the old key
                    daa_index_partition
                        .0
                        .remove(old_key.as_bytes())
                        .expect("Delete failed");

                    // Get the new value from compact_header_partition
                    if let Some(h) = compact_header_partition
                        .get_compact_header(&old_key.block_hash)
                        .expect("Get failed")
                    {
                        let mut blue_work_le = h.blue_work;
                        let blue_work_be = {
                            convert_bluework_endianness(&mut blue_work_le);
                            blue_work_le
                        };

                        // Insert with the new key
                        daa_index_partition
                            .insert(old_key.daa_score.get(), old_key.block_hash, blue_work_be)
                            .expect("Insert failed");
                    }
                }
            });
        }
    });

    reader_handle
        .join()
        .map_err(|_| anyhow!("Reader thread panicked"))??;

    info!("Migrating v0 to v1 finished, setting db version to 1");
    metadata_partition.set_db_version(1)?;
    Ok(())
}

/// Converts BlueWork between little-endian and big-endian byte representations
#[inline]
pub fn convert_bluework_endianness(bytes: &mut [u8; 24]) {
    for i in 0..12 {
        bytes.swap(i, 23 - i);
    }
}
