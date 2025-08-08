use std::str::FromStr;

use clap::{Command, arg, command};
use dotenv::dotenv;
use fjall::Config;
use indexer_lib::database::{
    self,
    headers::{BlockCompactHeaderPartition, BlockGapsPartition, DaaIndexPartition},
    messages::{
        ContextualMessageBySenderPartition, HandshakeByReceiverPartition,
        HandshakeBySenderPartition, PaymentByReceiverPartition, PaymentBySenderPartition,
        TxIdToHandshakePartition, TxIdToPaymentPartition,
    },
    processing::{
        AcceptingBlockToTxIDPartition, PendingSenderResolutionPartition, SkipTxByBlockPartition,
        SkipTxPartition, TxIDToAcceptancePartition, UnknownAcceptingDaaPartition,
        UnknownTxPartition,
    },
};
use kaspa_addresses::Address;
use kaspa_consensus_core::{Hash, tx::TransactionId};
use tracing::info;
use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::get_cli_config;

mod config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ignore faillures as .env might not be present at runtime, and this use-case is tolerated
    dotenv()
        .inspect_err(|err| println!("[WARN] reading .env files is failed with err {err}"))
        .ok();

    let config = get_cli_config()?;

    // logs
    let (non_blocking_appender, _guard_stdout) = tracing_appender::non_blocking(std::io::stdout());
    let stdout_subscriber = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking_appender)
        .with_filter(config.rust_log);
    tracing_subscriber::registry()
        .with(stdout_subscriber)
        .init();

    // Database

    let db_path = config
        .kasia_indexer_db_root
        .join(config.network_type.to_string());

    let config = Config::new(db_path).max_write_buffer_size(512 * 1024 * 1024);
    let tx_keyspace = config.open_transactional()?;

    let _handshake_by_receiver_partition = HandshakeByReceiverPartition::new(&tx_keyspace)?;
    let _tx_id_to_handshake_partition = TxIdToHandshakePartition::new(&tx_keyspace)?;
    let contextual_message_partition = ContextualMessageBySenderPartition::new(&tx_keyspace)?;
    let _payment_by_receiver_partition = PaymentByReceiverPartition::new(&tx_keyspace)?;
    let _tx_id_to_payment_partition = TxIdToPaymentPartition::new(&tx_keyspace)?;
    let _tx_id_to_acceptance_partition = TxIDToAcceptancePartition::new(&tx_keyspace)?;
    let _skip_tx_partition = SkipTxPartition::new(&tx_keyspace)?;
    let _skip_tx_by_block_partition = SkipTxByBlockPartition::new(&tx_keyspace)?;
    let _block_compact_header_partition = BlockCompactHeaderPartition::new(&tx_keyspace)?;
    let _acceptance_to_tx_id_partition = AcceptingBlockToTxIDPartition::new(&tx_keyspace)?;
    let _unknown_tx_partition = UnknownTxPartition::new(&tx_keyspace)?;
    let _unknown_accepting_daa_partition = UnknownAcceptingDaaPartition::new(&tx_keyspace)?;
    let _pending_sender_resolution_partition = PendingSenderResolutionPartition::new(&tx_keyspace)?;
    let _handshake_by_sender_partition = HandshakeBySenderPartition::new(&tx_keyspace)?;
    let _payment_by_sender_partition = PaymentBySenderPartition::new(&tx_keyspace)?;
    let _block_gaps_partition = BlockGapsPartition::new(&tx_keyspace)?;
    let _block_daa_index_partition = DaaIndexPartition::new(&tx_keyspace)?;

    let matches = command!() // requires `cargo` feature
        .propagate_version(true)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("message")
                .alias("m")
                .about("Contextual Messages")
                .subcommand(
                    Command::new("by-tx-id")
                        .alias("t")
                        .about("Messages by tx id")
                        .arg(arg!([TX_ID]).required(true)),
                ),
        )
        .get_matches();

    match matches.subcommand() {
        Some(("message", sub_matches)) => {
            match sub_matches.subcommand() {
                Some(("by-tx-id", sub_matches)) => {
                    let tx_id = sub_matches
                        .get_one::<String>("TX_ID")
                        .map(|s| s.as_str())
                        .unwrap();

                    let tx_hash = Hash::from_str(&tx_id).expect("Invalid tx id");

                    let rtx = tx_keyspace.read_tx();
                    let tmp_known_tx_id: [u8; 32] = [
                        22, 101, 162, 151, 21, 58, 52, 16, 6, 36, 149, 49, 251, 147, 193, 53, 6,
                        157, 139, 180, 238, 241, 198, 22, 98, 146, 134, 235, 2, 152, 125, 15,
                    ];
                    let mut iter =
                        contextual_message_partition.get_by_tx_id(&rtx, &tx_hash.as_bytes());

                    // get the first item from the iterator
                    let item = iter.next().unwrap().unwrap();

                    info!("item: {:?}", item.0);
                }
                _ => return Ok(()),
            }
            return Ok(());
        }
        _ => return Ok(()),
    }
}
