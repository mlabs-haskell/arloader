//! SDK for uploading files in bulk to [Arweave](https://www.arweave.org/).
//!
//! ## CLI
//! See [README.md](https://crates.io/crates/arloader) for usage instructions.
//!
//! The main cli application is all in `main` and follows a pattern of specifying arguments,
//! matching them, and then in turn passing them to commands, all of which are included
//! in the [`commands`] module in order to facilitate their use as library functions and
//! re-use in other command line applications.
//!
//! ## Library
//!
//! #### Overview
//! The library is focused on uploading files as efficiently as possible. Arweave has
//! two different transaction formats and two different upload formats. Transactions can either
//! be normal, single data item transactions (see [transaction format](https://docs.arweave.org/developers/server/http-api#transaction-format)
//!  for details), or bundle transactions (see [bundle format](https://github.com/joshbenaron/arweave-standards/blob/ans104/ans/ANS-104.md)
//! for details). The bundle format, introduced mid-2021, bundles together individual data items
//! into larger transactions, making uploading much more efficient and reducing network congestion.
//! The library supports both formats, with the recommended approach being to use the bundle format.
//!
//! There are also two upload formats, whole transactions, which if they are less than 12 MB can be
//! uploaded to the `tx/` endpoint, and chunked transactions, which get uploaded in 256 KB chunks
//! to the  `chunk/`endpoint. Arloader includes functionality for both formats.
//!
//! #### Transactions and DataItems
//! Both transaction formats start with chunking file data and creating merkle trees from the chunks.
//! The merkle tree logic can be found in the [`merkle`] module. All of the hashing functions and other crypto
//! operations are in the [`crypto`] module. Once the data is chunked, hashed, and a merkle root
//! calculated for it, it gets incorporated into either a [`Transaction`], which can be found in the
//! [`transaction`] module, or if it is going to be included in a bundle format transaction, a [`DataItem`],
//! which can be found in the [`bundle`] module.
//!
//! #### Tags
//! [`Tag`]s are structs with `name` and `value` properties that can be included with either [`Transaction`]s or
//! [`DataItem`]s. One subtlety is that for [`Transaction`]s, Arweave expects the content at each key to be a base64 url
//! encoded string, whereas for [`DataItem`]s, Arweave expects utf8-encoded strings. [`Tag`]s have been implemented for
//! for both types as [`Tag<Base64>`] and [`Tag<String>`]. Another subtlety is that [`Tag`]s for [`DataItem`]s are serialized
//! and deserialized using [`avro_rs`], the schema of which is implemented in [`bundle::get_tags_schema`].
//!
//! A [`Tag`] with a name property of `Content-Type` is used by the Arweave gateways to communicate the mime type of
//! the related content to browsers. Arloader creates a content type tag based on file extension if one is provided
//! or from the bytes of the data using [magic numbers](https://en.wikipedia.org/wiki/File_format#Magic_number) if not.
//!
//! #### Bytes and Base64Url Data
//! The library stores all data, signatures and addresses as a [`Base64`] struct with methods implemented for
//! serializing and deserializing the underlying bytes to and from the base64 url format required for uploading
//! to Arweave.
//!
//! #### Signing
//! A key part of constructing transactions is signing them. Arweave has a specific algorithm for generating the
//! digest that gets signed and then hashed to serve as a transaction id, called [deepHash](https://github.com/ArweaveTeam/arweave-js/blob/92d690a52ed0b647f36d58a6eba28542ef609b18/src/common/lib/deepHash.ts#L8).
//! It takes various [`Transaction`] or [`DataItem`] elements, including nested arrays of [`Tag`]s, and successively
//! hashes and concatenates them together. Arloader assembles the required elements via the [`ToItems`] trait, which
//! is implemented separately as [`Transaction::to_deep_hash_item`] and [`DataItem::to_deep_hash_item`] for each transaction
//! format. [`crypto::Provider::deep_hash`] is Arloader's implementation of the deep hash algorithm.
//!
//! #### Higher Level Functions
//! The functions for creating [`Transaction`]s and bundles of [`DataItem`]s are all consolidated on the [`Arweave`] struct.
//! In general, there are lower level functions for creating single items from data that are then composed in successively
//! higher level functions to allow multiple items to be created from collections of file paths and ultimately upload streams
//! of transactions to Arweave.
//!
//! #### Status Tracking
//! The library includes additional functionality to track and report on transaction statuses. There are two status structs,
//! [`Status`] and [`BundleStatus`] used for these purposes. They are essentially the same format, except that
//! [`BundleStatus`] is modified to include references to all of the included [`DataItem`]s instead of just a
//! single [`Transaction`] for [`Status`].
//!
//! #### Solana
//! The functions for allowing payment to be made in SOL can be found in the [`solana`] module.

use blake3;
use chrono::Utc;
use futures::{
    future::{try_join, try_join_all},
    stream, Stream, StreamExt,
};
use glob::glob;
use infer;
use log::debug;
use num_bigint::BigUint;
use rayon::prelude::*;
use reqwest::{
    self,
    header::{ACCEPT, CONTENT_TYPE},
    Client, StatusCode as ResponseStatusCode,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use solana_sdk::signer::keypair::Keypair;
use std::{
    collections::HashMap,
    fmt::Write,
    path::{Path, PathBuf},
    str::FromStr,
};
use tokio::{
    fs,
    time::{sleep, Duration},
};
use url::Url;

pub mod bundle;
pub mod commands;
pub mod crypto;
pub mod error;
pub mod merkle;
pub mod raw;
pub mod solana;
pub mod status;
pub mod transaction;
pub mod utils;

use bundle::DataItem;
use error::Error;
use merkle::{generate_data_root, generate_leaves, resolve_proofs};
use solana::{create_sol_transaction, get_sol_ar_signature, SigResponse, FLOOR, RATE};
use status::{BundleStatus, Filterable, Status, StatusCode};
use transaction::{Base64, Chunk, FromUtf8Strs, Tag, ToItems, Transaction};

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

/// Winstons are a sub unit of the native Arweave network token, AR. There are 10<sup>12</sup> Winstons per AR.
pub const WINSTONS_PER_AR: u64 = 1_000_000_000_000;

/// Block size used for pricing calculations = 256 KB
pub const BLOCK_SIZE: u64 = 1024 * 256;

/// Maximum data size to send to `tx/` endpoint. Sent to `chunk/` endpoint above this.
pub const MAX_TX_DATA: u64 = 10_000_000;

/// Multiplier applied to the buffer argument from the cli to determine the maximum number
/// of simultaneous request to the `chunk/ endpoint`.
pub const CHUNKS_BUFFER_FACTOR: usize = 20;

/// Number of times to retry posting chunks if not successful.
pub const CHUNKS_RETRIES: u16 = 10;

/// Number of seconds to wait between retying to post a failed chunk.
pub const CHUNKS_RETRY_SLEEP: u64 = 1;

//=========================
// Streams
//=========================

/// Uploads a stream of bundles from [`Vec<PathsChunk>`]s.
///
/// Buffers bundles if using the tx/ endpoint, else buffers chunks with bundles processed serially.
pub fn upload_bundles_stream<'a>(
    arweave: &'a Arweave,
    paths_chunks: Vec<PathsChunk>,
    tags: Vec<Tag<String>>,
    price_terms: (u64, u64),
    buffer: usize,
) -> impl Stream<Item = Result<BundleStatus, Error>> + 'a {
    let bundle_size = paths_chunks[0].1;
    let (bundles_buffer, chunks_buffer) = if bundle_size > MAX_TX_DATA {
        (1, buffer * CHUNKS_BUFFER_FACTOR)
    } else {
        (buffer, 1)
    };

    stream::iter(paths_chunks)
        .map(move |p| {
            arweave.post_bundle_transaction_from_file_paths(
                p,
                tags.clone(),
                price_terms,
                chunks_buffer,
            )
        })
        .buffer_unordered(bundles_buffer)
}

/// Queries network and updates locally stored [`BundleStatus`] structs.
pub fn update_bundle_statuses_stream<'a, IP>(
    arweave: &'a Arweave,
    paths_iter: IP,
    buffer: usize,
) -> impl Stream<Item = Result<BundleStatus, Error>> + 'a
where
    IP: Iterator<Item = PathBuf> + Send + Sync + 'a,
{
    stream::iter(paths_iter)
        .map(move |p| arweave.update_bundle_status(p))
        .buffer_unordered(buffer)
}

/// Uploads a stream of bundles from [`Vec<PathsChunk>`]s, paying with SOL.
pub fn upload_bundles_stream_with_sol<'a>(
    arweave: &'a Arweave,
    paths_chunks: Vec<PathsChunk>,
    tags: Vec<Tag<String>>,
    price_terms: (u64, u64),
    buffer: usize,
    solana_url: Url,
    sol_ar_url: Url,
    from_keypair: &'a Keypair,
) -> impl Stream<Item = Result<BundleStatus, Error>> + 'a {
    let bundle_size = paths_chunks[0].1;
    let (bundles_buffer, chunks_buffer) = if bundle_size > MAX_TX_DATA {
        (1, buffer * CHUNKS_BUFFER_FACTOR)
    } else {
        (buffer, 1)
    };

    stream::iter(paths_chunks)
        .map(move |p| {
            arweave.post_bundle_transaction_from_file_paths_with_sol(
                p,
                tags.clone(),
                price_terms,
                chunks_buffer,
                solana_url.clone(),
                sol_ar_url.clone(),
                from_keypair,
            )
        })
        .buffer_unordered(bundles_buffer)
}

/// Uploads a stream of chunks from [`Vec<Chunk>`]s.
pub fn upload_transaction_chunks_stream<'a>(
    arweave: &'a Arweave,
    signed_transaction: Transaction,
    buffer: usize,
) -> impl Stream<Item = Result<usize, Error>> + 'a {
    let client = Client::new();
    stream::iter(0..signed_transaction.chunks.len())
        .map(move |i| {
            let chunk = signed_transaction.get_chunk(i).unwrap();
            arweave.post_chunk_with_retries(chunk, client.clone())
        })
        .buffer_unordered(buffer)
}

/// Uploads files matching glob pattern, returning a stream of [`Status`] structs.
pub fn upload_files_stream<'a, IP>(
    arweave: &'a Arweave,
    paths_iter: IP,
    tags: Option<Vec<Tag<Base64>>>,
    log_dir: Option<PathBuf>,
    last_tx: Option<Base64>,
    price_terms: (u64, u64),
    buffer: usize,
) -> impl Stream<Item = Result<Status, Error>> + 'a
where
    IP: Iterator<Item = PathBuf> + Send + Sync + 'a,
{
    stream::iter(paths_iter)
        .map(move |p| {
            arweave.upload_file_from_path(
                p,
                log_dir.clone(),
                tags.clone(),
                last_tx.clone(),
                price_terms,
            )
        })
        .buffer_unordered(buffer)
}

/// Uploads files matching glob pattern, returning a stream of [`Status`] structs, paying with SOL.
pub fn upload_files_with_sol_stream<'a, IP>(
    arweave: &'a Arweave,
    paths_iter: IP,
    tags: Option<Vec<Tag<Base64>>>,
    log_dir: Option<PathBuf>,
    last_tx: Option<Base64>,
    price_terms: (u64, u64),
    solana_url: Url,
    sol_ar_url: Url,
    from_keypair: &'a Keypair,
    buffer: usize,
) -> impl Stream<Item = Result<Status, Error>> + 'a
where
    IP: Iterator<Item = PathBuf> + Send + Sync + 'a,
{
    stream::iter(paths_iter)
        .map(move |p| {
            arweave.upload_file_from_path_with_sol(
                p,
                log_dir.clone(),
                tags.clone(),
                last_tx.clone(),
                price_terms,
                solana_url.clone(),
                sol_ar_url.clone(),
                from_keypair,
            )
        })
        .buffer_unordered(buffer)
}

/// Queries network and updates locally stored [`Status`] structs.
pub fn update_statuses_stream<'a, IP>(
    arweave: &'a Arweave,
    paths_iter: IP,
    log_dir: PathBuf,
    buffer: usize,
) -> impl Stream<Item = Result<Status, Error>> + 'a
where
    IP: Iterator<Item = PathBuf> + Send + Sync + 'a,
{
    stream::iter(paths_iter)
        .map(move |p| arweave.update_status(p, log_dir.clone()))
        .buffer_unordered(buffer)
}

//=========================
// Helpers
//=========================

#[derive(Serialize, Deserialize, Debug)]
pub struct OraclePrice {
    pub arweave: OraclePricePair,
    pub solana: OraclePricePair,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OraclePricePair {
    pub usd: f32,
}

/// Tuple struct includes two elements: chunk of paths and aggregatge data size of paths.
#[derive(Clone, Debug)]
pub struct PathsChunk(Vec<PathBuf>, u64);

/// Used in updating [`BundleStatus`]s to determine whether a file stem includes a valid transaction id.
pub fn file_stem_is_valid_txid(file_path: &PathBuf) -> bool {
    match Base64::from_str(file_path.file_stem().unwrap().to_str().unwrap()) {
        Ok(txid) => match txid.0.len() {
            32 => true,
            _ => false,
        },
        Err(_) => false,
    }
}

//=========================
// Arweave
//=========================

/// Struct with methods for interacting with the Arweave network.
pub struct Arweave {
    pub name: String,
    pub units: String,
    pub base_url: Url,
    pub crypto: crypto::Provider,
}

impl Default for Arweave {
    fn default() -> Self {
        Self {
            name: String::from("arweave"),
            units: String::from("winstons"),
            base_url: Url::from_str("https://arweave.net/").unwrap(),
            crypto: crypto::Provider::default(),
        }
    }
}

impl Arweave {
    pub async fn from_keypair_path(keypair_path: PathBuf, base_url: Url) -> Result<Arweave, Error> {
        let crypto = crypto::Provider::from_keypair_path(keypair_path).await?;
        let arweave = Arweave {
            base_url,
            crypto,
            ..Default::default()
        };

        Ok(arweave)
    }

    pub fn from_keypair_path_sync(keypair_path: PathBuf, base_url: Url) -> Result<Arweave, Error> {
        let crypto = crypto::Provider::from_keypair_path_sync(keypair_path)?;
        let arweave = Arweave {
            base_url,
            crypto,
            ..Default::default()
        };

        Ok(arweave)
    }

    //-------------------------
    // Get Request
    //-------------------------

    /// Get pending network transaction count.
    pub async fn get_pending_count(&self) -> Result<usize, Error> {
        let url = self.base_url.join("tx/pending")?;
        let tx_ids: Vec<String> = reqwest::get(url).await?.json().await?;
        Ok(tx_ids.len())
    }

    /// Returns price of uploading data to the network in winstons and USD per AR and USD per SOL
    /// as a BigUint with two decimals.
    pub async fn get_price(&self, bytes: &u64) -> Result<(BigUint, BigUint, BigUint), Error> {
        let url = self.base_url.join("price/")?.join(&bytes.to_string())?;
        let winstons_per_bytes = reqwest::get(url)
            .await
            .map_err(|e| Error::ArweaveGetPriceError(e))?
            .json::<u64>()
            .await?;
        let winstons_per_bytes = BigUint::from(winstons_per_bytes);

        let oracle_url =
            "https://api.coingecko.com/api/v3/simple/price?ids=arweave,solana&vs_currencies=usd";
        let prices = reqwest::get(oracle_url)
            .await
            .map_err(|e| Error::OracleGetPriceError(e))?
            .json::<OraclePrice>()
            .await?;

        let usd_per_ar: BigUint = BigUint::from((prices.arweave.usd * 100.0).floor() as u32);
        let usd_per_sol: BigUint = BigUint::from((prices.solana.usd * 100.0).floor() as u32);

        Ok((winstons_per_bytes, usd_per_ar, usd_per_sol))
    }

    /// Gets base and incremental prices for a 256 KB block of data.
    pub async fn get_price_terms(&self, reward_mult: f32) -> Result<(u64, u64), Error> {
        let (prices1, prices2) = try_join(
            self.get_price(&(256 * 1024)),
            self.get_price(&(256 * 1024 * 2)),
        )
        .await?;
        let base = (prices1.0.to_u64_digits()[0] as f32 * reward_mult) as u64;
        let incremental = (prices2.0.to_u64_digits()[0] as f32 * reward_mult) as u64 - &base;
        Ok((base, incremental))
    }

    /// Gets transaction from the network.
    pub async fn get_transaction(&self, id: &Base64) -> Result<Transaction, Error> {
        let url = self.base_url.join("tx/")?.join(&id.to_string())?;
        let resp = reqwest::get(url).await?.json::<Transaction>().await?;
        Ok(resp)
    }

    /// Returns the balance of the wallet.
    pub async fn get_wallet_balance(
        &self,
        wallet_address: Option<String>,
    ) -> Result<BigUint, Error> {
        let wallet_address = if let Some(wallet_address) = wallet_address {
            wallet_address
        } else {
            self.crypto.wallet_address()?.to_string()
        };
        let url = self
            .base_url
            .join(&format!("wallet/{}/balance", &wallet_address))?;
        let winstons = reqwest::get(url).await?.json::<u64>().await?;
        Ok(BigUint::from(winstons))
    }

    //-------------------------
    // Bundle
    //-------------------------

    pub fn chunk_file_paths<IP>(
        &self,
        paths_iter: IP,
        data_size: u64,
    ) -> Result<Vec<PathsChunk>, Error>
    where
        IP: Iterator<Item = PathBuf> + Send,
    {
        let (mut paths_chunks, last_chunk, last_data_len) = paths_iter.fold(
            (Vec::<PathsChunk>::new(), Vec::<PathBuf>::new(), 0u64),
            |(mut ip, mut i, data_len), p| {
                let p_len = p.metadata().unwrap().len();
                if data_len + p_len > data_size {
                    ip.push(PathsChunk(i, data_len));
                    (ip, vec![p], p_len)
                } else {
                    i.push(p);
                    (ip, i, data_len + p_len)
                }
            },
        );

        if last_chunk.len() > 0 {
            paths_chunks.push(PathsChunk(last_chunk, last_data_len));
        }

        Ok(paths_chunks)
    }

    pub fn create_bundle_from_data_items(
        &self,
        data_items: Vec<(DataItem, Status)>,
    ) -> Result<(Vec<u8>, Value), Error> {
        let data_items_len = (data_items.len()) as u64;
        let ((headers, binaries), statuses): ((Vec<Vec<u8>>, Vec<Vec<u8>>), Vec<Status>) =
            data_items
                .into_iter()
                .map(|(d, s)| (d.to_bundle_item().unwrap(), s))
                .unzip();

        let manifest = self.create_manifest(statuses)?;

        let binary: Vec<_> = data_items_len
            .to_le_bytes()
            .into_par_iter()
            .chain([0u8; 24].into_par_iter())
            .chain(headers.into_par_iter().flatten())
            .chain(binaries.into_par_iter().flatten())
            .collect();

        Ok((binary, manifest))
    }

    pub async fn create_bundle_transaction_from_file_paths(
        &self,
        paths_iter: Vec<PathBuf>,
        tags: Vec<Tag<String>>,
        price_terms: (u64, u64),
    ) -> Result<(Transaction, Value), Error> {
        let data_items = self
            .create_data_items_from_file_paths(paths_iter, tags)
            .await?;

        let (bundle, manifest_object) = self.create_bundle_from_data_items(data_items)?;
        let other_tags = Some(vec![
            Tag::<Base64>::from_utf8_strs("Bundle-Format", "binary")?,
            Tag::<Base64>::from_utf8_strs("Bundle-Version", "2.0.0")?,
        ]);

        let transaction = self
            .create_transaction(bundle, other_tags, None, price_terms, true)
            .await?;

        Ok((transaction, manifest_object))
    }

    // Create [`data_item::DataItem`] for bundle.
    pub fn create_data_item(
        &self,
        data: Vec<u8>,
        mut tags: Vec<Tag<String>>,
        auto_content_tag: bool,
    ) -> Result<DataItem, Error> {
        tags.push(Tag::<String>::from_utf8_strs(
            "User-Agent",
            &format!("arloader/{}", VERSION),
        )?);

        // Get content type from [magic numbers](https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types)
        // and include additional tags if any.
        if auto_content_tag {
            let content_type = if let Some(kind) = infer::get(&data) {
                kind.mime_type()
            } else {
                "application/octet-stream"
            };

            tags.push(Tag::<String>::from_utf8_strs("Content-Type", content_type)?)
        }

        // let mut anchor = Base64(Vec::with_capacity(32));
        // self.crypto.fill_rand(&mut anchor.0)?;

        Ok(DataItem {
            data: Base64(data),
            tags,
            // anchor,
            ..DataItem::default()
        })
    }

    pub async fn create_data_item_from_file_path(
        &self,
        file_path: PathBuf,
        mut tags: Vec<Tag<String>>,
    ) -> Result<(DataItem, Status), Error> {
        let mut auto_content_tag = true;
        let mut status_content_type = mime_guess::mime::OCTET_STREAM.to_string();

        if let Some(content_type) = mime_guess::from_path(file_path.clone()).first() {
            status_content_type = content_type.to_string();
            auto_content_tag = false;
            let content_tag: Tag<String> =
                Tag::from_utf8_strs("Content-Type", &content_type.to_string())?;
            tags.push(content_tag);
        }

        let data = fs::read(&file_path).await?;
        let data_item = self.create_data_item(data, tags, auto_content_tag)?;
        let data_item = self.sign_data_item(data_item)?;

        let status = Status {
            id: data_item.id.clone(),
            file_path: Some(file_path),
            content_type: status_content_type,
            ..Status::default()
        };

        Ok((data_item, status))
    }

    pub async fn create_data_items_from_file_paths(
        &self,
        paths: Vec<PathBuf>,
        tags: Vec<Tag<String>>,
    ) -> Result<Vec<(DataItem, Status)>, Error> {
        try_join_all(
            paths
                .into_iter()
                .map(|p| self.create_data_item_from_file_path(p, tags.clone())),
        )
        .await
    }

    // Tested here instead of data_item to verify signature as well - crytpo on data_item.
    pub fn deserialize_bundle(&self, bundle: Vec<u8>) -> Result<Vec<DataItem>, Error> {
        let mut bundle_iter = bundle.into_iter();
        let result = [(); 8].map(|_| bundle_iter.next().unwrap());
        let number_of_data_items = u64::from_le_bytes(result) as usize;
        (0..24).for_each(|_| {
            bundle_iter.next().unwrap();
        });

        // Parse headers.
        let mut bytes_lens = Vec::<u64>::with_capacity(number_of_data_items);
        let mut ids = vec![Vec::<u8>::with_capacity(32); number_of_data_items];
        (0..number_of_data_items).for_each(|i| {
            let result = [(); 8].map(|_| bundle_iter.next().unwrap());
            bytes_lens.push(u64::from_le_bytes(result));
            (0..24).for_each(|_| {
                bundle_iter.next().unwrap();
            });
            (0..32).for_each(|_| {
                ids[i].push(bundle_iter.next().unwrap());
            });
        });

        // Parse data_items - data_item verified during deserialization - signatures verified
        // TODO: verify signature against data_item id.
        let mut bytes_lens_iter = bytes_lens.into_iter();
        let mut ids_iter = ids.into_iter();
        let data_items: Result<Vec<DataItem>, _> = (0..number_of_data_items)
            .map(|_| {
                let bytes_len = bytes_lens_iter.next().unwrap() as usize;
                let mut bytes_vec = Vec::<u8>::with_capacity(bytes_len);
                (0..bytes_len).for_each(|_| bytes_vec.push(bundle_iter.next().unwrap()));
                let mut data_item = DataItem::deserialize(bytes_vec)?;

                let deep_hash = self
                    .crypto
                    .deep_hash(data_item.to_deep_hash_item()?)
                    .unwrap();
                self.crypto
                    .verify(&data_item.signature.0, &deep_hash)
                    .unwrap();

                data_item.id.0 = ids_iter.next().unwrap();

                Ok(data_item)
            })
            .collect();

        data_items
    }

    pub async fn post_bundle_transaction_from_file_paths(
        &self,
        paths_chunk: PathsChunk,
        tags: Vec<Tag<String>>,
        price_terms: (u64, u64),
        buffer: usize,
    ) -> Result<BundleStatus, Error> {
        let number_of_files = paths_chunk.0.len() as u64;
        let data_items = self
            .create_data_items_from_file_paths(paths_chunk.0, tags)
            .await?;

        let (bundle, manifest) = self.create_bundle_from_data_items(data_items)?;
        let other_tags = Some(vec![
            Tag::<Base64>::from_utf8_strs("Bundle-Format", "binary")?,
            Tag::<Base64>::from_utf8_strs("Bundle-Version", "2.0.0")?,
        ]);

        let transaction = self
            .create_transaction(bundle, other_tags, None, price_terms, true)
            .await?;

        let signed_transaction = self.sign_transaction(transaction)?;

        let (id, reward) = if paths_chunk.1 > MAX_TX_DATA {
            self.post_transaction_chunks(signed_transaction, buffer)
                .await?
        } else {
            self.post_transaction(&signed_transaction).await?
        };

        let status = BundleStatus {
            id,
            reward,
            number_of_files,
            data_size: paths_chunk.1,
            file_paths: manifest["paths"].clone(),
            ..Default::default()
        };

        Ok(status)
    }

    pub async fn post_bundle_transaction_from_file_paths_with_sol(
        &self,
        paths_chunk: PathsChunk,
        tags: Vec<Tag<String>>,
        price_terms: (u64, u64),
        chunks_buffer: usize,
        solana_url: Url,
        sol_ar_url: Url,
        from_keypair: &Keypair,
    ) -> Result<BundleStatus, Error> {
        let number_of_files = paths_chunk.0.len() as u64;
        let data_items = self
            .create_data_items_from_file_paths(paths_chunk.0, tags)
            .await?;

        let (bundle, manifest) = self.create_bundle_from_data_items(data_items)?;
        let other_tags = Some(vec![
            Tag::<Base64>::from_utf8_strs("Bundle-Format", "binary")?,
            Tag::<Base64>::from_utf8_strs("Bundle-Version", "2.0.0")?,
        ]);

        let transaction = self
            .create_transaction(bundle, other_tags, None, price_terms, true)
            .await?;

        let (signed_transaction, sig_response): (Transaction, SigResponse) = self
            .sign_transaction_with_sol(transaction, solana_url, sol_ar_url, from_keypair)
            .await?;

        let (id, reward) = if paths_chunk.1 > MAX_TX_DATA {
            self.post_transaction_chunks(signed_transaction, chunks_buffer)
                .await?
        } else {
            self.post_transaction(&signed_transaction).await?
        };

        let status = BundleStatus {
            id,
            reward,
            number_of_files,
            data_size: paths_chunk.1,
            file_paths: manifest["paths"].clone(),
            sol_sig: Some(sig_response),
            ..Default::default()
        };

        Ok(status)
    }

    pub fn sign_data_item(&self, mut data_item: DataItem) -> Result<DataItem, Error> {
        data_item.owner = self.crypto.keypair_modulus()?;
        let deep_hash_item = data_item.to_deep_hash_item()?;
        let deep_hash = self.crypto.deep_hash(deep_hash_item)?;
        let signature = self.crypto.sign(&deep_hash)?;
        let id = self.crypto.hash_sha256(&signature)?;

        data_item.signature = Base64(signature);
        data_item.id = Base64(id.to_vec());
        Ok(data_item)
    }

    //-------------------------
    // Transaction
    //-------------------------

    pub async fn create_transaction(
        &self,
        data: Vec<u8>,
        other_tags: Option<Vec<Tag<Base64>>>,
        last_tx: Option<Base64>,
        price_terms: (u64, u64),
        auto_content_tag: bool,
    ) -> Result<Transaction, Error> {
        let mut transaction = self.merklize(data)?;
        transaction.owner = self.crypto.keypair_modulus()?;

        let mut tags = vec![Tag::<Base64>::from_utf8_strs(
            "User-Agent",
            &format!("arloader/{}", VERSION),
        )?];

        // Get content type from [magic numbers](https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types)
        // and include additional tags if any.
        if auto_content_tag {
            let content_type = if let Some(kind) = infer::get(&transaction.data.0) {
                kind.mime_type()
            } else {
                "application/octet-stream"
            };

            tags.push(Tag::<Base64>::from_utf8_strs("Content-Type", content_type)?)
        }

        // Add other tags if provided.
        if let Some(other_tags) = other_tags {
            tags.extend(other_tags);
        }
        transaction.tags = tags;

        // Fetch and set last_tx if not provided (primarily for testing).
        let last_tx = if let Some(last_tx) = last_tx {
            last_tx
        } else {
            let resp = reqwest::get(self.base_url.join("tx_anchor")?).await?;
            debug!("last_tx: {}", resp.status());
            let last_tx_str = resp.text().await?;
            Base64::from_str(&last_tx_str)?
        };
        transaction.last_tx = last_tx;

        let blocks_len =
            transaction.data_size / BLOCK_SIZE + (transaction.data_size % BLOCK_SIZE != 0) as u64;
        let reward = price_terms.0 + price_terms.1 * (blocks_len - 1);
        transaction.reward = reward;

        Ok(transaction)
    }

    pub async fn create_transaction_from_file_path(
        &self,
        file_path: PathBuf,
        other_tags: Option<Vec<Tag<Base64>>>,
        last_tx: Option<Base64>,
        price_terms: (u64, u64),
        auto_content_tag: bool,
    ) -> Result<Transaction, Error> {
        let data = fs::read(file_path).await?;
        self.create_transaction(data, other_tags, last_tx, price_terms, auto_content_tag)
            .await
    }

    pub fn merklize(&self, data: Vec<u8>) -> Result<Transaction, Error> {
        let mut chunks = generate_leaves(data.clone(), &self.crypto)?;
        let root = generate_data_root(chunks.clone(), &self.crypto)?;
        let data_root = Base64(root.id.clone().into_iter().collect());
        let mut proofs = resolve_proofs(root, None)?;

        // Discard the last chunk & proof if it's zero length.
        let last_chunk = chunks.last().unwrap();
        if last_chunk.max_byte_range == last_chunk.min_byte_range {
            chunks.pop();
            proofs.pop();
        }

        Ok(Transaction {
            format: 2,
            data_size: data.len() as u64,
            data: Base64(data),
            data_root,
            chunks,
            proofs,
            ..Default::default()
        })
    }

    pub async fn post_chunk(&self, chunk: &Chunk, client: &Client) -> Result<usize, Error> {
        let url = self.base_url.join("chunk")?;
        // let client = reqwest::Client::new();

        let resp = client
            .post(url)
            .json(&chunk)
            .header(&ACCEPT, "application/json")
            .header(&CONTENT_TYPE, "application/json")
            .send()
            .await
            .map_err(|e| Error::ArweavePostError(e))?;

        match resp.status() {
            reqwest::StatusCode::OK => Ok(chunk.offset),
            _ => Err(Error::StatusCodeNotOk),
        }
    }

    pub async fn post_chunk_with_retries(
        &self,
        chunk: Chunk,
        client: Client,
    ) -> Result<usize, Error> {
        let mut retries = 0;
        let mut resp = self.post_chunk(&chunk, &client).await;

        while retries < CHUNKS_RETRIES {
            match resp {
                Ok(offset) => return Ok(offset),
                Err(e) => {
                    log::debug!("post_chunk_with_retries: {:?}", e);
                    sleep(Duration::from_secs(CHUNKS_RETRY_SLEEP)).await;
                    retries += 1;
                    resp = self.post_chunk(&chunk, &client).await;
                }
            }
        }
        resp
    }

    pub async fn post_transaction(
        &self,
        signed_transaction: &Transaction,
    ) -> Result<(Base64, u64), Error> {
        if signed_transaction.id.0.is_empty() {
            return Err(error::Error::UnsignedTransaction.into());
        }

        let mut retries = 0;
        let mut status = reqwest::StatusCode::NOT_FOUND;
        let url = self.base_url.join("tx")?;
        let client = reqwest::Client::new();

        while (retries < CHUNKS_RETRIES) & (status != reqwest::StatusCode::OK) {
            status = client
                .post(url.clone())
                .json(&signed_transaction)
                .header(&ACCEPT, "application/json")
                .header(&CONTENT_TYPE, "application/json")
                .send()
                .await?
                .status();
            if status == reqwest::StatusCode::OK {
                return Ok((signed_transaction.id.clone(), signed_transaction.reward));
            }
            log::debug!("post_transaction: {:?}", status);
            sleep(Duration::from_secs(CHUNKS_RETRY_SLEEP)).await;
            retries += 1;
        }

        Err(Error::StatusCodeNotOk)
    }

    pub async fn post_transaction_chunks(
        &self,
        signed_transaction: Transaction,
        chunks_buffer: usize,
    ) -> Result<(Base64, u64), Error> {
        if signed_transaction.id.0.is_empty() {
            return Err(error::Error::UnsignedTransaction.into());
        }

        let transaction_with_no_data = signed_transaction.clone_with_no_data()?;
        let (id, reward) = self.post_transaction(&transaction_with_no_data).await?;

        let results: Vec<Result<usize, Error>> =
            upload_transaction_chunks_stream(&self, signed_transaction, chunks_buffer)
                .collect()
                .await;

        results.into_iter().collect::<Result<Vec<usize>, Error>>()?;

        Ok((id, reward))
    }

    /// Gets deep hash, signs and sets signature and id.
    pub fn sign_transaction(&self, mut transaction: Transaction) -> Result<Transaction, Error> {
        let deep_hash_item = transaction.to_deep_hash_item()?;
        let deep_hash = self.crypto.deep_hash(deep_hash_item)?;
        let signature = self.crypto.sign(&deep_hash)?;
        let id = self.crypto.hash_sha256(&signature)?;
        transaction.signature = Base64(signature);
        transaction.id = Base64(id.to_vec());
        Ok(transaction)
    }

    /// Signs transaction with sol_ar service.
    pub async fn sign_transaction_with_sol(
        &self,
        mut transaction: Transaction,
        solana_url: Url,
        sol_ar_url: Url,
        from_keypair: &Keypair,
    ) -> Result<(Transaction, SigResponse), Error> {
        let lamports = std::cmp::max(&transaction.reward / RATE, FLOOR);

        let mut sol_tx = create_sol_transaction(solana_url.clone(), from_keypair, lamports).await?;
        let mut resp = get_sol_ar_signature(
            sol_ar_url.clone(),
            transaction.to_deep_hash_item()?,
            sol_tx.clone(),
        )
        .await;

        let mut retries = 0;
        while retries < CHUNKS_RETRIES {
            match resp {
                Ok(_) => {
                    retries = CHUNKS_RETRIES;
                }
                Err(_) => {
                    println!(
                        "Retrying Solana transaction ({} of {})...",
                        retries + 1,
                        CHUNKS_RETRIES
                    );
                    retries += 1;
                    sleep(Duration::from_millis(300)).await;
                    sol_tx =
                        create_sol_transaction(solana_url.clone(), from_keypair, lamports).await?;
                    resp = get_sol_ar_signature(
                        sol_ar_url.clone(),
                        transaction.to_deep_hash_item()?,
                        sol_tx.clone(),
                    )
                    .await;
                }
            }
        }
        if let Ok(sig_response) = resp {
            let sig_response_copy = sig_response.clone();
            transaction.signature = sig_response.ar_tx_sig;
            transaction.id = sig_response.ar_tx_id;
            transaction.owner = sig_response.ar_tx_owner;
            Ok((transaction, sig_response_copy))
        } else {
            println!(
                "There was a problem with the Solana network. Please try again later or use AR."
            );
            Err(Error::SolanaNetworkError)
        }
    }

    pub async fn upload_file_from_path(
        &self,
        file_path: PathBuf,
        log_dir: Option<PathBuf>,
        mut additional_tags: Option<Vec<Tag<Base64>>>,
        last_tx: Option<Base64>,
        price_terms: (u64, u64),
    ) -> Result<Status, Error> {
        let mut auto_content_tag = true;
        let mut status_content_type = mime_guess::mime::OCTET_STREAM.to_string();

        if let Some(content_type) = mime_guess::from_path(file_path.clone()).first() {
            status_content_type = content_type.to_string();
            auto_content_tag = false;
            let content_tag: Tag<Base64> =
                Tag::from_utf8_strs("Content-Type", &content_type.to_string())?;
            if let Some(mut tags) = additional_tags {
                tags.push(content_tag);
                additional_tags = Some(tags);
            } else {
                additional_tags = Some(vec![content_tag]);
            }
        }

        let transaction = self
            .create_transaction_from_file_path(
                file_path.clone(),
                additional_tags,
                last_tx,
                price_terms,
                auto_content_tag,
            )
            .await?;
        let signed_transaction = self.sign_transaction(transaction)?;
        let (id, reward) = if signed_transaction.data.0.len() > MAX_TX_DATA as usize {
            self.post_transaction_chunks(signed_transaction, 100)
                .await?
        } else {
            self.post_transaction(&signed_transaction).await?
        };

        let status = Status {
            id,
            reward,
            file_path: Some(file_path),
            content_type: status_content_type,
            ..Default::default()
        };

        if let Some(log_dir) = log_dir {
            self.write_status(status.clone(), log_dir, None).await?;
        }
        Ok(status)
    }

    pub async fn upload_file_from_path_with_sol(
        &self,
        file_path: PathBuf,
        log_dir: Option<PathBuf>,
        mut additional_tags: Option<Vec<Tag<Base64>>>,
        last_tx: Option<Base64>,
        price_terms: (u64, u64),
        solana_url: Url,
        sol_ar_url: Url,
        from_keypair: &Keypair,
    ) -> Result<Status, Error> {
        let mut auto_content_tag = true;
        let mut status_content_type = mime_guess::mime::OCTET_STREAM.to_string();

        if let Some(content_type) = mime_guess::from_path(file_path.clone()).first() {
            status_content_type = content_type.to_string();
            auto_content_tag = false;
            let content_tag: Tag<Base64> =
                Tag::from_utf8_strs("Content-Type", &content_type.to_string())?;
            if let Some(mut tags) = additional_tags {
                tags.push(content_tag);
                additional_tags = Some(tags);
            } else {
                additional_tags = Some(vec![content_tag]);
            }
        }

        let transaction = self
            .create_transaction_from_file_path(
                file_path.clone(),
                additional_tags,
                last_tx,
                price_terms,
                auto_content_tag,
            )
            .await?;

        let (signed_transaction, sig_response): (Transaction, SigResponse) = self
            .sign_transaction_with_sol(transaction, solana_url, sol_ar_url, from_keypair)
            .await?;

        let (id, reward) = if signed_transaction.data.0.len() > MAX_TX_DATA as usize {
            self.post_transaction_chunks(signed_transaction, 100)
                .await?
        } else {
            self.post_transaction(&signed_transaction).await?
        };

        let mut status = Status {
            file_path: Some(file_path),
            content_type: status_content_type,
            id,
            reward,
            ..Default::default()
        };

        if let Some(log_dir) = log_dir {
            status.sol_sig = Some(sig_response);
            self.write_status(status.clone(), log_dir, None).await?;
        }
        Ok(status)
    }

    /// Uploads files from an iterator of paths.
    ///
    /// Optionally logs Status objects to `log_dir`, if provided and optionally adds tags to each
    ///  transaction from an iterator of tags that must be the same size as the paths iterator.
    pub async fn upload_files_from_paths<IP, IT>(
        &self,
        paths_iter: IP,
        log_dir: Option<PathBuf>,
        tags_iter: Option<IT>,
        last_tx: Option<Base64>,
        price_terms: (u64, u64),
    ) -> Result<Vec<Status>, Error>
    where
        IP: Iterator<Item = PathBuf> + Send,
        IT: Iterator<Item = Option<Vec<Tag<Base64>>>> + Send,
    {
        let statuses = if let Some(tags_iter) = tags_iter {
            try_join_all(paths_iter.zip(tags_iter).map(|(p, t)| {
                self.upload_file_from_path(p, log_dir.clone(), t, last_tx.clone(), price_terms)
            }))
        } else {
            try_join_all(paths_iter.map(|p| {
                self.upload_file_from_path(p, log_dir.clone(), None, last_tx.clone(), price_terms)
            }))
        }
        .await?;
        Ok(statuses)
    }

    //-------------------------
    // Status
    //-------------------------

    pub async fn create_log_dir(&self, parent_dir: &Path) -> Result<PathBuf, Error> {
        let mut rand_bytes: [u8; 8] = [0; 8];
        self.crypto.fill_rand(&mut rand_bytes)?;
        let suffix = base64::encode_config(rand_bytes, base64::URL_SAFE_NO_PAD);
        let log_dir = parent_dir.join(format!("arloader_{}", suffix));
        fs::create_dir_all(&log_dir).await?;
        Ok(log_dir)
    }

    /// Filters saved Status objects by status and/or number of confirmations. Return
    /// all statuses if no status codes or maximum confirmations are provided.
    ///
    /// If there is no raw status object and max_confirms is passed, it
    /// assumes there are zero confirms. This is designed to be used to
    /// determine whether all files have a confirmed status and to collect the
    /// paths of the files that need to be re-uploaded.
    pub fn filter_statuses<S>(
        &self,
        all_statuses: Vec<S>,
        statuses: Option<Vec<StatusCode>>,
        max_confirms: Option<u64>,
    ) -> Result<Vec<S>, Error>
    where
        S: Filterable,
    {
        // let all_statuses = self.read_statuses(paths_iter, log_dir).await?;

        let filtered = if let Some(statuses) = statuses {
            if let Some(max_confirms) = max_confirms {
                all_statuses
                    .into_iter()
                    .filter(|s| {
                        let s = s.get_filter_elements();
                        let confirms = if let Some(raw_status) = &s.raw_status {
                            raw_status.number_of_confirmations
                        } else {
                            0
                        };
                        (&statuses.iter().any(|c| c == s.status)) & (confirms <= max_confirms)
                    })
                    .collect()
            } else {
                all_statuses
                    .into_iter()
                    .filter(|s| {
                        let s = s.get_filter_elements();
                        statuses.iter().any(|c| c == s.status)
                    })
                    .collect()
            }
        } else {
            if let Some(max_confirms) = max_confirms {
                all_statuses
                    .into_iter()
                    .filter(|s| {
                        let s = s.get_filter_elements();
                        let confirms = if let Some(raw_status) = &s.raw_status {
                            raw_status.number_of_confirmations
                        } else {
                            0
                        };
                        confirms <= max_confirms
                    })
                    .collect()
            } else {
                all_statuses
            }
        };

        Ok(filtered)
    }

    /// Gets status from network.
    pub async fn get_status(&self, id: &Base64) -> Result<Status, Error> {
        let url = self.base_url.join(&format!("tx/{}/status", id))?;
        let resp = reqwest::get(url).await?;
        let mut status = Status {
            id: id.clone(),
            ..Status::default()
        };

        match resp.status() {
            ResponseStatusCode::OK => {
                let resp_string = resp.text().await?;
                if &resp_string == &String::from("Pending") {
                    status.status = StatusCode::Pending;
                } else {
                    status.raw_status = Some(serde_json::from_str(&resp_string)?);
                    status.status = StatusCode::Confirmed;
                }
            }
            ResponseStatusCode::ACCEPTED => {
                status.status = StatusCode::Pending;
            }
            ResponseStatusCode::NOT_FOUND => {
                status.status = StatusCode::NotFound;
            }
            other_status => {
                return Err(Error::ArweaveNetworkError(other_status));
            }
        }
        Ok(status)
    }

    pub async fn read_bundle_status(&self, file_path: PathBuf) -> Result<BundleStatus, Error> {
        let data = fs::read_to_string(&file_path).await?;
        let status = serde_json::from_str::<BundleStatus>(&data)?;
        Ok(status)
    }

    // Reads statuses from a list of paths.
    pub async fn read_bundle_statuses(&self, log_dir: &str) -> Result<Vec<BundleStatus>, Error> {
        let paths_iter = glob(&format!("{}*.json", log_dir))?
            .filter_map(Result::ok)
            .filter(|p| file_stem_is_valid_txid(p));
        try_join_all(paths_iter.map(|p| self.read_bundle_status(p))).await
    }

    pub async fn status_summary<IP>(
        &self,
        paths_iter: IP,
        log_dir: PathBuf,
    ) -> Result<String, Error>
    where
        IP: Iterator<Item = PathBuf> + Send,
    {
        let statuses = self.read_statuses(paths_iter, log_dir).await?;
        let status_counts: HashMap<StatusCode, u32> =
            statuses
                .into_iter()
                .fold(HashMap::new(), |mut map, status| {
                    *map.entry(status.status).or_insert(0) += 1;
                    map
                });

        let mut total = 0;
        let mut output = String::new();
        writeln!(output, " {:<15}  {:>10}", "status", "count")?;
        writeln!(output, "{:-<29}", "")?;
        for k in vec![
            StatusCode::Submitted,
            StatusCode::Pending,
            StatusCode::NotFound,
            StatusCode::Confirmed,
        ] {
            let v = status_counts.get(&k).unwrap_or(&0);
            writeln!(output, " {:<16} {:>10}", &k.to_string(), v)?;
            total += v;
        }

        writeln!(output, "{:-<29}", "")?;
        writeln!(output, " {:<15}  {:>10}", "Total", total)?;

        Ok(output)
    }

    // Reads a status from file.
    pub async fn read_status(&self, file_path: PathBuf, log_dir: PathBuf) -> Result<Status, Error> {
        let file_path_hash = blake3::hash(file_path.to_str().unwrap().as_bytes());

        let status_path = log_dir
            .join(file_path_hash.to_string())
            .with_extension("json");

        if status_path.exists() {
            let data = fs::read_to_string(status_path).await?;
            let status: Status = serde_json::from_str(&data)?;
            Ok(status)
        } else {
            Err(Error::StatusNotFound)
        }
    }

    // Reads statuses from a list of paths.
    pub async fn read_statuses<IP>(
        &self,
        paths_iter: IP,
        log_dir: PathBuf,
    ) -> Result<Vec<Status>, Error>
    where
        IP: Iterator<Item = PathBuf> + Send,
    {
        try_join_all(paths_iter.map(|p| self.read_status(p, log_dir.clone()))).await
    }

    pub async fn update_bundle_status(&self, file_path: PathBuf) -> Result<BundleStatus, Error> {
        let data = fs::read_to_string(&file_path).await?;
        let mut status: BundleStatus = serde_json::from_str(&data)?;
        let trans_status = self.get_status(&status.id).await?;
        status.last_modified = Utc::now();
        status.status = trans_status.status;
        status.raw_status = trans_status.raw_status;
        fs::write(&file_path, serde_json::to_string(&status)?).await?;
        Ok(status)
    }

    pub async fn update_status(
        &self,
        file_path: PathBuf,
        log_dir: PathBuf,
    ) -> Result<Status, Error> {
        let mut status = self.read_status(file_path, log_dir.clone()).await?;
        let trans_status = self.get_status(&status.id).await?;
        status.last_modified = Utc::now();
        status.status = trans_status.status;
        status.raw_status = trans_status.raw_status;
        self.write_status(status.clone(), log_dir, None).await?;
        Ok(status)
    }

    pub async fn update_statuses<IP>(
        &self,
        paths_iter: IP,
        log_dir: PathBuf,
    ) -> Result<Vec<Status>, Error>
    where
        IP: Iterator<Item = PathBuf> + Send,
    {
        try_join_all(paths_iter.map(|p| self.update_status(p, log_dir.clone()))).await
    }

    /// Writes Status Json to `log_dir` with file name based on BLAKE3 hash of `status.file_path`.
    ///
    /// This is done to facilitate checking the status of uploaded file and also means that only
    /// one status object can exist for a given `file_path`. If for some reason you wanted to record
    /// statuses for multiple uploads of the same file you can provide a different `log_dir` (or copy the
    /// file to a different directory and upload from there).
    pub async fn write_status(
        &self,
        status: Status,
        log_dir: PathBuf,
        file_stem: Option<String>,
    ) -> Result<(), Error> {
        let file_stem = if let Some(stem) = file_stem {
            stem
        } else {
            if let Some(file_path) = &status.file_path {
                if status.id.0.is_empty() {
                    return Err(error::Error::UnsignedTransaction.into());
                }
                blake3::hash(file_path.to_str().unwrap().as_bytes()).to_string()
            } else {
                format!("txid_{}", status.id)
            }
        };

        fs::write(
            log_dir.join(file_stem).with_extension("json"),
            serde_json::to_string(&status)?,
        )
        .await?;
        Ok(())
    }

    //-------------------------
    // Manifest
    //-------------------------

    pub fn create_data_item_from_manifest(&self, manifest: Value) -> Result<DataItem, Error> {
        let tags = vec![
            Tag::<String>::from_utf8_strs("Content-Type", "application/x.arweave-manifest+json")?,
            Tag::<String>::from_utf8_strs("User-Agent", &format!("arloader/{}", VERSION))?,
        ];

        // let mut anchor = Base64(Vec::with_capacity(32));
        // self.crypto.fill_rand(&mut anchor.0)?;

        Ok(DataItem {
            data: Base64(serde_json::to_string(&manifest)?.as_bytes().to_vec()),
            tags,
            // anchor,
            ..DataItem::default()
        })
    }

    pub fn create_manifest(&self, statuses: Vec<Status>) -> Result<Value, Error> {
        let paths = statuses
            .into_iter()
            .fold(serde_json::Map::new(), |mut m, s| {
                m.insert(
                    s.file_path.unwrap().to_str().unwrap().to_string(),
                    json!({"id": s.id.to_string(), "content_type": s.content_type}),
                );
                m
            });

        let manifest = json!({
            "manifest": "arweave/paths",
            "version": "0.1.0",
            "paths": Value::Object(paths)
        });

        Ok(manifest)
    }

    pub fn create_manifest_from_bundle_statuses(
        &self,
        statuses: Vec<BundleStatus>,
    ) -> Result<Value, Error> {
        let paths = statuses
            .into_iter()
            .fold(serde_json::Map::new(), |mut m, mut s| {
                m.append(s.file_paths.as_object_mut().unwrap());
                m
            });

        let manifest = json!({
            "manifest": "arweave/paths",
            "version": "0.1.0",
            "paths": Value::Object(paths)
        });

        Ok(manifest)
    }

    pub async fn create_transaction_from_manifest(
        &self,
        manifest: Value,
        price_terms: (u64, u64),
    ) -> Result<Transaction, Error> {
        let tags = vec![Tag::<Base64>::from_utf8_strs(
            "Content-Type",
            "application/x.arweave-manifest+json",
        )?];

        // let mut anchor = Base64(Vec::with_capacity(32));
        // self.crypto.fill_rand(&mut anchor.0)?;

        let data = serde_json::to_string(&manifest)?.as_bytes().to_vec();
        let transaction = self
            .create_transaction(data, Some(tags), None, price_terms, false)
            .await?;

        Ok(transaction)
    }

    pub async fn upload_manifest_from_bundle_log_dir(
        &self,
        log_dir: &str,
        price_terms: (u64, u64),
        solana_url: Url,
        sol_ar_url: Url,
        from_keypair: Option<Keypair>,
    ) -> Result<String, Error> {
        let paths: Vec<PathBuf> = glob(&format!("{}*.json", log_dir.clone()))?
            .filter_map(Result::ok)
            .collect();

        let paths_len = paths.len();
        if paths_len == 0 {
            return Ok(format!("No bundle statuses found in {}", log_dir));
        };

        let statuses = self.read_bundle_statuses(log_dir).await?;

        let manifest = self.create_manifest_from_bundle_statuses(statuses)?;
        let num_files = manifest["paths"].as_object().unwrap().keys().len();
        let transaction = self
            .create_transaction_from_manifest(manifest.clone(), price_terms)
            .await?;

        let signed_transaction = if let Some(from_keypair) = from_keypair {
            let (signed_transaction, _): (Transaction, SigResponse) = self
                .sign_transaction_with_sol(transaction, solana_url, sol_ar_url, &from_keypair)
                .await?;
            signed_transaction
        } else {
            self.sign_transaction(transaction)?
        };

        let (id, _) = self.post_transaction(&signed_transaction).await?;

        self.write_manifest(manifest, id.to_string(), PathBuf::from(log_dir))
            .await?;

        Ok(format!("Uploaded manifest for {} files and wrote to {}manifest_{id}.json.\n\nRun `arloader get-status {id}` to confirm manifest transaction.",
        num_files, log_dir, id=id.to_string()))
    }

    pub async fn write_manifest(
        &self,
        manifest: Value,
        transaction_id: String,
        log_dir: PathBuf,
    ) -> Result<(), Error> {
        let mut consolidated_paths = serde_json::Map::new();
        for (file_path, id_obj) in manifest["paths"].as_object().unwrap() {
            let id = id_obj["id"].as_str().unwrap();
            let content_type = id_obj["content_type"].as_str().unwrap();
            consolidated_paths.insert(
                file_path.to_owned(),
                json!({
                    "id": id,
                    "files": [
                        {"uri": format!("https://arweave.net/{}", id), "type": content_type},
                        {"uri": format!("https://arweave.net/{}/{}", transaction_id, file_path), "type": content_type}
                    ]
                }),
            );
        }
        fs::write(
            log_dir
                .join(format!("manifest_{}", transaction_id))
                .with_extension("json"),
            serde_json::to_string(&json!(consolidated_paths))?,
        )
        .await?;
        Ok(())
    }

    //-------------------------
    // Metadata
    //-------------------------

    pub async fn read_metadata_file(&self, file_path: PathBuf) -> Result<Value, Error> {
        let data = fs::read_to_string(file_path.clone()).await?;
        let metadata: Value = serde_json::from_str(&data)?;
        Ok(json!({"file_path": file_path.display().to_string(), "metadata": metadata}))
    }

    pub async fn update_metadata_file(
        &self,
        file_path: PathBuf,
        mut files_array: Vec<Value>,
        image_link: Option<String>,
        animation_url_link: Option<String>,
    ) -> Result<(), Error> {
        let data = fs::read_to_string(file_path.clone()).await?;
        let mut metadata: Value = serde_json::from_str(&data)?;
        let metadata = metadata.as_object_mut().unwrap();

        if let Some(image_link) = image_link {
            metadata.insert("image".to_string(), Value::String(image_link));
        }

        if let Some(animation_url_link) = animation_url_link {
            metadata.insert(
                "animation_url".to_string(),
                Value::String(animation_url_link),
            );
        }

        let properties = if let Some(properties) = metadata.get_mut("properties") {
            properties.as_object_mut().unwrap()
        } else {
            metadata.insert("properties".to_string(), json!({}));
            metadata["properties"].as_object_mut().unwrap()
        };

        if let Some(files) = properties.get_mut("files") {
            files.as_array_mut().unwrap().append(&mut files_array);
        } else {
            properties.insert("files".to_string(), Value::Array(files_array));
        }

        fs::write(file_path, serde_json::to_string(&json!(metadata))?).await?;
        Ok(())
    }

    pub async fn update_metadata<IP>(
        &self,
        paths_iter: IP,
        manifest_path: PathBuf,
        image_link_file: bool,
        update_image_link: bool,
        update_animation_url_link: bool,
    ) -> Result<(), Error>
    where
        IP: Iterator<Item = PathBuf> + Send,
    {
        if manifest_path.exists() {
            let manifest_id = manifest_path
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap()
                .replace("manifest_", "");
            let data = fs::read_to_string(manifest_path.clone()).await?;
            let mut manifest: Value = serde_json::from_str(&data)?;
            let manifest = manifest.as_object_mut().unwrap();

            try_join_all(paths_iter.map(|p| {
                let path_object = manifest.get(&p.display().to_string()).unwrap();

                let link = if image_link_file {
                    format!(
                        "https://arweave.net/{}/{}",
                        manifest_id,
                        &p.display().to_string()
                    )
                } else {
                    format!(
                        "https://arweave.net/{}",
                        path_object["id"].as_str().unwrap()
                    )
                };

                let image_link = if update_image_link {
                    Some(link.clone())
                } else {
                    None
                };

                let animation_url_link = if update_animation_url_link {
                    Some(link)
                } else {
                    None
                };

                let files_array = if image_link_file {
                    path_object["files"].as_array().unwrap().clone()
                } else {
                    vec![path_object["files"]
                        .as_array()
                        .unwrap()
                        .get(0)
                        .unwrap()
                        .clone()]
                };
                self.update_metadata_file(
                    p.with_extension("json"),
                    files_array,
                    image_link,
                    animation_url_link,
                )
            }))
            .await?;
            Ok(())
        } else {
            Err(Error::ManifestNotFound)
        }
    }

    pub async fn write_metaplex_items<IP>(
        &self,
        paths_iter: IP,
        manifest_path: PathBuf,
        link_file: bool,
    ) -> Result<PathBuf, Error>
    where
        IP: Iterator<Item = PathBuf> + Send,
    {
        if manifest_path.exists() {
            let manifest_id = manifest_path
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap()
                .replace("manifest_", "");
            let data = fs::read_to_string(manifest_path.clone()).await?;
            let mut manifest: Value = serde_json::from_str(&data)?;
            let manifest = manifest.as_object_mut().unwrap();

            let metadata = try_join_all(paths_iter.map(|p| self.read_metadata_file(p))).await?;

            let items = metadata.iter().fold(serde_json::Map::new(), |mut m, meta| {
                let name = meta["metadata"]["name"].as_str().unwrap();
                let file_path = meta["file_path"].as_str().unwrap();
                let id = manifest
                    .get(file_path)
                    .unwrap()
                    .get("id")
                    .unwrap()
                    .as_str()
                    .unwrap();
                let link = if link_file {
                    format!("https://arweave.net/{}/{}", manifest_id, file_path)
                } else {
                    format!("https://arweave.net/{}", id)
                };
                m.insert(
                    PathBuf::from(file_path)
                        .file_stem()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_string(),
                    json!({"name": name, "link": link, "onChain": false}),
                );
                m
            });

            let manifest_items_path = manifest_path
                .parent()
                .unwrap()
                .to_path_buf()
                .join(format!("metaplex_items_{}", manifest_id))
                .with_extension("json");
            fs::write(&manifest_items_path, serde_json::to_string(&json!(items))?).await?;
            Ok(manifest_items_path)
        } else {
            Err(Error::ManifestNotFound)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        error::Error,
        transaction::{Base64, FromUtf8Strs, Tag},
        utils::TempDir,
        Arweave, Status,
    };
    use futures::future::try_join_all;
    use glob::glob;
    use matches::assert_matches;
    use std::{path::PathBuf, str::FromStr, time::Instant};
    use tokio::fs;
    use url::Url;

    #[tokio::test]
    async fn test_cannot_post_unsigned_transaction() -> Result<(), Error> {
        let arweave = Arweave::from_keypair_path(
            PathBuf::from(
                "tests/fixtures/arweave-key-7eV1qae4qVNqsNChg3Scdi-DpOLJPCogct4ixoq1WNg.json",
            ),
            Url::from_str("http://url.com").unwrap(),
        )
        .await?;

        let file_path = PathBuf::from("tests/fixtures/0.png");
        let last_tx = Base64::from_str("LCwsLCwsLA")?;
        let other_tags = vec![Tag::<Base64>::from_utf8_strs("key2", "value2")?];
        let transaction = arweave
            .create_transaction_from_file_path(
                file_path,
                Some(other_tags),
                Some(last_tx),
                (0, 0),
                true,
            )
            .await?;

        let error = arweave.post_transaction(&transaction).await.unwrap_err();
        assert_matches!(error, Error::UnsignedTransaction);

        Ok(())
    }

    #[tokio::test]
    async fn test_create_write_read_status() -> Result<(), Error> {
        let arweave = Arweave::from_keypair_path(
            PathBuf::from(
                "tests/fixtures/arweave-key-7eV1qae4qVNqsNChg3Scdi-DpOLJPCogct4ixoq1WNg.json",
            ),
            Url::from_str("http://url.com").unwrap(),
        )
        .await?;

        let file_path = PathBuf::from("tests/fixtures/0.png");
        let last_tx = Base64::from_str("LCwsLCwsLA")?;
        let other_tags = vec![Tag::<Base64>::from_utf8_strs("key2", "value2")?];
        let transaction = arweave
            .create_transaction_from_file_path(
                file_path.clone(),
                Some(other_tags),
                Some(last_tx),
                (0, 0),
                true,
            )
            .await?;

        let signed_transaction = arweave.sign_transaction(transaction)?;

        let status = Status {
            id: signed_transaction.id.clone(),
            reward: signed_transaction.reward,
            file_path: Some(file_path.clone()),
            ..Default::default()
        };

        let temp_log_dir = TempDir::from_str("./tests/").await?;
        let log_dir = temp_log_dir.0.clone();

        arweave
            .write_status(status.clone(), log_dir.clone(), None)
            .await?;

        let read_status = arweave.read_status(file_path, log_dir).await?;

        assert_eq!(status, read_status);

        Ok(())
    }

    #[tokio::test]
    async fn test_create_and_deserialize_large_bundle() -> Result<(), Error> {
        let arweave = Arweave::from_keypair_path(
            PathBuf::from(
                "tests/fixtures/arweave-key-7eV1qae4qVNqsNChg3Scdi-DpOLJPCogct4ixoq1WNg.json",
            ),
            Url::from_str("http://url.com").unwrap(),
        )
        .await?;

        let file_path = PathBuf::from("tests/fixtures/1mb.bin");
        let temp_dir = TempDir::from_str("./tests/").await?;
        let start = Instant::now();

        let _ = try_join_all((0..100).map(|i| {
            fs::copy(
                file_path.clone(),
                temp_dir.0.join(format!("{}", i)).with_extension("bin"),
            )
        }))
        .await?;
        let duration = start.elapsed();
        println!("Time elapsed to prepare files: {} ms", duration.as_millis());

        let glob_str = format!("{}/*.bin", temp_dir.0.display().to_string());
        let paths_iter = glob(&glob_str)?.filter_map(Result::ok).collect();
        let pre_data_items = arweave
            .create_data_items_from_file_paths(paths_iter, Vec::new())
            .await?;
        let duration = start.elapsed() - duration;
        println!(
            "Time elapsed to create data items from file paths: {} ms",
            duration.as_millis()
        );

        let start = Instant::now();
        let (bundle, _) = arweave.create_bundle_from_data_items(pre_data_items.clone())?;
        let duration = start.elapsed();
        println!("Time elapsed to create bundle: {} ms", duration.as_millis());

        let start = Instant::now();
        let _ = arweave.create_transaction(bundle.clone(), None, None, (0, 0), true);
        let duration = start.elapsed();
        println!(
            "Time elapsed to create transaction: {} ms",
            duration.as_millis()
        );

        let start = Instant::now();
        let post_data_items = arweave.deserialize_bundle(bundle)?;
        let duration = start.elapsed();
        println!("Time elapsed to deserialize: {} ms", duration.as_millis());
        assert_eq!(post_data_items.len(), 100);

        Ok(())
    }

    #[tokio::test]
    async fn test_price_points() -> Result<(), Error> {
        let mut price = 0 as u64;
        println!("{:>6}  {:>12} {:>12}", "size", "winstons", "incremental");
        println!("{:-<40}", "");
        for p in 1..10 {
            let size = p * 100 * 256;
            let new_price = reqwest::get(format!("https://arweave.net/price/{}", size * 1024))
                .await?
                .json::<u64>()
                .await?;
            println!("{:>6}k {:>12} {:>12}", size, new_price, new_price - price);
            price = new_price;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_file_chunks() -> Result<(), Error> {
        let arweave = Arweave::from_keypair_path(
            PathBuf::from(
                "tests/fixtures/arweave-key-7eV1qae4qVNqsNChg3Scdi-DpOLJPCogct4ixoq1WNg.json",
            ),
            Url::from_str("http://url.com").unwrap(),
        )
        .await?;

        let paths_iter = glob("tests/fixtures/*.png")?.filter_map(Result::ok);

        let paths_chunks = arweave.chunk_file_paths(paths_iter, 5000)?;

        let (number_of_files, data_size) = paths_chunks
            .iter()
            .fold((0usize, 0u64), |(n, d), p| (n + p.0.len(), d + p.1));

        assert_eq!((10, 18265), (number_of_files, data_size));
        Ok(())
    }

    #[test]
    fn test_mime_types() -> Result<(), Error> {
        let file_paths = vec![
            "some.png",
            "some.jpg",
            "some.json",
            "some.txt",
            "some.css",
            "some.js",
        ];

        let paths_iter = file_paths.iter().map(|p| PathBuf::from(p));

        for p in paths_iter {
            println!("{}", mime_guess::from_path(p).first().unwrap());
        }

        Ok(())
    }
}
