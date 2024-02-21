use crate::incremental_verification::BlockState;
use libipld::Cid;
use wnfs_common::BlockStoreError;

/// Errors raised from the CAR mirror library
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// An error raised during receival of blocks, when more than the configured maximum
    /// bytes are received in a single batch. See the `Config` type.
    #[error("Expected to receive no more than {receive_maximum} bytes, but got at least {bytes_read}, aborting request.")]
    TooManyBytes {
        /// The configured amount of maximum bytes to receive
        receive_maximum: usize,
        /// The actual amount of bytes received so far
        bytes_read: usize,
    },

    /// An error raised when an individual block exceeded the maximum configured block size
    #[error("Maximum block size exceeded, maximum configured block size is {max_block_size} bytes, but got {block_bytes} at {cid}")]
    BlockSizeExceeded {
        /// The CID of the block that exceeded the maximum
        cid: Cid,
        /// The amount of bytes we got for this block up to this point
        block_bytes: usize,
        /// The maximum block size from our configuration
        max_block_size: usize,
    },

    /// This library only supports a subset of default codecs, including DAG-CBOR, DAG-JSON, DAG-PB and more.
    /// This is raised if an unknown codec is read from a CID. See the `libipld` library for more information.
    #[error("Unsupported codec in Cid: {cid}")]
    UnsupportedCodec {
        /// The CID with the unsupported codec
        cid: Cid,
    },

    /// This library only supports a subset of default hash functions, including SHA-256, SHA-3, BLAKE3 and more.
    /// This is raised if an unknown hash code is read from a CID. See the `libipld` library for more information.
    #[error("Unsupported hash code in CID {cid}")]
    UnsupportedHashCode {
        /// The CID with the unsupported hash function
        cid: Cid,
    },

    /// An error rasied from the blockstore.
    #[error("BlockStore error: {0}")]
    BlockStoreError(#[from] BlockStoreError),

    // -------------
    // Anyhow Errors
    // -------------
    /// An error raised when trying to parse a block (e.g. to look for further links)
    #[error("Error during block parsing: {0}")]
    ParsingError(anyhow::Error),

    // ----------
    // Sub-errors
    // ----------
    /// Errors related to incremental verification
    #[error(transparent)]
    IncrementalVerificationError(#[from] IncrementalVerificationError),

    /// An error rasied when trying to read or write a CAR file.
    #[error("CAR (de)serialization error: {0}")]
    CarFileError(#[from] iroh_car::Error),
}

/// Errors related to incremental verification
#[derive(thiserror::Error, Debug)]
pub enum IncrementalVerificationError {
    /// Raised when we receive a block with a CID that we don't expect.
    /// We only expect blocks when they're related to the root CID of a DAG.
    /// So a CID needs to have a path back to the root.
    #[error("Expected to want block {cid}, but block state is: {block_state:?}")]
    ExpectedWantedBlock {
        /// The CID of the block we're currently processing
        cid: Box<Cid>,
        /// The block state it has during incremental verification.
        /// So either we already have it or it's unexpected.
        block_state: BlockState,
    },

    /// Raised when the block stored in the CAR file doesn't match its hash.
    #[error("Digest mismatch in CAR file: expected {cid}, got {actual_cid}")]
    DigestMismatch {
        /// The expected CID
        cid: Box<Cid>,
        /// The CID it actually hashes to
        actual_cid: Box<Cid>,
    },
}
