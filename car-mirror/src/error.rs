use libipld::Cid;

use crate::incremental_verification::BlockState;

/// Errors raised from the CAR mirror library
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// An error raised during receival of blocks, when more than the configured maximum
    /// bytes are received in a single batch. See the `Config` type.
    #[error("Received more than {receive_maximum} bytes ({block_bytes}), aborting request.")]
    TooManyBytes {
        /// The configured amount of maximum bytes to receive
        receive_maximum: usize,
        /// The actual amount of bytes received so far
        block_bytes: usize,
    },

    /// This library only supports a subset of default codecs, including DAG-CBOR, DAG-JSON, DAG-PB and more.g
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

    /// This error is raised when the hash function that the `BlockStore` uses a different hashing function
    /// than the blocks which are received over the wire.
    /// This error will be removed in the future, when the block store trait gets modified to support specifying
    /// the hash function.
    #[error("BlockStore uses an incompatible hashing function: CID mismatched, expected {cid}, got {actual_cid}")]
    BlockStoreIncompatible {
        /// The expected CID
        cid: Box<Cid>,
        /// The CID returned from the BlockStore implementation
        actual_cid: Box<Cid>,
    },

    // -------------
    // Anyhow Errors
    // -------------
    /// An error raised when trying to parse a block (e.g. to look for further links)
    #[error("Error during block parsing: {0}")]
    ParsingError(anyhow::Error),

    /// An error rasied when trying to read or write a CAR file.
    #[error("CAR (de)serialization error: {0}")]
    CarFileError(anyhow::Error),

    /// An error rasied from the blockstore.
    #[error("BlockStore error: {0}")]
    BlockStoreError(anyhow::Error),

    // ----------
    // Sub-errors
    // ----------
    /// Errors related to incremental verification
    #[error(transparent)]
    IncrementalVerificationError(#[from] IncrementalVerificationError),
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
