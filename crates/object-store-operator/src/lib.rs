mod auth;
mod operator;

pub use auth::{
    credential_source_from_fields, normalized_credential, parse_expiration,
    resolve_credential_from_process, CachedCredentialSource, CredentialFields, CredentialSource,
    CredentialSourceOptions, ResolvedCredential, CREDENTIAL_PROCESS_TIMEOUT,
    CREDENTIAL_REFRESH_BUFFER,
};
pub use operator::{
    build_object_store_operator, run_with_refresh, AddressingStyle, ObjectStoreOperatorConfig,
    ObjectStoreOperatorError, ObjectStoreOperatorResult, ObjectStoreService,
    OperatorWithCredential, DEFAULT_MAX_RETRIES, DEFAULT_TIMEOUT,
};

pub use opendal::{Error as OpenDalError, ErrorKind as OpenDalErrorKind, Operator};
pub type OpenDalResult<T> = opendal::Result<T>;
