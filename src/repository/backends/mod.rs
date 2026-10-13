pub(crate) mod common;
pub(crate) mod oss;
pub(crate) mod posixfs;

pub(crate) use oss::OssRepository;
pub(crate) use posixfs::PosixFsRepository;
