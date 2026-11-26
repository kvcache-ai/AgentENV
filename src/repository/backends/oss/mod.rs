mod client;
mod config;
mod documents;
mod repository;

#[cfg(test)]
pub(crate) use self::client::OssClient;
pub(crate) use self::repository::OssRepository;
