mod transport;

pub use http_client;
pub use reqwest;

macro_rules! impl_envd_client {
    ($module_name:ident, $proto_name:literal, $generated_client_mod:ident, $client_struct:ident) => {
        pub mod $module_name {
            tonic::include_proto!($proto_name);

            use crate::transport::Channel;
            use $generated_client_mod::$client_struct as GeneratedClient;

            #[derive(Clone)]
            pub struct $client_struct {
                inner: GeneratedClient<Channel>,
            }

            impl $client_struct {
                pub async fn connect(
                    dst: &str,
                    access_token: Option<&str>,
                ) -> Result<Self, anyhow::Error> {
                    let chan = crate::transport::new_channel(dst, access_token)?;
                    Ok(Self {
                        inner: GeneratedClient::new(chan),
                    })
                }

                pub fn new(chan: Channel) -> Self {
                    Self {
                        inner: GeneratedClient::new(chan),
                    }
                }
            }

            impl std::ops::Deref for $client_struct {
                type Target = GeneratedClient<Channel>;
                fn deref(&self) -> &Self::Target {
                    &self.inner
                }
            }

            impl std::ops::DerefMut for $client_struct {
                fn deref_mut(&mut self) -> &mut Self::Target {
                    &mut self.inner
                }
            }
        }
    };
}

impl_envd_client!(
    filesystem,
    "filesystem",
    filesystem_client,
    FilesystemClient
);

impl_envd_client!(process, "process", process_client, ProcessClient);
