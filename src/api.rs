use async_trait::async_trait;

pub use crate::*;

#[async_trait]
pub trait ArweaveApi {
    fn get_url(&self) -> &Url;

    async fn get_price_terms(&self, reward_mult: f32) -> Result<(u64, u64), Error>;

    async fn upload_raw_data(
        &self,
        data: Vec<u8>,
        content_type: Option<&str>,
        log_dir: Option<PathBuf>,
        additional_tags: Option<Vec<Tag<Base64>>>,
        last_tx: Option<Base64>,
    ) -> Result<Status, Error>;

    async fn upload_file_from_path(
        &self,
        file_path: PathBuf,
        log_dir: Option<PathBuf>,
        additional_tags: Option<Vec<Tag<Base64>>>,
        last_tx: Option<Base64>,
        price_terms: (u64, u64),
    ) -> Result<Status, Error>;

    async fn get_status(&self, id: &Base64) -> Result<Status, Error>;

    async fn get_price(&self, bytes: &u64) -> Result<BytesPrice, Error>;
}

#[async_trait]
impl ArweaveApi for Arweave {
    fn get_url(&self) -> &Url {
        &self.base_url
    }

    async fn get_price_terms(&self, reward_mult: f32) -> Result<(u64, u64), Error> {
        self.get_price_terms(reward_mult).await
    }

    async fn upload_raw_data(
        &self,
        data: Vec<u8>,
        content_type: Option<&str>,
        log_dir: Option<PathBuf>,
        additional_tags: Option<Vec<Tag<Base64>>>,
        last_tx: Option<Base64>,
    ) -> Result<Status, Error> {
        self.upload_raw_data(data, content_type, log_dir, additional_tags, last_tx)
            .await
    }

    async fn upload_file_from_path(
        &self,
        file_path: PathBuf,
        log_dir: Option<PathBuf>,
        additional_tags: Option<Vec<Tag<Base64>>>,
        last_tx: Option<Base64>,
        price_terms: (u64, u64),
    ) -> Result<Status, Error> {
        self.upload_file_from_path(file_path, log_dir, additional_tags, last_tx, price_terms)
            .await
    }

    async fn get_status(&self, id: &Base64) -> Result<Status, Error> {
        self.get_status(id).await
    }

    async fn get_price(&self, bytes: &u64) -> Result<BytesPrice, Error> {
        self.get_price(bytes).await
    }
}
