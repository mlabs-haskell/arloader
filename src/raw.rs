pub use crate::*;

impl Arweave {
    pub async fn upload_raw_data(
        &self,
        data: Vec<u8>,
        content_type: Option<&str>,
        log_dir: Option<PathBuf>,
        mut additional_tags: Option<Vec<Tag<Base64>>>,
        last_tx: Option<Base64>,
    ) -> Result<Status, Error> {
        let price_terms = self.get_price_terms(1.0).await?;
        let mut auto_content_tag = true;
        let mut status_content_type = mime_guess::mime::OCTET_STREAM.to_string();

        if let Some(content_type) =
            content_type.or(infer::get(&data).map(|kind| kind.mime_type().into()))
        {
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
            .create_transaction(
                data.clone(),
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
            content_type: status_content_type,
            ..Default::default()
        };

        if let Some(log_dir) = log_dir {
            self.write_status(status.clone(), log_dir, None).await?;
        }
        Ok(status)
    }

    pub async fn upload_raw_data_with_sol(
        &self,
        data: Vec<u8>,
        content_type: Option<&str>,
        log_dir: Option<PathBuf>,
        mut additional_tags: Option<Vec<Tag<Base64>>>,
        last_tx: Option<Base64>,
        solana_url: Url,
        sol_ar_url: Url,
        from_keypair: &Keypair,
    ) -> Result<Status, Error> {
        let price_terms = self.get_price_terms(1.0).await?;
        let mut auto_content_tag = true;
        let mut status_content_type = mime_guess::mime::OCTET_STREAM.to_string();

        if let Some(content_type) =
            content_type.or(infer::get(&data).map(|kind| kind.mime_type().into()))
        {
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
            .create_transaction(
                data.clone(),
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
            id,
            reward,
            content_type: status_content_type,
            ..Default::default()
        };

        if let Some(log_dir) = log_dir {
            status.sol_sig = Some(sig_response);
            self.write_status(status.clone(), log_dir, None).await?;
        }
        Ok(status)
    }
}
