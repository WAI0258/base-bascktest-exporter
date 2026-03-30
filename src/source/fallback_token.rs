use std::collections::HashMap;

use super::{normalize_evm_address, SourceError, TokenMetadataProvider, TokenMetadataRef};

#[derive(Debug, Clone)]
pub struct FallbackTokenMetadataProvider<'a, P> {
    primary: &'a P,
    overrides: HashMap<String, TokenMetadataRef>,
}

impl<'a, P> FallbackTokenMetadataProvider<'a, P> {
    pub fn new(primary: &'a P, overrides: HashMap<String, TokenMetadataRef>) -> Self {
        Self { primary, overrides }
    }
}

impl<P> TokenMetadataProvider for FallbackTokenMetadataProvider<'_, P>
where
    P: TokenMetadataProvider,
{
    fn fetch_token_metadata(
        &self,
        token_address: &str,
        block_number: u64,
    ) -> Result<Option<TokenMetadataRef>, SourceError> {
        let normalized_token = normalize_evm_address("token_address", token_address)?;

        match self
            .primary
            .fetch_token_metadata(&normalized_token, block_number)
        {
            Ok(Some(token)) => Ok(Some(token)),
            Ok(None) | Err(_) => Ok(self.overrides.get(&normalized_token).cloned()),
        }
    }
}
