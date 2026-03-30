use crate::protocol::registry::{resolve_protocol, NormalizedProtocol, ProtocolResolution};

use super::{
    BaseNodeRpcAdapter, IndexerPoolMetadata, JsonRpcClient, PoolMetadataProvider, SourceError,
};

#[derive(Debug, Clone)]
pub struct RpcBackfilledPoolMetadataProvider<'a, P, C> {
    primary: &'a P,
    node_adapter: &'a BaseNodeRpcAdapter<C>,
    default_fee_lookup_block_number: u64,
}

impl<'a, P, C> RpcBackfilledPoolMetadataProvider<'a, P, C> {
    pub fn new(
        primary: &'a P,
        node_adapter: &'a BaseNodeRpcAdapter<C>,
        default_fee_lookup_block_number: u64,
    ) -> Self {
        Self {
            primary,
            node_adapter,
            default_fee_lookup_block_number,
        }
    }
}

impl<P, C> PoolMetadataProvider for RpcBackfilledPoolMetadataProvider<'_, P, C>
where
    P: PoolMetadataProvider,
    C: JsonRpcClient,
{
    fn fetch_pool_metadata(&self, pool_address: &str) -> Result<IndexerPoolMetadata, SourceError> {
        let mut pool = self.primary.fetch_pool_metadata(pool_address)?;
        if pool.fee.is_some() {
            return Ok(pool);
        }

        let is_aerodrome_v3 = matches!(
            resolve_protocol(&pool.protocol),
            ProtocolResolution::Supported(spec)
                if spec.protocol == NormalizedProtocol::AerodromeV3
        );
        if !is_aerodrome_v3 {
            return Ok(pool);
        }

        let Some(factory_address) = pool.factory_address.as_deref() else {
            return Ok(pool);
        };
        let fee_lookup_block_number = pool
            .creation_block_number
            .map(|creation_block_number| {
                creation_block_number.max(self.default_fee_lookup_block_number)
            })
            .unwrap_or(self.default_fee_lookup_block_number);

        pool.fee = Some(self.node_adapter.fetch_aerodrome_v3_fee(
            factory_address,
            &pool.address,
            fee_lookup_block_number,
        )?);
        Ok(pool)
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, collections::HashMap, rc::Rc};

    use serde_json::{json, Value};

    use super::*;

    #[derive(Debug, Clone)]
    struct MockPoolMetadataProvider {
        pools: HashMap<String, IndexerPoolMetadata>,
    }

    impl PoolMetadataProvider for MockPoolMetadataProvider {
        fn fetch_pool_metadata(
            &self,
            pool_address: &str,
        ) -> Result<IndexerPoolMetadata, SourceError> {
            self.pools
                .get(pool_address)
                .cloned()
                .ok_or_else(|| SourceError::NotFound {
                    resource: "pool",
                    address: pool_address.to_owned(),
                })
        }
    }

    #[derive(Debug, Clone)]
    struct MockJsonRpcClient {
        call_log: Rc<RefCell<Vec<String>>>,
        response_by_call_key: HashMap<String, String>,
    }

    impl JsonRpcClient for MockJsonRpcClient {
        fn call(&self, method: &str, params: Vec<Value>) -> Result<Value, SourceError> {
            self.call_log.borrow_mut().push(method.to_owned());
            if method != "eth_call" {
                return Err(SourceError::Rpc {
                    code: -32601,
                    message: format!("unsupported method {method}"),
                });
            }
            let Some(Value::Object(call_obj)) = params.first() else {
                return Err(SourceError::InvalidRpcResponse {
                    message: "missing eth_call object".to_owned(),
                });
            };
            let Some(Value::String(to)) = call_obj.get("to") else {
                return Err(SourceError::InvalidRpcResponse {
                    message: "missing eth_call.to".to_owned(),
                });
            };
            let Some(Value::String(data)) = call_obj.get("data") else {
                return Err(SourceError::InvalidRpcResponse {
                    message: "missing eth_call.data".to_owned(),
                });
            };
            let Some(Value::String(block_tag)) = params.get(1) else {
                return Err(SourceError::InvalidRpcResponse {
                    message: "missing eth_call block tag".to_owned(),
                });
            };
            let call_key = format!("{to}:{data}:{block_tag}");
            let response = self.response_by_call_key.get(&call_key).ok_or_else(|| {
                SourceError::InvalidRpcResponse {
                    message: format!("missing mock eth_call response for {call_key}"),
                }
            })?;
            Ok(json!(response))
        }
    }

    #[test]
    fn rpc_backfilled_pool_metadata_provider_fills_missing_aerodrome_v3_fee() {
        let pool_address = "0xb2cc224c1c9fee385f8ad6a55b4d94e92359dc59".to_owned();
        let factory_address = "0x5e7bb104d84c7cb9b682aac2f3d509f5f406809a".to_owned();
        let call_data =
            crate::source::node_rpc::encode_aerodrome_v3_get_swap_fee_calldata(&pool_address)
                .unwrap_or_else(|error| panic!("calldata encoding failed: {error}"));

        let primary = MockPoolMetadataProvider {
            pools: HashMap::from([(
                pool_address.clone(),
                IndexerPoolMetadata {
                    address: pool_address.clone(),
                    protocol: "AerodromeV3".to_owned(),
                    tokens: vec![
                        "0x4200000000000000000000000000000000000006".to_owned(),
                        "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913".to_owned(),
                    ],
                    factory_address: Some(factory_address.clone()),
                    creation_block_number: Some(13_899_892),
                    fee: None,
                    tick_spacing: Some(100),
                },
            )]),
        };
        let call_log = Rc::new(RefCell::new(Vec::new()));
        let rpc_client = MockJsonRpcClient {
            call_log: call_log.clone(),
            response_by_call_key: HashMap::from([(
                format!("{factory_address}:{call_data}:0x1a835a8"),
                "0x00000000000000000000000000000000000000000000000000000000000007d0".to_owned(),
            )]),
        };
        let node_adapter = BaseNodeRpcAdapter::with_client(rpc_client);
        let provider = RpcBackfilledPoolMetadataProvider::new(&primary, &node_adapter, 27_801_000);

        let resolved = provider
            .fetch_pool_metadata(&pool_address)
            .unwrap_or_else(|error| panic!("pool lookup failed: {error}"));

        assert_eq!(resolved.fee, Some(2_000));
        assert_eq!(call_log.borrow().as_slice(), &["eth_call".to_owned()]);
    }

    #[test]
    fn rpc_backfilled_pool_metadata_provider_uses_creation_block_when_pool_is_newer_than_export_range(
    ) {
        let pool_address = "0xdbc6998296caa1652a810dc8d3baf4a8294330f1".to_owned();
        let factory_address = "0x5e7bb104d84c7cb9b682aac2f3d509f5f406809a".to_owned();
        let call_data =
            crate::source::node_rpc::encode_aerodrome_v3_get_swap_fee_calldata(&pool_address)
                .unwrap_or_else(|error| panic!("calldata encoding failed: {error}"));

        let primary = MockPoolMetadataProvider {
            pools: HashMap::from([(
                pool_address.clone(),
                IndexerPoolMetadata {
                    address: pool_address.clone(),
                    protocol: "AerodromeV3".to_owned(),
                    tokens: vec![
                        "0x4200000000000000000000000000000000000006".to_owned(),
                        "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913".to_owned(),
                    ],
                    factory_address: Some(factory_address.clone()),
                    creation_block_number: Some(34_763_160),
                    fee: None,
                    tick_spacing: Some(1),
                },
            )]),
        };
        let call_log = Rc::new(RefCell::new(Vec::new()));
        let rpc_client = MockJsonRpcClient {
            call_log: call_log.clone(),
            response_by_call_key: HashMap::from([(
                format!("{factory_address}:{call_data}:0x2127198"),
                "0x0000000000000000000000000000000000000000000000000000000000000064".to_owned(),
            )]),
        };
        let node_adapter = BaseNodeRpcAdapter::with_client(rpc_client);
        let provider = RpcBackfilledPoolMetadataProvider::new(&primary, &node_adapter, 27_801_000);

        let resolved = provider
            .fetch_pool_metadata(&pool_address)
            .unwrap_or_else(|error| panic!("pool lookup failed: {error}"));

        assert_eq!(resolved.fee, Some(100));
        assert_eq!(call_log.borrow().as_slice(), &["eth_call".to_owned()]);
    }
}
