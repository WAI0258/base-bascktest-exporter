use std::collections::HashMap;

use base_backtest_exporter::{BaseNodeRpcAdapter, JsonRpcClient, SourceError};
use serde_json::{json, Value};

#[derive(Debug, Default)]
struct MockJsonRpcClient {
    responses: HashMap<String, Value>,
}

impl MockJsonRpcClient {
    fn with_response(mut self, method: &str, params: Vec<Value>, result: Value) -> Self {
        self.responses.insert(Self::key(method, &params), result);
        self
    }

    fn key(method: &str, params: &[Value]) -> String {
        let params_str = match serde_json::to_string(params) {
            Ok(value) => value,
            Err(_) => "[]".to_owned(),
        };
        format!("{method}|{params_str}")
    }
}

impl JsonRpcClient for MockJsonRpcClient {
    fn call(&self, method: &str, params: Vec<Value>) -> Result<Value, SourceError> {
        let key = Self::key(method, &params);
        self.responses
            .get(&key)
            .cloned()
            .ok_or_else(|| SourceError::InvalidRpcResponse {
                message: format!("missing mock response for key {key}"),
            })
    }
}

#[test]
fn node_rpc_adapter_parses_block_header_and_receipts_without_reordering() {
    let block_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let tx_hash_0 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let tx_hash_1 = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    let topic_0 = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let topic_1 = "0x2222222222222222222222222222222222222222222222222222222222222222";
    let client = MockJsonRpcClient::default()
        .with_response(
            "eth_getBlockByNumber",
            vec![json!("0x100"), json!(false)],
            json!({
                "number": "0x100",
                "hash": block_hash,
                "timestamp": "0x65f4c5d2"
            }),
        )
        .with_response(
            "eth_getBlockReceipts",
            vec![json!("0x100")],
            json!([
                {
                    "transactionHash": tx_hash_0,
                    "transactionIndex": "0x2",
                    "blockNumber": "0x100",
                    "blockHash": block_hash,
                    "logs": [
                        {
                            "address": "0x3333333333333333333333333333333333333333",
                            "topics": [topic_0, topic_1],
                            "data": "0x00",
                            "logIndex": "0x0",
                            "removed": false
                        }
                    ]
                },
                {
                    "transactionHash": tx_hash_1,
                    "transactionIndex": "0x1",
                    "blockNumber": "0x100",
                    "blockHash": block_hash,
                    "logs": []
                }
            ]),
        );
    let adapter = BaseNodeRpcAdapter::with_client(client);

    let header = match adapter.fetch_block_header(0x100) {
        Ok(header) => header,
        Err(error) => panic!("header parse should succeed: {error}"),
    };
    assert_eq!(header.block_number, 0x100);
    assert_eq!(header.timestamp_secs, 0x65f4c5d2);
    assert_eq!(header.block_hash, block_hash);

    let receipts = match adapter.fetch_block_receipts(0x100) {
        Ok(receipts) => receipts,
        Err(error) => panic!("receipt parse should succeed: {error}"),
    };
    assert_eq!(receipts.len(), 2);
    assert_eq!(receipts[0].transaction_hash, tx_hash_0);
    assert_eq!(receipts[0].transaction_index, 2);
    assert_eq!(receipts[1].transaction_hash, tx_hash_1);
    assert_eq!(receipts[1].transaction_index, 1);
    assert_eq!(receipts[0].logs.len(), 1);
    assert_eq!(receipts[0].logs[0].topics, vec![topic_0, topic_1]);
}

#[test]
fn node_rpc_adapter_fetch_block_range_is_monotonic_and_header_receipts_aligned() {
    let block_hash_0 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let block_hash_1 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let tx_hash_0 = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    let tx_hash_1 = "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    let client = MockJsonRpcClient::default()
        .with_response(
            "eth_getBlockByNumber",
            vec![json!("0x100"), json!(false)],
            json!({
                "number": "0x100",
                "hash": block_hash_0,
                "timestamp": "0x65f4c5d2"
            }),
        )
        .with_response(
            "eth_getBlockByNumber",
            vec![json!("0x101"), json!(false)],
            json!({
                "number": "0x101",
                "hash": block_hash_1,
                "timestamp": "0x65f4c5d3"
            }),
        )
        .with_response(
            "eth_getBlockReceipts",
            vec![json!("0x100")],
            json!([
                {
                    "transactionHash": tx_hash_0,
                    "transactionIndex": "0x0",
                    "blockNumber": "0x100",
                    "blockHash": block_hash_0,
                    "logs": []
                }
            ]),
        )
        .with_response(
            "eth_getBlockReceipts",
            vec![json!("0x101")],
            json!([
                {
                    "transactionHash": tx_hash_1,
                    "transactionIndex": "0x0",
                    "blockNumber": "0x101",
                    "blockHash": block_hash_1,
                    "logs": []
                }
            ]),
        );
    let adapter = BaseNodeRpcAdapter::with_client(client);

    let blocks = match adapter.fetch_block_range(0x100, 0x101) {
        Ok(blocks) => blocks,
        Err(error) => panic!("block range parse should succeed: {error}"),
    };
    assert_eq!(blocks.len(), 2);
    assert_eq!(blocks[0].header.block_number, 0x100);
    assert_eq!(blocks[1].header.block_number, 0x101);
    assert_eq!(blocks[0].receipts.len(), 1);
    assert_eq!(blocks[1].receipts.len(), 1);
    assert_eq!(blocks[0].receipts[0].block_hash, blocks[0].header.block_hash);
    assert_eq!(blocks[1].receipts[0].block_hash, blocks[1].header.block_hash);
}
