use std::{fs, path::PathBuf};

use base_backtest_exporter::contract::{
    current_lpbot_base_accepts_swap_word_count, detect_v3_swap_payload_shape,
    protocol_capabilities, validate_current_raw_line, validate_pool_manifest_str,
    validate_stable_token_list_str, validate_state_line_str, validate_target_raw_line,
    validate_unresolved_stable_side_report_str, EventContractStatus, V3SwapPayloadShape,
};

fn fixture(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(path)
}

fn read_fixture(path: &str) -> String {
    let file = fixture(path);
    match fs::read_to_string(&file) {
        Ok(contents) => contents,
        Err(err) => panic!("failed to read fixture {}: {err}", file.display()),
    }
}

#[test]
fn standard_v3_raw_fixture_is_a_valid_target_sample() {
    let contents = read_fixture("raw/standard_v3_valid.json");
    let raw = match validate_target_raw_line(&contents) {
        Ok(raw) => raw,
        Err(err) => panic!("expected valid standard V3 raw fixture: {err}"),
    };
    let shape = match detect_v3_swap_payload_shape(&raw.data) {
        Ok(shape) => shape,
        Err(err) => panic!("expected standard V3 payload shape: {err}"),
    };
    assert_eq!(shape, V3SwapPayloadShape::StandardV3);
}

#[test]
fn missing_block_timestamp_is_rejected_by_target_contract_but_not_current_view() {
    let contents = read_fixture("raw/missing_block_timestamp.json");

    match validate_current_raw_line(&contents) {
        Ok(_) => {}
        Err(err) => panic!("current view should accept missing blockTimestamp: {err}"),
    }

    let err = match validate_target_raw_line(&contents) {
        Ok(_) => panic!("target contract must reject missing blockTimestamp"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("blockTimestamp"),
        "unexpected error: {err}"
    );
}

#[test]
fn pancake_v3_raw_fixture_is_supported_by_target_contract_and_flags_decoder_gap() {
    let contents = read_fixture("raw/pancake_v3_valid.json");
    let raw = match validate_target_raw_line(&contents) {
        Ok(raw) => raw,
        Err(err) => panic!("expected valid PancakeV3 raw fixture: {err}"),
    };

    let shape = match detect_v3_swap_payload_shape(&raw.data) {
        Ok(shape) => shape,
        Err(err) => panic!("expected PancakeV3 payload shape: {err}"),
    };
    assert_eq!(shape, V3SwapPayloadShape::PancakeV3);
    assert!(
        !current_lpbot_base_accepts_swap_word_count(7),
        "current lpbot-base decoder should still reject 7-word swap payloads"
    );

    let pancake = protocol_capabilities()
        .iter()
        .find(|entry| entry.protocol == "PancakeV3");
    match pancake {
        Some(entry) => {
            assert_eq!(
                entry.swap,
                EventContractStatus::AcceptedWithLpbotBaseDecoderExtension
            );
            assert_eq!(entry.mint, EventContractStatus::Accepted);
            assert_eq!(entry.burn, EventContractStatus::Accepted);
            assert_eq!(entry.collect, EventContractStatus::Accepted);
        }
        None => panic!("expected PancakeV3 capability entry"),
    }
}

#[test]
fn state_line_contract_accepts_valid_shape_and_rejects_invalid_shape() {
    let valid = read_fixture("state/valid_state_line.json");
    match validate_state_line_str(&valid) {
        Ok(_) => {}
        Err(err) => panic!("expected valid state line fixture: {err}"),
    }

    let missing_field = read_fixture("state/missing_token1_balance_raw.json");
    match validate_state_line_str(&missing_field) {
        Ok(_) => panic!("state fixture missing token1_balance_raw must fail"),
        Err(err) => {
            assert!(
                err.to_string().contains("token1_balance_raw"),
                "unexpected error: {err}"
            );
        }
    }

    let wrong_type = read_fixture("state/invalid_block_number_type.json");
    match validate_state_line_str(&wrong_type) {
        Ok(_) => panic!("state fixture with invalid block_number type must fail"),
        Err(err) => {
            assert!(
                err.to_string().contains("block_number"),
                "unexpected error: {err}"
            );
        }
    }
}

#[test]
fn pool_manifest_contract_requires_decimals_fee_tier_and_stable_flags() {
    let valid = read_fixture("pool_manifest/valid_minimal.json");
    let manifest = match validate_pool_manifest_str(&valid) {
        Ok(manifest) => manifest,
        Err(err) => panic!("expected valid pool manifest fixture: {err}"),
    };
    assert_eq!(manifest.version, 1);
    assert_eq!(manifest.pools.len(), 1);

    let invalid = read_fixture("pool_manifest/missing_required_fields.json");
    let err = match validate_pool_manifest_str(&invalid) {
        Ok(_) => panic!("pool manifest missing required fields must fail"),
        Err(err) => err,
    };
    let message = err.to_string();
    assert!(
        message.contains("decimals")
            || message.contains("token1")
            || message.contains("fee_tier")
            || message.contains("token1_is_stable"),
        "unexpected error: {message}"
    );
}

#[test]
fn pool_manifest_contract_rejects_unknown_version() {
    let invalid_version = read_fixture("pool_manifest/invalid_version.json");
    let err = match validate_pool_manifest_str(&invalid_version) {
        Ok(_) => panic!("pool manifest with unknown version must fail"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("version"),
        "unexpected error: {err}"
    );
}

#[test]
fn stable_token_list_contract_validates_required_fields_and_address_format() {
    let valid = read_fixture("stable_tokens/valid_minimal.json");
    let stable_tokens = match validate_stable_token_list_str(&valid) {
        Ok(stable_tokens) => stable_tokens,
        Err(err) => panic!("expected valid stable token list fixture: {err}"),
    };
    assert_eq!(stable_tokens.version, 1);
    assert_eq!(stable_tokens.tokens.len(), 2);

    let missing_required = read_fixture("stable_tokens/missing_required_fields.json");
    let err = match validate_stable_token_list_str(&missing_required) {
        Ok(_) => panic!("stable token list missing required fields must fail"),
        Err(err) => err,
    };
    let message = err.to_string();
    assert!(
        message.contains("symbol") || message.contains("name") || message.contains("address"),
        "unexpected error: {message}"
    );

    let invalid_address = read_fixture("stable_tokens/invalid_address.json");
    let err = match validate_stable_token_list_str(&invalid_address) {
        Ok(_) => panic!("stable token list with invalid address must fail"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("address"),
        "unexpected error: {err}"
    );
}

#[test]
fn unresolved_stable_side_report_contract_validates_required_fields() {
    let valid = read_fixture("unresolved_stable_side_report/valid_minimal.json");
    let report = match validate_unresolved_stable_side_report_str(&valid) {
        Ok(report) => report,
        Err(err) => panic!("expected valid unresolved report fixture: {err}"),
    };
    assert_eq!(report.version, 1);
    assert_eq!(report.items.len(), 1);

    let missing_required = read_fixture("unresolved_stable_side_report/missing_required_fields.json");
    let err = match validate_unresolved_stable_side_report_str(&missing_required) {
        Ok(_) => panic!("unresolved stable-side report missing required fields must fail"),
        Err(err) => err,
    };
    let message = err.to_string();
    assert!(
        message.contains("pool_address") || message.contains("reason"),
        "unexpected error: {message}"
    );
}
