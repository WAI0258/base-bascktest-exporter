use base_backtest_exporter::{
    resolve_protocol, NormalizedProtocol, ProtocolResolution, RawEventFamily, V3SwapPayloadShape,
};

#[test]
fn protocol_registry_maps_supported_v3_family_protocols() {
    let uniswap = resolve_protocol("UniswapV3");
    let pancake = resolve_protocol("PancakeV3");
    let sushi = resolve_protocol("SushiswapV3");
    let aero = resolve_protocol("AerodromeV3");
    let alien = resolve_protocol("AlienV3");

    match uniswap {
        ProtocolResolution::Supported(spec) => {
            assert_eq!(spec.protocol, NormalizedProtocol::UniswapV3);
            assert_eq!(spec.swap_payload_shape, V3SwapPayloadShape::StandardV3);
            assert_eq!(spec.event_family, RawEventFamily::V3);
        }
        ProtocolResolution::Unsupported { .. } => panic!("UniswapV3 should be supported"),
    }
    match pancake {
        ProtocolResolution::Supported(spec) => {
            assert_eq!(spec.protocol, NormalizedProtocol::PancakeV3);
            assert_eq!(spec.swap_payload_shape, V3SwapPayloadShape::PancakeV3);
            assert_eq!(spec.event_family, RawEventFamily::V3);
        }
        ProtocolResolution::Unsupported { .. } => panic!("PancakeV3 should be supported"),
    }
    match sushi {
        ProtocolResolution::Supported(spec) => {
            assert_eq!(spec.protocol, NormalizedProtocol::SushiswapV3);
            assert_eq!(spec.swap_payload_shape, V3SwapPayloadShape::StandardV3);
        }
        ProtocolResolution::Unsupported { .. } => panic!("SushiswapV3 should be supported"),
    }
    match aero {
        ProtocolResolution::Supported(spec) => {
            assert_eq!(spec.protocol, NormalizedProtocol::AerodromeV3);
            assert_eq!(spec.swap_payload_shape, V3SwapPayloadShape::StandardV3);
        }
        ProtocolResolution::Unsupported { .. } => panic!("AerodromeV3 should be supported"),
    }
    match alien {
        ProtocolResolution::Supported(spec) => {
            assert_eq!(spec.protocol, NormalizedProtocol::AlienV3);
            assert_eq!(spec.swap_payload_shape, V3SwapPayloadShape::StandardV3);
        }
        ProtocolResolution::Unsupported { .. } => panic!("AlienV3 should be supported"),
    }
}

#[test]
fn protocol_registry_marks_v2_v4_and_unknown_as_unsupported() {
    assert!(matches!(
        resolve_protocol("UniswapV2"),
        ProtocolResolution::Unsupported { .. }
    ));
    assert!(matches!(
        resolve_protocol("UniswapV4"),
        ProtocolResolution::Unsupported { .. }
    ));
    assert!(matches!(
        resolve_protocol("SomeFutureDexV9"),
        ProtocolResolution::Unsupported { .. }
    ));
}
