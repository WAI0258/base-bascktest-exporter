use crate::contract::V3SwapPayloadShape;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NormalizedProtocol {
    UniswapV3,
    PancakeV3,
    SushiswapV3,
    AerodromeV3,
    AlienV3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawEventFamily {
    V3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupportedProtocolSpec {
    pub protocol: NormalizedProtocol,
    pub swap_payload_shape: V3SwapPayloadShape,
    pub event_family: RawEventFamily,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolResolution {
    Supported(SupportedProtocolSpec),
    Unsupported { input: String },
}

pub fn resolve_protocol(protocol: &str) -> ProtocolResolution {
    let trimmed = protocol.trim();
    let normalized = trimmed.to_ascii_lowercase();
    let supported = match normalized.as_str() {
        "uniswapv3" | "uniswap_v3" => Some(SupportedProtocolSpec {
            protocol: NormalizedProtocol::UniswapV3,
            swap_payload_shape: V3SwapPayloadShape::StandardV3,
            event_family: RawEventFamily::V3,
        }),
        "pancakev3" | "pancakeswapv3" | "pancakeswap_v3" => Some(SupportedProtocolSpec {
            protocol: NormalizedProtocol::PancakeV3,
            swap_payload_shape: V3SwapPayloadShape::PancakeV3,
            event_family: RawEventFamily::V3,
        }),
        "sushiswapv3" | "sushi_v3" | "sushiswap_v3" => Some(SupportedProtocolSpec {
            protocol: NormalizedProtocol::SushiswapV3,
            swap_payload_shape: V3SwapPayloadShape::StandardV3,
            event_family: RawEventFamily::V3,
        }),
        "aerodromev3" | "aerodrome_v3" | "slipstream" => Some(SupportedProtocolSpec {
            protocol: NormalizedProtocol::AerodromeV3,
            swap_payload_shape: V3SwapPayloadShape::StandardV3,
            event_family: RawEventFamily::V3,
        }),
        "alienv3" | "alien_v3" => Some(SupportedProtocolSpec {
            protocol: NormalizedProtocol::AlienV3,
            swap_payload_shape: V3SwapPayloadShape::StandardV3,
            event_family: RawEventFamily::V3,
        }),
        _ => None,
    };
    match supported {
        Some(spec) => ProtocolResolution::Supported(spec),
        None => ProtocolResolution::Unsupported {
            input: trimmed.to_owned(),
        },
    }
}
