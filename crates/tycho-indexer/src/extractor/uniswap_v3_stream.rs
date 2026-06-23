#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Block {
    #[prost(bytes = "vec", tag = "1")]
    pub hash: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub parent_hash: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag = "3")]
    pub number: u64,
    #[prost(uint64, tag = "4")]
    pub ts: u64,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Transaction {
    #[prost(bytes = "vec", tag = "1")]
    pub hash: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub from: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub to: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag = "4")]
    pub index: u64,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Events {
    #[prost(message, optional, tag = "1")]
    pub block: ::core::option::Option<Block>,
    #[prost(message, repeated, tag = "3")]
    pub pool_events: ::prost::alloc::vec::Vec<events::PoolEvent>,
}

pub mod events {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct PoolEvent {
        #[prost(uint64, tag = "100")]
        pub log_ordinal: u64,
        #[prost(string, tag = "102")]
        pub pool_address: ::prost::alloc::string::String,
        #[prost(string, tag = "103")]
        pub token0: ::prost::alloc::string::String,
        #[prost(string, tag = "104")]
        pub token1: ::prost::alloc::string::String,
        #[prost(message, optional, tag = "105")]
        pub transaction: ::core::option::Option<super::Transaction>,
        #[prost(oneof = "pool_event::Type", tags = "1, 2, 3, 4, 5, 6, 7, 8, 9")]
        pub r#type: ::core::option::Option<pool_event::Type>,
    }

    pub mod pool_event {
        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Initialize {
            #[prost(string, tag = "1")]
            pub sqrt_price: ::prost::alloc::string::String,
            #[prost(int32, tag = "2")]
            pub tick: i32,
        }

        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Mint {
            #[prost(string, tag = "1")]
            pub sender: ::prost::alloc::string::String,
            #[prost(string, tag = "2")]
            pub owner: ::prost::alloc::string::String,
            #[prost(int32, tag = "3")]
            pub tick_lower: i32,
            #[prost(int32, tag = "4")]
            pub tick_upper: i32,
            #[prost(string, tag = "5")]
            pub amount: ::prost::alloc::string::String,
            #[prost(string, tag = "6")]
            pub amount_0: ::prost::alloc::string::String,
            #[prost(string, tag = "7")]
            pub amount_1: ::prost::alloc::string::String,
        }

        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Collect {
            #[prost(string, tag = "1")]
            pub owner: ::prost::alloc::string::String,
            #[prost(string, tag = "2")]
            pub recipient: ::prost::alloc::string::String,
            #[prost(int32, tag = "3")]
            pub tick_lower: i32,
            #[prost(int32, tag = "4")]
            pub tick_upper: i32,
            #[prost(string, tag = "5")]
            pub amount_0: ::prost::alloc::string::String,
            #[prost(string, tag = "6")]
            pub amount_1: ::prost::alloc::string::String,
        }

        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Burn {
            #[prost(string, tag = "1")]
            pub owner: ::prost::alloc::string::String,
            #[prost(int32, tag = "2")]
            pub tick_lower: i32,
            #[prost(int32, tag = "3")]
            pub tick_upper: i32,
            #[prost(string, tag = "4")]
            pub amount: ::prost::alloc::string::String,
            #[prost(string, tag = "5")]
            pub amount_0: ::prost::alloc::string::String,
            #[prost(string, tag = "6")]
            pub amount_1: ::prost::alloc::string::String,
        }

        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Swap {
            #[prost(string, tag = "1")]
            pub sender: ::prost::alloc::string::String,
            #[prost(string, tag = "2")]
            pub recipient: ::prost::alloc::string::String,
            #[prost(string, tag = "3")]
            pub amount_0: ::prost::alloc::string::String,
            #[prost(string, tag = "4")]
            pub amount_1: ::prost::alloc::string::String,
            #[prost(string, tag = "6")]
            pub sqrt_price: ::prost::alloc::string::String,
            #[prost(string, tag = "7")]
            pub liquidity: ::prost::alloc::string::String,
            #[prost(int32, tag = "8")]
            pub tick: i32,
        }

        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Flash {
            #[prost(string, tag = "1")]
            pub sender: ::prost::alloc::string::String,
            #[prost(string, tag = "2")]
            pub recipient: ::prost::alloc::string::String,
            #[prost(string, tag = "3")]
            pub amount_0: ::prost::alloc::string::String,
            #[prost(string, tag = "4")]
            pub amount_1: ::prost::alloc::string::String,
            #[prost(string, tag = "5")]
            pub paid_0: ::prost::alloc::string::String,
            #[prost(string, tag = "6")]
            pub paid_1: ::prost::alloc::string::String,
        }

        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct SetFeeProtocol {
            #[prost(uint64, tag = "1")]
            pub fee_protocol_0_old: u64,
            #[prost(uint64, tag = "2")]
            pub fee_protocol_1_old: u64,
            #[prost(uint64, tag = "3")]
            pub fee_protocol_0_new: u64,
            #[prost(uint64, tag = "4")]
            pub fee_protocol_1_new: u64,
        }

        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct CollectProtocol {
            #[prost(string, tag = "1")]
            pub sender: ::prost::alloc::string::String,
            #[prost(string, tag = "2")]
            pub recipient: ::prost::alloc::string::String,
            #[prost(string, tag = "3")]
            pub amount_0: ::prost::alloc::string::String,
            #[prost(string, tag = "4")]
            pub amount_1: ::prost::alloc::string::String,
        }

        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct PoolCreated {
            #[prost(uint64, tag = "1")]
            pub fee: u64,
            #[prost(int32, tag = "2")]
            pub tick_spacing: i32,
        }

        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum Type {
            #[prost(message, tag = "1")]
            Initialize(Initialize),
            #[prost(message, tag = "2")]
            Mint(Mint),
            #[prost(message, tag = "3")]
            Collect(Collect),
            #[prost(message, tag = "4")]
            Burn(Burn),
            #[prost(message, tag = "5")]
            Swap(Swap),
            #[prost(message, tag = "6")]
            Flash(Flash),
            #[prost(message, tag = "7")]
            SetFeeProtocol(SetFeeProtocol),
            #[prost(message, tag = "8")]
            CollectProtocol(CollectProtocol),
            #[prost(message, tag = "9")]
            PoolCreated(PoolCreated),
        }
    }
}
