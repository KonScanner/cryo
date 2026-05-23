use super::collect_generic::{fetch_partition, join_partition_handles};
use crate::{CollectError, Datatype, Params, Partition, Query, Source, ToDataFrames};
use polars::prelude::*;
use std::{collections::HashMap, future::Future};
use tokio::sync::mpsc;

type R<T> = ::core::result::Result<T, CollectError>;

/// defines how to collect dataset by transaction
///
/// See [`super::collect_by_block::CollectByBlock`] for the rationale on the
/// native AFIT migration with explicit `+ Send` bounds.
pub trait CollectByTransaction: 'static + Send + Default + ToDataFrames {
    /// type of transaction data responses
    type Response: Send;

    /// fetch dataset data by transaction
    fn extract(
        _request: Params,
        _source: Arc<Source>,
        _query: Arc<Query>,
    ) -> impl Future<Output = R<Self::Response>> + Send {
        async {
            Err(CollectError::CollectError("CollectByTransaction not implemented".to_string()))
        }
    }

    /// transform block data response into column data
    fn transform(_response: Self::Response, _columns: &mut Self, _query: &Arc<Query>) -> R<()> {
        Err(CollectError::CollectError("CollectByTransaction not implemented".to_string()))
    }

    /// collect data into DataFrame
    fn collect_by_transaction(
        partition: Partition,
        source: Arc<Source>,
        query: Arc<Query>,
        inner_request_size: Option<u64>,
    ) -> impl Future<Output = R<HashMap<Datatype, DataFrame>>> + Send {
        async move {
            let (sender, receiver) = mpsc::channel(1);
            let chain_id = source.chain_id;
            let handles = fetch_partition(
                Self::extract,
                partition,
                source,
                inner_request_size,
                query.clone(),
                sender,
            )
            .await?;
            let columns = Self::transform_channel(receiver, &query).await?;
            join_partition_handles(handles).await?;
            columns.create_dfs(&query.schemas, chain_id)
        }
    }

    /// convert transaction-derived data to dataframe
    fn transform_channel<'a>(
        mut receiver: mpsc::Receiver<R<Self::Response>>,
        query: &'a Arc<Query>,
    ) -> impl Future<Output = R<Self>> + Send + 'a {
        async move {
            let mut columns = Self::default();
            while let Some(message) = receiver.recv().await {
                match message {
                    Ok(message) => Self::transform(message, &mut columns, query)?,
                    Err(e) => return Err(e),
                }
            }
            Ok(columns)
        }
    }

    /// whether data can be collected by transaction
    fn can_collect_by_transaction() -> bool {
        std::any::type_name::<Self::Response>() != "()"
    }
}
