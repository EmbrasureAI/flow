use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::{Error, Statement, query, simple_query, slice_iter};
use bytes::Bytes;
use futures_util::Stream;
use log::debug;
use pin_project_lite::pin_project;
use postgres_protocol::message::backend::Message;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

// A COPY response can contain an arbitrarily large row. Keep only one response
// buffer queued so downstream polling provides byte-oriented backpressure.
const RESPONSE_CHANNEL_CAPACITY: usize = 1;

pub async fn copy_out_simple(client: &InnerClient, query: &str) -> Result<CopyOutStream, Error> {
    debug!("executing copy out query {}", query);

    let buf = simple_query::encode(client, query)?;
    let responses = start(client, buf, true).await?;
    Ok(CopyOutStream { responses })
}

pub async fn copy_out(client: &InnerClient, statement: Statement) -> Result<CopyOutStream, Error> {
    debug!("executing copy out statement {}", statement.name());

    let buf = query::encode(client, &statement, slice_iter(&[]))?;
    let responses = start(client, buf, false).await?;
    Ok(CopyOutStream { responses })
}

async fn start(client: &InnerClient, buf: Bytes, simple: bool) -> Result<Responses, Error> {
    let mut responses = client.send_with_capacity(
        RequestMessages::Single(FrontendMessage::Raw(buf)),
        RESPONSE_CHANNEL_CAPACITY,
    )?;

    if !simple {
        match responses.next().await? {
            Message::BindComplete => {}
            _ => return Err(Error::unexpected_message()),
        }
    }

    match responses.next().await? {
        Message::CopyOutResponse(_) => {}
        _ => return Err(Error::unexpected_message()),
    }

    Ok(responses)
}

pin_project! {
    /// A stream of `COPY ... TO STDOUT` query data.
    #[project(!Unpin)]
    pub struct CopyOutStream {
        responses: Responses,
    }
}

impl Stream for CopyOutStream {
    type Item = Result<Bytes, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match ready!(this.responses.poll_next(cx)?) {
            Message::CopyData(body) => Poll::Ready(Some(Ok(body.into_bytes()))),
            Message::CopyDone => Poll::Ready(None),
            _ => Poll::Ready(Some(Err(Error::unexpected_message()))),
        }
    }
}
