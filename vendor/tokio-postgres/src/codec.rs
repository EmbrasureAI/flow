use bytes::{Buf, Bytes, BytesMut};
use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend;
use postgres_protocol::message::frontend::CopyData;
use std::io;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio_util::codec::{Decoder, Encoder};

pub enum FrontendMessage {
    Raw(Bytes),
    CopyData(CopyData<Box<dyn Buf + Send>>),
}

pub enum BackendMessage {
    Normal {
        messages: BackendMessages,
        request_complete: bool,
    },
    Async(backend::Message),
}

pub struct BackendMessages(BytesMut);

impl BackendMessages {
    pub fn empty() -> BackendMessages {
        BackendMessages(BytesMut::new())
    }
}

impl FallibleIterator for BackendMessages {
    type Item = backend::Message;
    type Error = io::Error;

    fn next(&mut self) -> io::Result<Option<backend::Message>> {
        backend::Message::parse(&mut self.0)
    }
}

// Connection errors normally reach the driver only. Preserve admission errors
// for user response handles when dropping that driver closes their channels.
#[derive(Clone, Default)]
pub(crate) struct BackendMessageLimit(Arc<AtomicBool>);

impl BackendMessageLimit {
    pub(crate) fn exceeded(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    fn error() -> io::Error {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "PostgreSQL backend message exceeds configured byte limit",
        )
    }

    fn reject(&self) -> io::Error {
        self.0.store(true, Ordering::Relaxed);
        Self::error()
    }

    pub(crate) fn closed_error(&self) -> crate::Error {
        if self.exceeded() {
            crate::Error::io(Self::error())
        } else {
            crate::Error::closed()
        }
    }
}

pub struct PostgresCodec {
    pub max_backend_message_bytes: Option<usize>,
    pub(crate) message_limit: BackendMessageLimit,
}

impl Encoder<FrontendMessage> for PostgresCodec {
    type Error = io::Error;

    fn encode(&mut self, item: FrontendMessage, dst: &mut BytesMut) -> io::Result<()> {
        match item {
            FrontendMessage::Raw(buf) => dst.extend_from_slice(&buf),
            FrontendMessage::CopyData(data) => data.write(dst),
        }

        Ok(())
    }
}

impl Decoder for PostgresCodec {
    type Item = BackendMessage;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<BackendMessage>, io::Error> {
        let mut idx = 0;
        let mut request_complete = false;

        while let Some(header) = backend::Header::parse(&src[idx..])? {
            let payload_len = header.len() as usize - 4;
            if self
                .max_backend_message_bytes
                .is_some_and(|limit| payload_len > limit)
            {
                return Err(self.message_limit.reject());
            }
            let len = header.len() as usize + 1;
            if src[idx..].len() < len {
                break;
            }

            match header.tag() {
                backend::NOTICE_RESPONSE_TAG
                | backend::NOTIFICATION_RESPONSE_TAG
                | backend::PARAMETER_STATUS_TAG => {
                    if idx == 0 {
                        let message = backend::Message::parse(src)?.unwrap();
                        return Ok(Some(BackendMessage::Async(message)));
                    } else {
                        break;
                    }
                }
                _ => {}
            }

            idx += len;

            if header.tag() == backend::READY_FOR_QUERY_TAG {
                request_complete = true;
                break;
            }
        }

        if idx == 0 {
            Ok(None)
        } else {
            Ok(Some(BackendMessage::Normal {
                messages: BackendMessages(src.split_to(idx)),
                request_complete,
            }))
        }
    }
}
