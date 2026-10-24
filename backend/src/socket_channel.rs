use dashmap::DashMap;
use std::sync::OnceLock;
use tokio::sync::broadcast;

#[derive(Clone)]
struct JoinPayload {}

#[derive(Clone)]
enum ChannelMessage {
    Join(JoinPayload),
}

pub type Sender = tokio::sync::broadcast::Sender<ChannelMessage>;
pub type Receiver = tokio::sync::broadcast::Receiver<ChannelMessage>;

struct ChannelStoreData {
    tx: Sender,
    member_count: usize,
}

type ChannelStoreType = DashMap<usize, ChannelStoreData>;

static CHANNEL_STORE: OnceLock<ChannelStoreType> = OnceLock::new();

pub fn get_channel_store() -> &'static ChannelStoreType {
    CHANNEL_STORE.get_or_init(|| DashMap::new())
}

pub fn create_channel(room_id: usize) -> Sender {
    let (tx, rx) = broadcast::channel::<ChannelMessage>(32);
    let store = get_channel_store();
    store.insert(
        room_id,
        ChannelStoreData {
            tx: tx.clone(),
            member_count: 1,
        },
    );
    tx
}
