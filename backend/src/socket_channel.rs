use dashmap::DashMap;
use std::sync::LazyLock;
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct JoinPayload {}

#[derive(Clone)]
pub enum ChannelMessage {
    Join(JoinPayload),
    Broadcast(String),
}

pub type Sender = tokio::sync::broadcast::Sender<ChannelMessage>;
pub type _Receiver = tokio::sync::broadcast::Receiver<ChannelMessage>;

struct ChannelStoreData {
    tx: Sender,
    member_count: usize,
}

type ChannelStoreType = DashMap<usize, ChannelStoreData>;

static CHANNEL_STORE: LazyLock<ChannelStoreType> = LazyLock::new(|| DashMap::new());

fn create_channel(room_id: usize) -> Sender {
    println!("CREATING CHANNEL");
    let (tx, _) = broadcast::channel::<ChannelMessage>(32);
    CHANNEL_STORE.insert(
        room_id,
        ChannelStoreData {
            tx: tx.clone(),
            member_count: 1,
        },
    );
    tx
}

pub fn join_channel(room_id: usize) -> Sender {
    let entry = CHANNEL_STORE.get_mut(&room_id);
    match entry {
        Some(mut entry) => {
            entry.member_count += 1;
            if entry.tx.send(ChannelMessage::Join(JoinPayload {})).is_err() {
                eprintln!("Failed to send message on existing sender")
            };
            return entry.tx.clone();
        }
        None => {
            return create_channel(room_id);
        }
    }
}

pub fn leave_channel(room_id: usize) {
    println!(
        "DELETING ROOM, total room count(before delete): {}",
        CHANNEL_STORE.len()
    );
    CHANNEL_STORE.remove_if(&room_id, |_, val| {
        return val.member_count <= 1;
    });
    if let Some(mut val) = CHANNEL_STORE.get_mut(&room_id) {
        val.member_count -= 1;
    }
    println!(
        "DELETING ROOM, total room count(after delete): {}",
        CHANNEL_STORE.len()
    );
}
