use {
    crate::repair::{serve_repair::ServeRepair, xdp_sender::RepairXdpSender},
    crossbeam_channel::{Receiver, RecvTimeoutError, Sender, bounded},
    solana_net_utils::SocketAddrSpace,
    solana_perf::{packet::PacketBatch, recycler::Recycler},
    solana_streamer::{
        evicting_sender::EvictingSender,
        streamer::{self, StreamerReceiveStats},
    },
    solana_time_utils::timestamp,
    std::{
        net::UdpSocket,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

pub struct ServeRepairService {
    thread_hdls: Vec<JoinHandle<()>>,
}

/// Repair request channel size. Grossly overprovisioned compared to actual needs (~1024 would be sufficient).
pub(crate) const REQUEST_CHANNEL_SIZE: usize = 4096;

/// Repair response channel size. Grossly overprovisioned compared to actual needs (~256 would be sufficient).
pub(crate) const RESPONSE_CHANNEL_SIZE: usize = REQUEST_CHANNEL_SIZE;

impl ServeRepairService {
    pub(crate) fn new(
        serve_repair: ServeRepair,
        serve_repair_socket: UdpSocket,
        socket_addr_space: SocketAddrSpace,
        stats_reporter_sender: Sender<Box<dyn FnOnce() + Send>>,
        repair_xdp_sender: Option<RepairXdpSender>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let (request_sender, request_receiver) = EvictingSender::new_bounded(REQUEST_CHANNEL_SIZE);
        let serve_repair_socket = Arc::new(serve_repair_socket);
        let t_receiver = streamer::receiver(
            "solRcvrServeRep".to_string(),
            serve_repair_socket.clone(),
            exit.clone(),
            request_sender,
            Recycler::default(),
            Arc::new(StreamerReceiveStats::new("serve_repair_receiver")),
            Some(Duration::from_millis(1)), // coalesce
            false,                          // use_pinned_memory
            false,                          // is_staked_service
        );
        let (response_sender, response_receiver) = bounded(RESPONSE_CHANNEL_SIZE);
        // Egress: prefer XDP when a sender is configured; otherwise fall back to
        // the legacy kernel UDP responder.
        let t_responder = if let Some(xdp_sender) = repair_xdp_sender {
            spawn_xdp_responder(
                xdp_sender,
                response_receiver,
                socket_addr_space,
                exit.clone(),
            )
        } else {
            streamer::responder(
                "Repair",
                serve_repair_socket,
                response_receiver,
                socket_addr_space,
                Some(stats_reporter_sender),
            )
        };
        let t_listen = serve_repair.listen(request_receiver, response_sender, exit);

        let thread_hdls = vec![t_receiver, t_responder, t_listen];
        Self { thread_hdls }
    }

    pub(crate) fn join(self) -> thread::Result<()> {
        self.thread_hdls.into_iter().try_for_each(JoinHandle::join)
    }
}

#[derive(Default)]
struct RepairXdpResponderStats {
    sent: AtomicU64,
    send_failures: AtomicU64,
    dropped_addr_space: AtomicU64,
}

impl RepairXdpResponderStats {
    fn report_and_reset(&self) {
        let sent = self.sent.swap(0, Ordering::Relaxed);
        let send_failures = self.send_failures.swap(0, Ordering::Relaxed);
        let dropped_addr_space = self.dropped_addr_space.swap(0, Ordering::Relaxed);
        if sent | send_failures | dropped_addr_space == 0 {
            return;
        }
        solana_metrics::datapoint_info!(
            "repair_xdp_responder",
            ("sent", sent as i64, i64),
            ("send_failures", send_failures as i64, i64),
            ("dropped_addr_space", dropped_addr_space as i64, i64),
        );
    }
}

const STATS_REPORT_INTERVAL: Duration = Duration::from_secs(2);

fn spawn_xdp_responder(
    xdp_sender: RepairXdpSender,
    receiver: Receiver<PacketBatch>,
    socket_addr_space: SocketAddrSpace,
    exit: Arc<AtomicBool>,
) -> JoinHandle<()> {
    Builder::new()
        .name("solRspndrRepairXdp".to_string())
        .spawn(move || {
            xdp_responder_loop(xdp_sender, receiver, socket_addr_space, exit);
        })
        .unwrap()
}

fn xdp_responder_loop(
    xdp_sender: RepairXdpSender,
    receiver: Receiver<PacketBatch>,
    socket_addr_space: SocketAddrSpace,
    exit: Arc<AtomicBool>,
) {
    let stats = RepairXdpResponderStats::default();
    let mut last_report = Instant::now();
    // Rotates TX-channel index across packets so we spread egress across
    // the underlying `agave_xdp` TX threads (matches turbine's retransmit
    // load-spreading).
    let mut sender_index: usize = 0;
    let mut last_log_errors = 0u64;
    let mut last_log_ts = timestamp();

    while !exit.load(Ordering::Relaxed) {
        match receiver.recv_timeout(Duration::from_secs(1)) {
            Ok(batch) => {
                for pkt in batch.iter() {
                    let addr = pkt.meta().socket_addr();
                    if !socket_addr_space.check(&addr) {
                        stats.dropped_addr_space.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    // `PacketRef::to_bytes_packet` handles both legacy `Packet`
                    // and `BytesPacket` variants. For `BytesPacket` this is a
                    // cheap `Bytes` refcount bump; for legacy `Packet` it copies
                    // the on-wire bytes (one copy, acceptable for repair).
                    let bp = pkt.to_bytes_packet();
                    let payload = bp.buffer().clone();
                    let idx = sender_index;
                    sender_index = sender_index.wrapping_add(1);
                    if let Err(e) = xdp_sender.try_send(idx, addr, payload) {
                        let prev = stats.send_failures.fetch_add(1, Ordering::Relaxed);
                        let now = timestamp();
                        // Rate-limit log spam (matches turbine's xdp drop log behavior).
                        if now.saturating_sub(last_log_ts) > 1000
                            && prev != last_log_errors
                        {
                            log::warn!("repair xdp responder channel full: {e:?}");
                            last_log_errors = prev;
                            last_log_ts = now;
                        }
                    } else {
                        stats.sent.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                stats.report_and_reset();
                return;
            }
        }
        if last_report.elapsed() >= STATS_REPORT_INTERVAL {
            stats.report_and_reset();
            last_report = Instant::now();
        }
    }
    stats.report_and_reset();
}
