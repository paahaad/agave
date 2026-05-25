//! Fixed-source XDP sender used by [`crate::repair::serve_repair_service`]
//! to push repair *response* packets onto the shared `agave_xdp` transmitter.
//!
//! Repair responses always egress from a single UDP source address
//! (the local `serve_repair_socket`), so we store it once and attach it to
//! every packet automatically — mirroring `solana_turbine::XdpSender`.
use {
    agave_xdp::transmitter as tx, bytes::Bytes, crossbeam_channel::TrySendError,
    std::net::SocketAddrV4,
};

#[derive(Clone)]
pub struct RepairXdpSender {
    sender: tx::XdpSender,
    src_addr: SocketAddrV4,
}

impl RepairXdpSender {
    pub fn new(sender: tx::XdpSender, src_addr: SocketAddrV4) -> Self {
        Self { sender, src_addr }
    }

    /// Enqueue a single repair response packet on the XDP TX channel selected
    /// by `sender_index % num_tx_threads`. Returns `TrySendError` when the
    /// channel is full.
    #[inline]
    pub fn try_send(
        &self,
        sender_index: usize,
        addr: impl Into<tx::XdpAddrs>,
        payload: Bytes,
    ) -> Result<(), TrySendError<tx::BytesTxPacket>> {
        self.sender.try_send(
            sender_index,
            tx::BytesTxPacket::new(self.src_addr, addr, None, payload),
        )
    }
}
