use {
    agave_xdp::transmitter as tx, bytes::Bytes, crossbeam_channel::TrySendError,
    std::net::SocketAddrV4,
};

/// Convenience wrapper around [`tx::XdpSender`] for the repair path.
///
/// Like turbine, repair always sends from a fixed source address, so we store
/// it once and attach it to every packet automatically.
#[derive(Clone)]
pub struct RepairXdpSender {
    sender: tx::XdpSender,
    src_addr: SocketAddrV4,
}

impl RepairXdpSender {
    pub fn new(sender: tx::XdpSender, src_addr: SocketAddrV4) -> Self {
        Self { sender, src_addr }
    }

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
