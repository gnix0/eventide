#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerRecord {
    pub worker_id: String,
    pub endpoint: String,
    pub availability_zone: String,
    pub max_assignments: u16,
    pub labels: Vec<String>,
    pub status: WorkerStatus,
    pub active_assignments: u16,
    pub registered_at_epoch_secs: u64,
    pub last_heartbeat_at_epoch_secs: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerStatus {
    Ready,
    Draining,
    Expired,
}
