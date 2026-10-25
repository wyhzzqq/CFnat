use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use tokio::task::JoinHandle;

use crate::core::backend::Backend;
use crate::core::cancel::CancellationToken;
use crate::core::config::get_global_config;
use crate::core::httping::{PingConfig, PingResultDetail};
use crate::log::push_log;

/// 连续失败次数达到该值时判定为超限
const MAX_CONSECUTIVE_FAILURES: u32 = 3;

/// 主队列健康检查间隔相对于备选队列的倍数
const PRIMARY_CHECK_INTERVAL_MULT: u32 = 4;

/// 主队列活跃下限与备选队列容量均按 primary_target 的一半推导
const HALF_TARGET_DIVISOR: f32 = 2.0;

/// 备选队列容量下限
const MIN_BACKUP_TARGET: usize = 2;

/// 构造默认的 Sticky 槽位数（实际值由 ServiceConfig 传入）
const DEFAULT_MAX_STICKY_SLOTS: usize = 5;

/// 健康检查 JoinSet 中单个任务的产出
type CheckTaskOutput = Result<(Arc<Backend>, Option<PingResultDetail>), tokio::task::JoinError>;

/// 墙上时钟毫秒：只用于给用户展示的时刻，不参与任何时长判定
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// 单调毫秒的零点，进程内首次取用时固定
static MONO_BASE: OnceLock<Instant> = OnceLock::new();

/// 单调毫秒：TTL 等"持续时长"判定用它，语义是有效运行时长；
/// 墙上时钟会被系统挂起与校时推动，用它算时长会让恢复后的判断失真
fn mono_ms() -> u64 {
    MONO_BASE.get_or_init(Instant::now).elapsed().as_millis() as u64
}

/// TTL 到期判定。要求两个时刻同源（都用 mono_ms）：混用挂钟值时
/// 差值会被 saturating_sub 饱和成 0，槽会被误判为永不过期
#[inline]
fn slot_expired(current_ms: u64, last_access_ms: u64, ttl_ms: u64) -> bool {
    current_ms.saturating_sub(last_access_ms) >= ttl_ms
}

#[derive(Debug)]
struct StickySlot {
    backend: Arc<Backend>,
    last_switch: Instant,
    /// 单调毫秒（mono_ms），与展示用的墙上时钟是两个时钟
    last_access_ms: AtomicU64,
    interval: Duration,
}

#[derive(Debug)]
struct BalancerInner {
    primary: Vec<Arc<Backend>>,
    backup: Vec<Arc<Backend>>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum PoolType {
    Primary,
    Backup,
}

impl PoolType {
    fn as_str(self) -> &'static str {
        match self {
            PoolType::Primary => "主队列",
            PoolType::Backup => "备选",
        }
    }
}

#[derive(Debug)]
pub struct LoadBalancer {
    inner: RwLock<BalancerInner>,
    ip_set: RwLock<HashSet<std::net::IpAddr>>,
    primary_index: AtomicUsize,
    primary_target: AtomicUsize,
    backup_target: AtomicUsize,
    min_active_target: usize,
    health_check_url: String,
    tls_port: u16,
    http_port: u16,
    timeout_ms: u64,
    notify_tx: Option<tokio::sync::watch::Sender<bool>>,
    delay_threshold: AtomicU32,
    loss_threshold: AtomicU32,
    client: RwLock<Option<Arc<crate::core::hyper::MyHyperClient>>>,
    colo_filter: Option<Arc<Vec<String>>>,
    sticky_slots: RwLock<Vec<StickySlot>>,
    last_expand_ms: AtomicU64,
    cancel_token: CancellationToken,
    next_health_check_ms: AtomicU64,
    max_sticky_slots: usize,
}

pub enum AddResult {
    AddedToPrimary,
    AddedToBackup,
    QueueFull,
    AlreadyExists,
}

impl LoadBalancer {
    pub fn new(primary_target: usize) -> Self {
        let half = (primary_target as f32 / HALF_TARGET_DIVISOR).ceil() as usize;
        let backup_target = half.min(get_global_config().max_backup_target).max(MIN_BACKUP_TARGET);
        let min_active_target = half;
        Self {
            inner: RwLock::new(BalancerInner {
                primary: Vec::new(),
                backup: Vec::new(),
            }),
            ip_set: RwLock::new(HashSet::new()),
            primary_index: AtomicUsize::new(0),
            primary_target: AtomicUsize::new(primary_target),
            backup_target: AtomicUsize::new(backup_target),
            min_active_target,
            health_check_url: String::new(),
            tls_port: 443,
            http_port: 80,
            timeout_ms: 2000,
            notify_tx: None,
            delay_threshold: AtomicU32::new(0.0f32.to_bits()),
            loss_threshold: AtomicU32::new(0.0f32.to_bits()),
            client: RwLock::new(None),
            colo_filter: None,
            sticky_slots: RwLock::new(Vec::new()),
            // 与 maintain_sticky_slots 里的比较同源，必须用单调毫秒
            last_expand_ms: AtomicU64::new(mono_ms()),
            cancel_token: CancellationToken::new(),
            // 仅用于界面倒计时，走墙上时钟
            next_health_check_ms: AtomicU64::new(now_ms()),
            max_sticky_slots: DEFAULT_MAX_STICKY_SLOTS,
        }
    }

    pub fn try_add_backend(
        &self,
        addr: SocketAddr,
        delay: f32,
        colo: Option<&str>,
    ) -> AddResult {
        if self.contains(addr.ip()) {
            return AddResult::AlreadyExists;
        }
        
        let primary_count = self.get_primary_count();
        let backup_count = self.get_backup_count();
        
        let primary_target = self.primary_target.load(Ordering::Relaxed);
        let backup_target = self.backup_target.load(Ordering::Relaxed);
        
        let colo_string = colo.map(|s| s.to_string());
        
        if primary_count < primary_target {
            self.add_to_primary(addr, delay, 0.0, colo_string);
            push_log("INFO", &format!("[+] {} {:.0}ms [{}]", addr, delay, colo.unwrap_or("")));
            AddResult::AddedToPrimary
        } else if backup_count < backup_target {
            self.add_to_backup(addr, delay, 0.0, colo_string);
            push_log("INFO", &format!("[+] {} {:.0}ms [{}] (备选)", addr, delay, colo.unwrap_or("")));
            AddResult::AddedToBackup
        } else {
            AddResult::QueueFull
        }
    }

    pub fn with_delay_threshold(self, delay_threshold: f32) -> Self {
        self.delay_threshold.store(delay_threshold.to_bits(), Ordering::Relaxed);
        self
    }

    pub fn with_loss_threshold(self, loss_threshold: f32) -> Self {
        self.loss_threshold.store(loss_threshold.to_bits(), Ordering::Relaxed);
        self
    }

    pub fn with_colo_filter(mut self, colo_filter: Option<Vec<String>>) -> Self {
        self.colo_filter = colo_filter.map(Arc::new);
        self
    }

    pub fn with_health_check_url(mut self, url: String) -> Self {
        self.health_check_url = url;
        self
    }

    pub fn with_ports(mut self, tls_port: u16, http_port: u16) -> Self {
        self.tls_port = tls_port;
        self.http_port = http_port;
        self
    }

    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    pub fn with_notify(mut self, tx: tokio::sync::watch::Sender<bool>) -> Self {
        self.notify_tx = Some(tx);
        self
    }

    pub(crate) fn with_client(self, client: Arc<crate::core::hyper::MyHyperClient>) -> Self {
        *self.client.write() = Some(client);
        self
    }

    pub fn rebuild_client(&self) {
        *self.client.write() = Some(Arc::new(crate::core::hyper::build_hyper_client(self.timeout_ms)));
        push_log("INFO", "[重建] HTTP 客户端已重建");
    }

    pub(crate) fn current_client(&self) -> Option<Arc<crate::core::hyper::MyHyperClient>> {
        self.client.read().clone()
    }

    pub fn reset_all_metrics(&self) {
        let inner = self.inner.read();
        for backend in inner.primary.iter() {
            backend.reset_metrics();
        }
        for backend in inner.backup.iter() {
            backend.reset_metrics();
        }
    }

    pub fn with_max_sticky_slots(mut self, max_sticky_slots: usize) -> Self {
        self.max_sticky_slots = max_sticky_slots;
        self
    }

    pub fn contains(&self, ip: std::net::IpAddr) -> bool {
        self.ip_set.read().contains(&ip)
    }

    pub fn primary_full(&self) -> bool {
        self.get_primary_count() >= self.primary_target.load(Ordering::Relaxed)
    }

    pub fn backup_full(&self) -> bool {
        self.get_backup_count() >= self.backup_target.load(Ordering::Relaxed)
    }

    pub fn should_pause(&self) -> bool {
        self.primary_full() && self.backup_full()
    }

    pub fn notify_resume(&self) {
        if let Some(tx) = &self.notify_tx {
            let _ = tx.send(true);
        }
    }

    pub fn select(&self) -> Option<Arc<Backend>> {
        let slots = self.sticky_slots.read();
        // 只从仍可用的槽中选取；已移除/隔离等不可用节点跳过，交给池路径处理
        let usable: Vec<&StickySlot> = slots.iter().filter(|s| s.backend.is_selectable()).collect();

        if !usable.is_empty() {
            return self.select_from_slots(&usable, usable.len());
        }
        drop(slots);

        let inner = self.inner.read();
        if !inner.primary.is_empty() {
            return self.select_from_pool(&inner.primary, &self.primary_index);
        }
        if !inner.backup.is_empty() {
            return self.select_from_pool(&inner.backup, &self.primary_index);
        }
        None
    }

    fn select_from_slots(&self, slots: &[&StickySlot], len: usize) -> Option<Arc<Backend>> {
        let current_ms = mono_ms();
        let on_select = |slot: &StickySlot| {
            slot.last_access_ms.store(current_ms, Ordering::Release);
        };

        if len == 1 {
            on_select(slots[0]);
            slots[0].backend.fetch_add_connection(1);
            return Some(slots[0].backend.clone());
        }

        Self::select_by_connection(slots, &self.primary_index, |s| &s.backend, |s| s.backend.clone(), |s: &&StickySlot| on_select(s))
    }

    fn select_from_pool(&self, pool: &[Arc<Backend>], index: &AtomicUsize) -> Option<Arc<Backend>> {
        // 优先选正常后端，没有则保底选隔离的
        let selectable: Vec<&Arc<Backend>> = pool.iter()
            .filter(|b| b.is_selectable())
            .collect();
        let fallback: Vec<&Arc<Backend>> = pool.iter()
            .filter(|b| b.is_isolated())
            .collect();

        let candidates = if !selectable.is_empty() { &selectable } else { &fallback };
        let len = candidates.len();
        if len == 0 {
            return None;
        }
        
        if len == 1 {
            candidates[0].fetch_add_connection(1);
            return Some(candidates[0].clone());
        }
        
        Self::select_by_connection(candidates, index, |b| b.as_ref(), |b| (*b).clone(), |_| {})
    }

    fn select_by_connection<T>(
        items: &[T],
        index: &AtomicUsize,
        get_backend: impl Fn(&T) -> &Backend,
        get_arc: impl Fn(&T) -> Arc<Backend>,
        on_select: impl Fn(&T),
    ) -> Option<Arc<Backend>> {
        let len = items.len();
        let i = index.fetch_add(1, Ordering::Relaxed) % len;
        let j = (i + len / 2) % len;
        
        let item_i = items.get(i)?;
        let item_j = items.get(j)?;
        
        let backend_i = get_backend(item_i);
        let backend_j = get_backend(item_j);
        
        let idx = if backend_i.connections() <= backend_j.connections() {
            i
        } else {
            j
        };
        
        let item = items.get(idx)?;
        on_select(item);
        get_backend(item).fetch_add_connection(1);
        
        Some(get_arc(item))
    }

    fn maintain_sticky_slots(&self, pool: &[Arc<Backend>]) {
        let now = Instant::now();
        let current_ms = mono_ms();
        let ttl_ms = get_global_config().sticky_slot_ttl.as_millis() as u64;
        let expand_interval_ms = get_global_config().sticky_slot_expand_interval.as_millis() as u64;
        
        let mut slots = self.sticky_slots.write();
        
        slots.retain(|s| {
            if s.backend.is_isolated() || s.backend.is_removed() {
                return false;
            }
            !slot_expired(current_ms, s.last_access_ms.load(Ordering::Acquire), ttl_ms)
        });
        slots.shrink_to_fit();
        
        let get_active_unused = |used_addrs: &HashSet<SocketAddr>| {
            pool.iter()
                .filter(|b| b.is_selectable() && !used_addrs.contains(&b.addr))
                .min_by_key(|b| b.connections())
                .cloned()
        };
        
        let mut used_addrs: HashSet<_> = slots.iter().map(|s| s.backend.addr).collect();
        
        for s in slots.iter_mut().filter(|s| now.duration_since(s.last_switch) >= s.interval) {
            if let Some(new_b) = get_active_unused(&used_addrs) {
                used_addrs.insert(new_b.addr);
                s.backend = new_b;
                s.last_switch = now;
            }
        }
        
        let total_conns: usize = slots.iter().map(|s| s.backend.connections()).sum();
        let last_expand = self.last_expand_ms.load(Ordering::Relaxed);
        let should_expand = slots.is_empty() || (
            slots.len() < self.max_sticky_slots && (
                current_ms.saturating_sub(last_expand) >= expand_interval_ms ||
                slots.len() * slots.len() < total_conns
            )
        );
        
        if should_expand {
            let used_addrs: HashSet<_> = slots.iter().map(|s| s.backend.addr).collect();
            if let Some(b) = get_active_unused(&used_addrs) {
                let interval = get_global_config().sticky_base_interval + 
                    Duration::from_secs((slots.len() as u64) * get_global_config().sticky_increment_interval.as_secs());
                slots.push(StickySlot {
                    backend: b,
                    last_switch: now,
                    last_access_ms: AtomicU64::new(current_ms),
                    interval,
                });
                self.last_expand_ms.store(current_ms, Ordering::Relaxed);
            }
        }
    }

    fn start_sticky_maintainer(self: Arc<Self>) -> JoinHandle<()> {
        let cancel_token = self.cancel_token.clone();
        let lb = self.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            // 挂起恢复后不补跑历史 tick，只执行一次当前维护（Skip 只影响补跑方式）
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            interval.tick().await;
            
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        break;
                    }
                    _ = interval.tick() => {
                        let inner = lb.inner.read();
                        if !inner.primary.is_empty() {
                            lb.maintain_sticky_slots(&inner.primary);
                        } else if !inner.backup.is_empty() {
                            lb.maintain_sticky_slots(&inner.backup);
                        }
                    }
                }
            }
        })
    }

    fn check_warming_backends(&self, pool: &mut [Arc<Backend>]) {
        for backend in pool.iter() {
            if backend.check_warming_expired() {
                backend.mark_active();
            }
        }
    }

    fn cleanup_removed(&self) {
        let mut inner = self.inner.write();
        
        let removed_ips: Vec<_> = inner.primary.iter()
            .chain(inner.backup.iter())
            .filter(|b| b.is_removed())
            .map(|b| b.addr.ip())
            .collect();
        
        let before = inner.primary.len();
        inner.primary.retain(|b| !b.is_removed());
        inner.primary.shrink_to_fit();
        let primary_removed = before - inner.primary.len();
        
        let backup_before = inner.backup.len();
        inner.backup.retain(|b| !b.is_removed());
        inner.backup.shrink_to_fit();
        let backup_removed = backup_before - inner.backup.len();

        drop(inner);
        
        for ip in removed_ips {
            self.ip_set.write().remove(&ip);
        }

        let removed_count = primary_removed + backup_removed;
        if removed_count > 0 {
            push_log("INFO", &format!("[-] 清理 {} 个失效节点", removed_count));
            self.notify_resume();
        }
    }

    pub fn release(&self, backend: &Backend) {
        backend.fetch_sub_connection(1);
    }

    pub fn record(&self, backend: &Backend, delay_ms: Option<f32>, loss: f32) {
        backend.record(delay_ms, loss);
    }

    pub fn check_and_evict(&self, backend: &Backend) -> bool {
        if backend.is_isolated() || backend.is_removed() {
            return false;
        }

        let sample_count = backend.get_sample_count();
        
        if sample_count < get_global_config().evict_threshold {
            return false;
        }
        
        let avg_delay = backend.get_avg_delay();
        let loss_rate = backend.get_loss_rate();

        let delay_threshold = f32::from_bits(self.delay_threshold.load(Ordering::Relaxed));
        let loss_threshold = f32::from_bits(self.loss_threshold.load(Ordering::Relaxed));

        if avg_delay > delay_threshold || loss_rate > loss_threshold {
            if self.try_deactivate() {
                backend.mark_isolated();
                push_log("WARN", &format!("[→] {} 进入隔离状态 (延迟{:.0}ms 丢包{:.0}%)", 
                    backend.addr, avg_delay, loss_rate * 100.0));
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    /// 单一收口：检查是否允许减少一个可选后端。
    /// 达到下限时从备池补位，主队列总量不超出 primary_target。
    /// 返回 true 表示允许操作。
    pub(crate) fn try_deactivate(&self) -> bool {
        let selectable = self.inner.read().primary.iter()
            .filter(|b| b.is_selectable()).count();

        if selectable > self.min_active_target {
            return true;
        }

        let mut inner = self.inner.write();
        let Some(pos) = inner.backup.iter().position(|b| b.is_selectable()) else {
            return false;
        };
        let replacement = inner.backup.remove(pos);
        let promoted_addr = replacement.addr;
        replacement.mark_active();

        // 总量守恒：主队列已达目标上限时，先回收一个不可用节点（隔离/剔除）再放入替补
        let mut evicted_ip = None;
        if inner.primary.len() >= self.primary_target.load(Ordering::Relaxed)
            && let Some(iso_pos) = inner.primary.iter().position(|b| !b.is_selectable())
        {
            let evicted = inner.primary.remove(iso_pos);
            evicted_ip = Some(evicted.addr.ip());
        }
        inner.primary.push(replacement);
        drop(inner);

        if let Some(ip) = evicted_ip {
            self.ip_set.write().remove(&ip);
        }

        self.notify_resume();
        push_log("INFO", &format!("[↑] {} 从备选补充到负载均衡队列", promoted_addr));
        true
    }

    pub fn refill_from_backup(&self) {
        let primary_target = self.primary_target.load(Ordering::Relaxed);

        let mut inner = self.inner.write();
        let mut promoted = 0;
        while inner.primary.len() < primary_target && !inner.backup.is_empty() {
            let backend = inner.backup.remove(0);
            inner.primary.push(backend);
            promoted += 1;
        }
        drop(inner);
        
        if promoted > 0 {
            push_log("INFO", &format!("[↑] {} 个备选提升到主队列", promoted));
        }

        self.notify_resume();
    }

    pub fn get_backup_count(&self) -> usize {
        self.inner.read().backup.len()
    }

    pub fn get_primary_count(&self) -> usize {
        self.inner.read().primary.len()
    }

    /// 测速 / 健康检查请求超时（毫秒）。HTTP 客户端的连接预算初次构建与
    /// rebuild_client 都取这一值
    pub fn timeout_ms(&self) -> u64 {
        self.timeout_ms
    }

    pub fn get_primary_target(&self) -> usize {
        self.primary_target.load(Ordering::Relaxed)
    }

    pub fn get_backup_target(&self) -> usize {
        self.backup_target.load(Ordering::Relaxed)
    }

    pub fn add_to_primary(&self, addr: SocketAddr, initial_delay: f32, initial_loss: f32, colo: Option<String>) {
        self.add_to_list(addr, initial_delay, initial_loss, colo, &mut self.inner.write().primary);
    }

    pub fn add_to_backup(&self, addr: SocketAddr, initial_delay: f32, initial_loss: f32, colo: Option<String>) {
        self.add_to_list(addr, initial_delay, initial_loss, colo, &mut self.inner.write().backup);
    }

    fn add_to_list(&self, addr: SocketAddr, initial_delay: f32, initial_loss: f32, colo: Option<String>, target: &mut Vec<Arc<Backend>>) {
        let backend = Arc::new(Backend::new_with_initial(addr, initial_delay, initial_loss, colo));
        target.push(backend);
        self.ip_set.write().insert(addr.ip());
    }

    pub fn get_primary_backends(&self) -> Vec<Arc<Backend>> {
        self.inner.read().primary.clone()
    }

    pub fn get_backup_backends(&self) -> Vec<Arc<Backend>> {
        self.inner.read().backup.clone()
    }

    pub fn get_sticky_ips(&self) -> Vec<std::net::IpAddr> {
        self.sticky_slots
            .read()
            .iter()
            .map(|s| s.backend.addr.ip())
            .collect()
    }

    pub fn stop(&self) {
        self.cancel_token.cancel();
    }

    /// 刷新"下次检查时刻"：给界面展示的倒计时用，墙上时钟，不参与任何调度判定
    fn refresh_next_health_check_ms(&self) {
        let next = now_ms() + get_global_config().health_check_interval.as_millis() as u64;
        self.next_health_check_ms.store(next, Ordering::Relaxed);
    }

    pub fn get_next_health_check_secs(&self) -> u64 {
        let next_ms = self.next_health_check_ms.load(Ordering::Relaxed);
        let current_ms = now_ms();
        if next_ms > current_ms {
            (next_ms - current_ms) / 1000
        } else {
            0
        }
    }

    fn sort_backup(&self) {
        let mut inner = self.inner.write();
        inner.backup.sort_by(|a, b| Backend::cmp_eviction(b, a));
    }

    /// 启动 sticky 维护与健康检查后台任务，返回全部句柄供实例登记；
    /// URL 解析失败提前返回时，已启动的 sticky 维护句柄仍需交还
    pub fn start_health_check(self: Arc<Self>) -> Vec<JoinHandle<()>> {
        let mut handles = Vec::new();

        if self.client.read().is_none() {
            return handles;
        }

        handles.push(self.clone().start_sticky_maintainer());

        let cancel_token = self.cancel_token.clone();

        let Some((_, host, scheme, path)) = crate::core::hyper::parse_url(&self.health_check_url) else {
            push_log("WARN", &format!("健康检查 URL 无法解析，本次未启动健康检查: {}", self.health_check_url));
            return handles;
        };

        let ping_config = PingConfig {
            tls_port: self.tls_port,
            http_port: self.http_port,
            host: Arc::from(host.as_str()),
            scheme: Arc::from(scheme),
            path: Arc::from(path.as_str()),
            timeout_ms: self.timeout_ms,
            colo_filter: self.colo_filter.clone(),
        };

        self.refresh_next_health_check_ms();

        let lb = self.clone();
        let health_handle = tokio::spawn(async move {
            let mut backup_interval = tokio::time::interval(get_global_config().health_check_interval);
            // 挂起恢复后不补跑历史 tick，只执行一次当前检查
            backup_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            backup_interval.tick().await;
            
            let mut primary_interval = tokio::time::interval(get_global_config().health_check_interval.saturating_mul(PRIMARY_CHECK_INTERVAL_MULT));
            primary_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            primary_interval.tick().await;

            // 恢复校准的唯一入口：挂钟增量超过阈值即视为经历过挂起或校时，
            // 一次性重建客户端、重置后端指标并刷新展示用的下次检查时刻。
            // 两个时钟的增量一并记入日志，用于确认本平台上 Instant 是否计入挂起。
            let mut last_wall = SystemTime::now();
            let mut last_mono = Instant::now();
            const TIME_JUMP_THRESHOLD: Duration = Duration::from_secs(60);

            loop {
                let is_primary = tokio::select! {
                    _ = cancel_token.cancelled() => {
                        push_log("INFO", "[健康检查] 收到停止信号，退出");
                        break;
                    }
                    _ = backup_interval.tick() => false,
                    _ = primary_interval.tick() => true,
                };

                let now = SystemTime::now();
                let wall_delta = now.duration_since(last_wall).unwrap_or_default();
                let mono_delta = last_mono.elapsed();
                if wall_delta > TIME_JUMP_THRESHOLD {
                    push_log("INFO", &format!(
                        "[健康检查] 时间跳跃 wall={:?} mono={:?}，重建客户端并重置后端指标",
                        wall_delta, mono_delta));
                    lb.rebuild_client();
                    lb.reset_all_metrics();
                    lb.refresh_next_health_check_ms();
                }
                last_wall = now;
                last_mono = Instant::now();

                if is_primary {
                    lb.run_primary_health_check(ping_config.clone());
                } else {
                    lb.run_backup_health_check(ping_config.clone());
                }
            }
        });

        handles.push(health_handle);
        handles
    }

    fn run_health_check_for_pool(
        self: &Arc<Self>,
        ping_config: PingConfig,
        pool_type: PoolType,
    ) {
        let is_primary = pool_type == PoolType::Primary;

        // 每轮从 client 字段取当前客户端，时间跳跃后的重建从本轮检查起生效
        let Some(client) = self.current_client() else { return };

        let backends = {
            let mut inner = self.inner.write();
            let pool = if is_primary { &mut inner.primary } else { &mut inner.backup };
            self.check_warming_backends(pool);
            pool.clone()
        };
        
        if backends.is_empty() {
            if !is_primary {
                self.refresh_next_health_check_ms();
            }
            return;
        }

        let lb = self.clone();
        let pool_type_str = pool_type.as_str();
        let concurrency = get_global_config().health_check_concurrency;
        
        tokio::spawn(async move {
            let mut join_set = tokio::task::JoinSet::new();
            let mut removed_count = 0usize;

            // 限流路径与排空路径共用的单结果处理
            let handle_result = |res: Option<CheckTaskOutput>| {
                let Some(Ok((backend, result))) = res else {
                    return false;
                };
                Self::handle_health_check_result(backend, result, &lb, pool_type)
            };

            for backend in backends {
                if backend.is_removed() {
                    continue;
                }

                let pc = ping_config.clone();
                let client = client.clone();
                join_set.spawn(async move {
                    let result = crate::core::httping::http_ping_multi(backend.addr.ip(), &client, &pc).await;
                    (backend, result)
                });

                if join_set.len() >= concurrency && handle_result(join_set.join_next().await) {
                    removed_count += 1;
                }
            }

            while handle_result(join_set.join_next().await) {
                removed_count += 1;
            }

            lb.cleanup_removed();
            
            if removed_count > 0 {
                push_log("INFO", &format!("[{}] 检查完成，移除 {} 个", pool_type_str, removed_count));
            }
            
            if is_primary {
                lb.refill_from_backup();
            } else {
                lb.sort_backup();
                lb.refresh_next_health_check_ms();
            }
        });
    }

    fn run_backup_health_check(self: &Arc<Self>, config: PingConfig) {
        self.run_health_check_for_pool(config, PoolType::Backup);
    }

    fn run_primary_health_check(self: &Arc<Self>, config: PingConfig) {
        self.run_health_check_for_pool(config, PoolType::Primary);
    }

    fn handle_health_check_result(
        backend: Arc<Backend>,
        result: Option<PingResultDetail>,
        lb: &LoadBalancer,
        pool_type: PoolType,
    ) -> bool {
        let is_primary = pool_type == PoolType::Primary;

        // 阈值仅构造期设置一次，直接从 lb 读取，避免参数快照复制
        let delay_threshold = f32::from_bits(lb.delay_threshold.load(Ordering::Relaxed));
        let loss_threshold = f32::from_bits(lb.loss_threshold.load(Ordering::Relaxed));

        enum Action {
            None,
            Recover,
            Isolate,
            Remove(String),
        }

        let action = match result {
            Some(detail) if detail.colo_mismatch => {
                Action::Remove(format!("数据中心[{}]不匹配", detail.colo.as_deref().unwrap_or("未知")))
            }
            Some(detail) => {
                let ping_times = get_global_config().ping_times;
                backend.record(
                    Some(detail.delay),
                    (ping_times as f32 - detail.success_count as f32) / ping_times as f32,
                );
                if let Some(c) = detail.colo {
                    backend.set_colo(Some(c));
                }

                if backend.get_sample_count() < get_global_config().sample_window as usize {
                    Action::None
                } else if backend.get_avg_delay() > delay_threshold || backend.get_loss_rate() > loss_threshold {
                    if backend.is_isolated() || !is_primary {
                        Action::Remove("隔离后仍不达标".into())
                    } else {
                        Action::Isolate
                    }
                } else {
                    Action::Recover
                }
            }
            None => {
                backend.record(None, 1.0);
                backend.record_failure();

                let loss_rate = backend.get_loss_rate();
                let failures = backend.consecutive_failures();
                let over_limit = loss_rate > loss_threshold || failures >= MAX_CONSECUTIVE_FAILURES;

                if backend.get_sample_count() >= get_global_config().sample_window as usize && over_limit {
                    if backend.is_isolated() || !is_primary {
                        Action::Remove("无响应".into())
                    } else {
                        Action::Isolate
                    }
                } else {
                    Action::None
                }
            }
        };

        match action {
            Action::Isolate => {
                let isolated = lb.try_deactivate();
                if isolated {
                    backend.mark_isolated();
                    push_log("WARN", &format!("[→] {} 隔离 (延迟{:.0}ms 丢包{:.0}%)",
                        backend.addr, backend.get_avg_delay(), backend.get_loss_rate() * 100.0));
                }
                isolated
            }
            Action::Remove(reason) => {
                if lb.try_deactivate() {
                    backend.mark_removed();
                    push_log("WARN", &format!("[-] {} 移除 ({})", backend.addr, reason));
                    if is_primary {
                        lb.refill_from_backup();
                    }
                    true
                } else {
                    false
                }
            }
            Action::Recover => {
                backend.record_success();
                if backend.is_isolated() {
                    backend.mark_active();
                    push_log("INFO", &format!("[←] {} 恢复正常", backend.addr));
                }
                false
            }
            Action::None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 无 HTTP client 时不启动 sticky 维护任务；有 client 时至少交回 sticky 句柄
    #[tokio::test]
    async fn sticky_maintainer_requires_client() {
        let lb = Arc::new(LoadBalancer::new(1));
        assert!(lb.clone().start_health_check().is_empty());

        let client = Arc::new(crate::core::build_hyper_client(500));
        let lb = Arc::new(LoadBalancer::new(1).with_client(client));
        assert!(!lb.clone().start_health_check().is_empty());
    }

    /// 单调毫秒与墙上时钟是两个时钟：前者随运行时间前进、量级远小于后者
    #[test]
    fn mono_ms_is_distinct_from_wall_clock() {
        let a = mono_ms();
        std::thread::sleep(Duration::from_millis(5));
        let b = mono_ms();
        assert!(b >= a + 5, "单调毫秒应随运行时间前进: {a} -> {b}");
        assert!(b < now_ms(), "单调毫秒不应是墙上时钟");
    }

    /// TTL 判定要求同源时钟；混用时差值饱和为 0，槽会被误判为永不过期
    #[test]
    fn slot_expiry_requires_one_clock() {
        assert!(slot_expired(60_000, 0, 15_000), "60s 前访问、TTL 15s 应过期");
        assert!(!slot_expired(60_000, 55_000, 15_000), "5s 前访问不应过期");
        assert!(
            !slot_expired(1_000, 1_741_000_000_000, 15_000),
            "单调毫秒与挂钟值混用时差值饱和为 0，判定失效"
        );
    }
}