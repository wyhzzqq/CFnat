use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::{Mutex, RwLock};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use crate::core::{IpPool, LoadBalancer, HttpingConfig, build_hyper_client, run_continuous_httping, run_forward, CancellationToken, ConnectionRegistry};
use crate::core::types::{StatusInfo, ConfigOverrides};
use crate::core::accept_error;
use crate::log::{push_log, reset_start_time, get_log_buffer};

/// 单次测速 / 健康检查请求超时（毫秒）
const REQUEST_TIMEOUT_MS: u64 = 1800;

/// stop 阶段第一轮等待的总预算，所有句柄共享同一份，不按句柄重复计算
const HANDLE_STOP_BUDGET: Duration = Duration::from_secs(3);

/// 服务相位：实例所有权随相位移动，任意时刻至多一个有效实例
enum Phase {
    Stopped,
    Starting,
    /// 实例所有权在相位内。Running 只是生命周期相位，不保证转发任务仍然存活；
    /// 转发是否存活以 ServiceInstance::forward_fault 为准
    Running(Arc<ServiceInstance>),
    /// 实例由 stop 的局部变量持有，此相位仅表示停止进行中
    Stopping,
}

/// 一次有效运行的全部资源：句柄与取消令牌随实例生灭，不再有游离字段
struct ServiceInstance {
    cancel_token: CancellationToken,
    lb: Arc<LoadBalancer>,
    registry: Arc<ConnectionRegistry>,
    start_time: Instant,
    handles: Mutex<Vec<JoinHandle<()>>>,
    /// 转发任务首次异常退出的原因；None 表示转发任务仍存活
    forward_fault: Arc<Mutex<Option<String>>>,
}

impl ServiceInstance {
    /// 转发任务首次异常退出的原因，None 表示仍存活。
    /// 读取时不持锁做日志、网络或其他阻塞操作
    fn forward_fault(&self) -> Option<String> {
        self.forward_fault.lock().clone()
    }
}

/// 只保留首次故障原因，后续错误不覆盖原始原因
fn record_forward_fault(slot: &Mutex<Option<String>>, reason: String) {
    let mut fault = slot.lock();
    if fault.is_none() {
        *fault = Some(reason);
    }
}

/// 错误性质：用少量稳定类别描述，具体原因由 message 承载，
/// 避免每新增一条文案就改动跨层接口
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    /// 配置的语法或结构非法
    Validation,
    /// 与服务生命周期相位冲突
    Lifecycle,
    /// 启动或运行期失败
    Runtime,
}

/// 服务状态机错误：稳定的 category + 供展示的具体原因
#[derive(Debug, Clone)]
pub struct ServiceError {
    pub category: ErrorCategory,
    pub message: String,
}

impl ServiceError {
    fn validation(message: impl Into<String>) -> Self {
        Self { category: ErrorCategory::Validation, message: message.into() }
    }

    fn lifecycle(message: impl Into<String>) -> Self {
        Self { category: ErrorCategory::Lifecycle, message: message.into() }
    }

    /// 启动阶段可同步判定的运行期失败（资源、绑定等）
    fn runtime(message: impl Into<String>) -> Self {
        Self { category: ErrorCategory::Runtime, message: message.into() }
    }
}

impl std::fmt::Display for ServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

pub struct ServiceState {
    /// 相位放在 Arc 下：stop 的清理任务独立于调用方 future 存续，
    /// 需要在任务内改写相位
    phase: Arc<Mutex<Phase>>,
    config: RwLock<ServiceConfig>,
}

/// 启动过程的所有权守卫：构建期间相位保持 Starting，
/// panic 或提前返回时回滚为 Stopped；启动成功后由 finish_start 消费（forget）
struct StartGuard<'a>(&'a Mutex<Phase>);

impl Drop for StartGuard<'_> {
    fn drop(&mut self) {
        *self.0.lock() = Phase::Stopped;
    }
}

/// 等待全部句柄退出。第一轮共享一个总预算：预算耗尽后不再对后续句柄正常等待，
/// 统一 abort 再等其实际落地，不静默丢弃。返回被强制终止的任务数。
/// 预算只约束第一轮；收尾等待没有时间上限，以保证进入 Stopped 前资源确已释放。
async fn await_handles(handles: &mut [JoinHandle<()>], budget: Duration) -> usize {
    // deadline 取自 tokio 时钟：生产构建下与 std::time::Instant 同源，
    // 测试下随虚拟时间推进，使共享预算可被确定性验证
    let deadline = tokio::time::Instant::now() + budget;
    let mut stuck = Vec::new();
    let mut stuck_ids = Vec::new();
    for handle in handles.iter_mut() {
        let id = handle.id();
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if tokio::time::timeout(remaining, &mut *handle).await.is_err() {
            stuck_ids.push(id);
            stuck.push(handle);
        }
    }
    let count = stuck.len();
    for handle in &stuck {
        handle.abort();
    }
    let tail_start = tokio::time::Instant::now();
    for handle in stuck {
        // abort 后任务在下一个 await 点落地，等待其完全退出再宣告停止
        let _ = handle.await;
    }
    // 收尾等待无上限，超预算只记录不缩短
    let tail = tail_start.elapsed();
    if count > 0 && tail > budget {
        push_log("ERROR", &format!(
            "{} 个任务被强制终止后，收尾等待 {:?} 超出预算 {:?}（任务 id: {:?}）",
            count, tail, budget, stuck_ids
        ));
    }
    count
}

/// 同步绑定监听端口。绑定失败在启动阶段返回给调用方，因此不产生任何后台任务，
/// 也不需要走故障槽；转发任务只负责"绑定成功之后"的失败
fn bind_listener(addr: SocketAddr) -> Result<TcpListener, ServiceError> {
    let listener = std::net::TcpListener::bind(addr).map_err(|e| {
        ServiceError::runtime(format!("端口 {} 无法绑定：{}", addr, accept_error::describe(&e)))
    })?;
    listener.set_nonblocking(true).map_err(|e| {
        ServiceError::runtime(format!("端口 {} 切换非阻塞失败：{}", addr, accept_error::describe(&e)))
    })?;
    TcpListener::from_std(listener).map_err(|e| {
        ServiceError::runtime(format!("端口 {} 注册到运行时失败：{}", addr, accept_error::describe(&e)))
    })
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ServiceConfig {
    pub ip_file: String,
    pub http: String,
    pub delay_limit: u64,
    pub tlr: f64,
    pub ips: usize,
    pub threads: usize,
    pub tls_port: u16,
    pub http_port: u16,
    pub colo: Option<Vec<String>>,
    pub addr: SocketAddr,
    pub max_sticky_slots: usize,
    #[cfg(feature = "web")]
    pub api_addr: Option<SocketAddr>,
}

impl ServiceConfig {
    // 各字段合法界与 args.rs 的 CLI 解析保持同一套（dl 1..=2000、tlr 0..=1、ips 1..=128、
    // threads 1..=1024、sticky 槽 1..=32），两个入口不出现第二套规则；
    // 端口不在入口收窄，0 由 validate 显式拒绝
    pub fn apply_overrides(&mut self, overrides: &ConfigOverrides) {
        if let Some(v) = &overrides.ip_file { self.ip_file = v.clone(); }
        if let Some(v) = &overrides.http { self.http = v.clone(); }
        if let Some(v) = overrides.delay_limit { self.delay_limit = v.clamp(1, 2000); }
        if let Some(v) = overrides.tlr { self.tlr = v.clamp(0.0, 1.0); }
        if let Some(v) = overrides.ips { self.ips = v.clamp(1, 128); }
        if let Some(v) = overrides.threads { self.threads = v.clamp(1, 1024); }
        if let Some(v) = overrides.tls_port { self.tls_port = v; }
        if let Some(v) = overrides.http_port { self.http_port = v; }
        if let Some(v) = &overrides.colo { self.colo = Some(v.clone()); }
        if let Some(v) = overrides.addr { self.addr = v; }
        if let Some(v) = overrides.max_sticky_slots { self.max_sticky_slots = v.clamp(1, 32); }
        #[cfg(feature = "web")]
        if let Some(v) = overrides.api_addr { self.api_addr = Some(v); }
    }

    /// 校验启动所需配置的语法与结构，不做网络可达性检查。
    /// 复用测速 / 健康检查的同一套 URL 解析，不引入第二套规则
    pub fn validate(&self) -> Result<(), ServiceError> {
        if self.tls_port == 0 || self.http_port == 0 {
            return Err(ServiceError::validation(format!(
                "端口必须为 1..=65535（tls_port={}, http_port={}）",
                self.tls_port, self.http_port
            )));
        }
        if crate::core::hyper::parse_url(&self.http).is_none() {
            return Err(ServiceError::validation(
                format!("测速地址无效（需包含 scheme 与 host）: {}", self.http),
            ));
        }
        Ok(())
    }
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            ip_file: "ip.txt".to_string(),
            http: "http://cp.cloudflare.com/cdn-cgi/trace".to_string(),
            delay_limit: 500,
            tlr: 0.1,
            ips: 10,
            threads: 16,
            tls_port: 443,
            http_port: 80,
            colo: None,
            addr: "127.6.6.6:1234".parse().unwrap(),
            max_sticky_slots: 5,
            #[cfg(feature = "web")]
            api_addr: None,
        }
    }
}

impl Default for ServiceState {
    fn default() -> Self {
        Self::new()
    }
}

impl ServiceState {
    pub fn new() -> Self {
        Self {
            phase: Arc::new(Mutex::new(Phase::Stopped)),
            config: RwLock::new(ServiceConfig::default()),
        }
    }

    /// 仅表示生命周期相位为 Running，不保证转发任务仍存活；
    /// 对外是否按正常运行对待见 build_full_status
    pub fn is_running(&self) -> bool {
        matches!(&*self.phase.lock(), Phase::Running(_))
    }

    pub fn get_config(&self) -> ServiceConfig {
        self.config.read().clone()
    }

    pub fn update_config(&self, new_config: ServiceConfig) {
        *self.config.write() = new_config;
    }

    pub fn get_uptime_secs(&self) -> u64 {
        match &*self.phase.lock() {
            Phase::Running(inst) => inst.start_time.elapsed().as_secs(),
            _ => 0,
        }
    }

    /// 对外状态契约：
    /// - `running=true` 当且仅当相位为 Running 且转发任务仍存活；
    /// - `running=false` 只表示不能按正常运行对待，不代表清理已完成：
    ///   此时相位可能仍是 Running（实例待 stop 清理），也可能已是 Stopped；
    /// - 转发故障时返回 StatusInfo::empty()，不上报 uptime 与 IP 指标，
    ///   避免把"端口已关、任务已死"呈现成正常运行。
    pub fn build_full_status(&self) -> StatusInfo {
        match &*self.phase.lock() {
            Phase::Running(inst) => {
                // 先读故障槽再决定返回哪种状态，判断集中在这一处
                if inst.forward_fault().is_some() {
                    return StatusInfo::empty();
                }
                let mut info = StatusInfo::from_loadbalancer(&inst.lb);
                info.running = true;
                info.uptime_secs = inst.start_time.elapsed().as_secs();
                info
            }
            _ => StatusInfo::empty(),
        }
    }

    /// 占住 Starting 相位，构建期间任何并发 start/stop 都得到明确错误。
    /// 返回的守卫在 panic 或提前返回时把相位回滚为 Stopped。
    fn begin_start(&self) -> Result<StartGuard<'_>, ServiceError> {
        let mut phase = self.phase.lock();
        match *phase {
            Phase::Stopped => {
                *phase = Phase::Starting;
                Ok(StartGuard(&self.phase))
            }
            Phase::Starting => Err(ServiceError::lifecycle("服务正在启动")),
            Phase::Running(_) => Err(ServiceError::lifecycle("服务已在运行")),
            Phase::Stopping => Err(ServiceError::lifecycle("服务正在停止")),
        }
    }

    pub fn start(&self) -> Result<(), ServiceError> {
        let guard = self.begin_start()?;

        let config = self.get_config();
        config.validate()?;
        let ip_pool = Arc::new(IpPool::from_file(&config.ip_file));
        if ip_pool.total_count() == 0 {
            return Err(ServiceError::validation("未找到有效的 IP"));
        }

        self.finish_start(config, ip_pool, guard)
    }

    pub fn start_with_ips(&self, ip_file: Option<&str>, ip_content: Option<&[String]>) -> Result<(), ServiceError> {
        let guard = self.begin_start()?;

        let config = self.get_config();
        config.validate()?;

        let mut all_ips = Vec::new();

        if let Some(file) = ip_file
            && !file.is_empty()
        {
            match std::fs::File::open(file) {
                Ok(f) => {
                    use std::io::{BufRead, BufReader};
                    for line in BufReader::new(f).lines().map_while(Result::ok) {
                        let line = line.trim();
                        if !line.is_empty() {
                            all_ips.push(line.to_string());
                        }
                    }
                }
                Err(e) => push_log("WARN", &format!("IP 文件打开失败: {} ({})", file, e)),
            }
        }

        if let Some(content) = ip_content {
            all_ips.extend(content.iter().cloned());
        }

        let ip_pool = Arc::new(IpPool::new(&all_ips));
        if ip_pool.total_count() == 0 {
            // 显式来源由调用方选定，产不出 IP 时报错并指明来源；两者都未指定时使用配置中的路径
            if let Some(file) = ip_file.filter(|f| !f.is_empty()) {
                return Err(ServiceError::validation(format!("IP 文件 {} 未提供有效 IP", file)));
            }
            if ip_content.is_some_and(|c| !c.is_empty()) {
                return Err(ServiceError::validation("填入的 IP 中没有有效条目"));
            }
            let ip_pool = Arc::new(IpPool::from_file(&config.ip_file));
            if ip_pool.total_count() == 0 {
                return Err(ServiceError::validation("未找到有效的 IP"));
            }
            return self.finish_start(config, ip_pool, guard);
        }

        self.finish_start(config, ip_pool, guard)
    }

    /// 构建 instance 并登记为 Running。构建全部在临界区外完成；
    /// 守卫在此被消费，之后相位由 Phase::Running 承接。
    /// 配置取自入口处已校验的那一份，不在此重新读取，避免校验过的值与实际使用的值不一致
    fn finish_start(&self, config: ServiceConfig, ip_pool: Arc<IpPool>, guard: StartGuard<'_>) -> Result<(), ServiceError> {
        // 先绑定：失败即返回，此时尚未产生任何后台任务
        let listener = bind_listener(config.addr)?;

        // 启动前清空旧日志并重置时间基准
        reset_start_time();
        get_log_buffer().clear();

        let (notify_tx, notify_rx) = tokio::sync::watch::channel(false);
        let colo_filter = config.colo.clone();

        let cancel_token = CancellationToken::new();

        let lb_builder = LoadBalancer::new(config.ips)
            .with_delay_threshold(config.delay_limit as f32)
            .with_loss_threshold(config.tlr as f32)
            .with_health_check_url(config.http.clone())
            .with_ports(config.tls_port, config.http_port)
            .with_timeout(REQUEST_TIMEOUT_MS)
            .with_notify(notify_tx)
            .with_colo_filter(colo_filter.clone());
        // 客户端连接预算与测速/健康检查请求超时同源，rebuild_client 沿用同一值
        let client = Arc::new(build_hyper_client(lb_builder.timeout_ms()));
        let lb = Arc::new(lb_builder.with_client(client).with_max_sticky_slots(config.max_sticky_slots));

        crate::core::init_global_limiter(config.threads);

        let registry = Arc::new(ConnectionRegistry::new());
        let mut handles: Vec<JoinHandle<()>> = Vec::new();

        let lb_clone = lb.clone();
        let http = config.http.clone();
        let tls_port = config.tls_port;
        let http_port = config.http_port;
        let delay_limit = config.delay_limit;
        let cancel_token_for_httping = cancel_token.clone();

        let httping_handle: JoinHandle<()> = tokio::spawn(async move {
            run_continuous_httping(
                ip_pool,
                lb_clone,
                &http,
                HttpingConfig {
                    tls_port,
                    http_port,
                    timeout_ms: REQUEST_TIMEOUT_MS,
                    delay_limit,
                    colo_filter: colo_filter.map(Arc::new),
                },
                notify_rx,
                cancel_token_for_httping,
            ).await;
        });
        handles.push(httping_handle);

        handles.extend(lb.clone().start_health_check());

        let lb_forward = lb.clone();
        let forward_cancel_token = cancel_token.clone();
        let registry_forward = registry.clone();
        let forward_fault = Arc::new(Mutex::new(None));
        let forward_fault_task = forward_fault.clone();

        let forward_handle: JoinHandle<()> = tokio::spawn(async move {
            if let Err(e) = run_forward(listener, lb_forward, tls_port, http_port, forward_cancel_token, registry_forward).await {
                let reason = accept_error::describe(&e);
                // 先落故障信息再记日志：状态读取只依赖槽位，不依赖日志
                record_forward_fault(&forward_fault_task, reason.clone());
                push_log("ERROR", &format!("转发服务错误：{}", reason));
            }
        });
        handles.push(forward_handle);

        std::mem::forget(guard);
        *self.phase.lock() = Phase::Running(Arc::new(ServiceInstance {
            cancel_token,
            lb,
            registry,
            start_time: Instant::now(),
            handles: Mutex::new(handles),
            forward_fault,
        }));

        Ok(())
    }

    pub async fn stop(&self) -> Result<(), ServiceError> {
        // 同步段一次持锁完成：相位切换、取消信号、连接关断、句柄摘取。
        // 之后不再持有 MutexGuard await（保证 stop future 为 Send）
        let mut handles = {
            let mut phase = self.phase.lock();
            match &*phase {
                Phase::Running(_) => {}
                Phase::Stopping => return Err(ServiceError::lifecycle("服务正在停止")),
                Phase::Starting => return Err(ServiceError::lifecycle("服务正在启动")),
                Phase::Stopped => return Err(ServiceError::lifecycle("服务未运行")),
            }
            let inst = match std::mem::replace(&mut *phase, Phase::Stopped) {
                Phase::Running(inst) => inst,
                _ => unreachable!(),
            };
            *phase = Phase::Stopping;

            inst.cancel_token.cancel();
            inst.lb.stop();
            // 先关断全部存量连接，唤醒阻塞在读上的转发任务（含 Linux splice 线程）
            inst.registry.shutdown_all();
            let mut lock = inst.handles.lock();
            std::mem::take(&mut *lock)
            // 实例在此释放：句柄已摘出，其余资源由各任务持有的 Arc 延续
        };

        // 清理在独立任务中执行：调用方取消只取消等待，不取消清理本身，
        // 相位最终仍收敛为 Stopped
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let phase = Arc::clone(&self.phase);
        tokio::spawn(async move {
            let stuck = await_handles(&mut handles, HANDLE_STOP_BUDGET).await;
            if stuck > 0 {
                push_log("WARN", &format!("停止超时，{} 个任务未自行退出，已强制终止", stuck));
            }
            *phase.lock() = Phase::Stopped;
            push_log("INFO", "服务已停止");
            let _ = done_tx.send(());
        });

        match done_rx.await {
            Ok(()) => Ok(()),
            // 清理任务异常退出时相位可能停在 Stopping，不能向调用方报告"已停止"
            Err(_) => Err(ServiceError::runtime("停止清理任务异常退出")),
        }
    }

    pub fn get_loadbalancer(&self) -> Option<Arc<LoadBalancer>> {
        match &*self.phase.lock() {
            Phase::Running(inst) => Some(inst.lb.clone()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 覆盖层与 CLI（args.rs）必须落在同一套界内，防止入口规则分裂；
    /// 端口不在入口收窄，0 交给 validate 拒绝
    #[test]
    fn overrides_clamp_to_cli_bounds() {
        let mut config = ServiceConfig::default();
        config.apply_overrides(&ConfigOverrides {
            delay_limit: Some(0),
            tlr: Some(5.0),
            ips: Some(0),
            threads: Some(100000),
            tls_port: Some(0),
            http_port: Some(0),
            max_sticky_slots: Some(0),
            ..Default::default()
        });
        assert_eq!(config.delay_limit, 1);
        assert_eq!(config.tlr, 1.0);
        assert_eq!(config.ips, 1);
        assert_eq!(config.threads, 1024);
        assert_eq!(config.tls_port, 0);
        assert_eq!(config.http_port, 0);
        assert_eq!(config.max_sticky_slots, 1);
        assert!(config.validate().is_err(), "端口 0 应被校验拒绝");

        config.apply_overrides(&ConfigOverrides {
            delay_limit: Some(999999),
            ips: Some(999999),
            max_sticky_slots: Some(999999),
            ..Default::default()
        });
        assert_eq!(config.delay_limit, 2000);
        assert_eq!(config.ips, 128);
        assert_eq!(config.max_sticky_slots, 32);

        config.tls_port = 1;
        config.http_port = 1;
        assert!(config.validate().is_ok(), "端口落在 1..=65535 应通过校验");
    }

    /// 启动前校验只认 URL 语法与结构：缺 scheme 或整体不可解析都拒绝
    #[test]
    fn validate_accepts_well_formed_url_and_rejects_malformed() {
        let mut config = ServiceConfig::default();
        assert!(config.validate().is_ok(), "默认测速地址应通过");

        config.http = "https://example.com/cdn-cgi/trace".to_string();
        assert!(config.validate().is_ok(), "https 地址应通过");

        config.http = "cp.cloudflare.com/cdn-cgi/trace".to_string();
        assert!(config.validate().is_err(), "缺少 scheme 应被拒绝");

        config.http = String::new();
        assert!(config.validate().is_err(), "空地址应被拒绝");
    }

    /// 校验失败必须由 StartGuard 回滚为 Stopped，且不阻断后续正常启动；
    /// 同时固定 begin_start 的相位错误优先于配置错误的语义
    #[test]
    fn invalid_config_rolls_back_and_phase_error_takes_precedence() {
        let rt = test_runtime();
        let (service, path) = service_with_ip_file("invalid_http");
        let _guard = rt.handle().enter();

        // Stopped 态放入非法 URL（缺 scheme）
        {
            let mut config = service.get_config();
            config.http = "cp.cloudflare.com/cdn-cgi/trace".to_string();
            service.update_config(config);
        }
        let err = service.start().unwrap_err();
        assert!(err.message.contains("测速地址无效"), "应返回配置错误，实际: {}", err.message);
        assert_eq!(err.category, ErrorCategory::Validation);
        assert!(!service.is_running(), "校验失败后相位必须回滚为 Stopped");

        // 修复配置后可正常启动，证明回滚未留下残留
        {
            let mut config = service.get_config();
            config.http = "http://127.0.0.1:9/cdn-cgi/trace".to_string();
            service.update_config(config);
        }
        service.start().unwrap();
        assert!(service.is_running());

        // 运行中即使配置非法，也应先报相位错误而非配置错误
        {
            let mut config = service.get_config();
            config.http = String::new();
            service.update_config(config);
        }
        assert_eq!(service.start().unwrap_err().message, "服务已在运行");
        assert_eq!(service.start().unwrap_err().category, ErrorCategory::Lifecycle);

        rt.block_on(service.stop()).unwrap();
        std::fs::remove_file(&path).ok();
    }

    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap()
    }

    /// 写入临时 IP 文件并返回配置好该文件的服务；测试结束后删除文件
    fn service_with_ip_file(tag: &str) -> (Arc<ServiceState>, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!("cfnat_test_ips_{}_{}.txt", tag, std::process::id()));
        std::fs::write(&path, "1.2.3.4\n5.6.7.8\n").unwrap();

        let service = Arc::new(ServiceState::new());
        let mut config = service.get_config();
        config.ip_file = path.to_string_lossy().into_owned();
        config.http = "http://127.0.0.1:9/cdn-cgi/trace".to_string();
        config.addr = "127.0.0.1:0".parse().unwrap();
        service.update_config(config);
        (service, path)
    }

    #[test]
    fn concurrent_start_admits_single_instance() {
        let rt = test_runtime();
        let (service, path) = service_with_ip_file("conc_start");
        let handle = rt.handle().clone();

        let results: Vec<_> = (0..8).map(|_| {
            let service = service.clone();
            let handle = handle.clone();
            std::thread::spawn(move || {
                let _guard = handle.enter();
                service.start()
            })
        }).collect::<Vec<_>>().into_iter().map(|j| j.join().unwrap()).collect();

        let ok = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(ok, 1, "8 个并发 start 应恰有 1 个成功，实际: {:?}", results);
        assert!(service.is_running());

        rt.block_on(service.stop()).unwrap();
        assert!(!service.is_running());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn stop_then_start_recovers() {
        let rt = test_runtime();
        let (service, path) = service_with_ip_file("restart");

        // Stopped 时 stop 得到明确错误
        assert_eq!(rt.block_on(service.stop()).unwrap_err().message, "服务未运行");

        let _guard = rt.handle().enter();
        service.start().unwrap();
        assert!(service.is_running());
        rt.block_on(service.stop()).unwrap();
        assert!(!service.is_running());

        // stop 完成后可以再次启动，实例无残留
        service.start().unwrap();
        assert!(service.is_running());
        rt.block_on(service.stop()).unwrap();
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn second_start_while_running_is_rejected() {
        let rt = test_runtime();
        let (service, path) = service_with_ip_file("double_start");

        let _guard = rt.handle().enter();
        service.start().unwrap();
        assert_eq!(service.start().unwrap_err().message, "服务已在运行");
        assert!(service.is_running());
        rt.block_on(service.stop()).unwrap();
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn starting_phase_rejects_start_and_stop_and_rolls_back() {
        let rt = test_runtime();
        let (service, path) = service_with_ip_file("starting_phase");

        let guard = service.begin_start().unwrap();
        assert_eq!(service.start().unwrap_err().message, "服务正在启动");
        let _guard = rt.handle().enter();
        assert_eq!(rt.block_on(service.stop()).unwrap_err().message, "服务正在启动");

        // 守卫 drop（模拟启动 panic / 提前返回）后相位回滚，可正常再次启动
        drop(guard);
        assert!(!service.is_running());
        let _guard2 = rt.handle().enter();
        service.start().unwrap();
        assert!(service.is_running());
        rt.block_on(service.stop()).unwrap();
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn await_handles_aborts_stuck_tasks() {
        let rt = test_runtime();
        let _guard = rt.handle().enter();

        let stuck = tokio::spawn(async { tokio::time::sleep(Duration::from_secs(60)).await });
        let quick = tokio::spawn(async {});
        let mut handles = vec![stuck, quick];

        let aborted = rt.block_on(await_handles(&mut handles, Duration::from_millis(50)));
        assert_eq!(aborted, 1, "只有超时的任务被强制终止");
    }

    /// 共享预算：多个卡在同一等待阶段的句柄不再各自获得一份完整预算。
    /// 用暂停时钟 + 自动推进按虚拟时间断言，不依赖真实墙钟
    #[test]
    fn await_handles_shares_one_deadline_across_handles() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();

        rt.block_on(async {
            let mut handles: Vec<_> = (0..3)
                .map(|_| tokio::spawn(async { tokio::time::sleep(Duration::from_secs(60)).await }))
                .collect();

            let started = tokio::time::Instant::now();
            let aborted = await_handles(&mut handles, HANDLE_STOP_BUDGET).await;
            let elapsed = started.elapsed();

            assert_eq!(aborted, 3, "三个卡住的任务都应被强制终止");
            // 每句柄各自计预算会推进 3 份预算，共享预算只推进 1 份
            assert!(
                elapsed >= HANDLE_STOP_BUDGET && elapsed < HANDLE_STOP_BUDGET * 2,
                "共享预算应在 1 份预算内完成，实际推进 {:?}",
                elapsed
            );
        });
    }

    #[test]
    fn forward_fault_keeps_first_reason() {
        let slot = Mutex::new(None);
        record_forward_fault(&slot, "首次原因".to_string());
        record_forward_fault(&slot, "后续原因".to_string());
        assert_eq!(slot.lock().as_deref(), Some("首次原因"));
    }

    /// 转发任务异常退出后：状态不再报告运行，但相位未变、实例仍待清理，
    /// stop 仍按既有顺序收尾，之后可重新启动并重新绑定端口
    #[test]
    fn forward_failure_reports_not_running_and_stop_recovers() {
        let rt = test_runtime();
        let (service, path) = service_with_ip_file("forward_fault");

        let _guard = rt.handle().enter();
        service.start().unwrap();
        assert!(service.build_full_status().running, "启动后应报告运行中");

        // 直接置位故障槽，模拟 accept 循环在启动之后异常退出。
        // 绑定失败已由 finish_start 同步返回，不再经过故障槽
        match &*service.phase.lock() {
            Phase::Running(inst) => {
                record_forward_fault(&inst.forward_fault, "模拟转发故障".to_string())
            }
            _ => panic!("启动后相位应为 Running"),
        }

        let info = service.build_full_status();
        assert!(!info.running, "转发任务已退出，不应再报告运行中");
        assert_eq!(info.uptime_secs, 0, "故障状态不暴露 uptime");
        assert!(info.primary_ips.is_empty(), "故障状态不暴露主队列 IP");
        assert!(info.backup_ips.is_empty(), "故障状态不暴露备选 IP");
        assert!(info.sticky_ips.is_empty(), "故障状态不暴露 sticky IP");

        // 故障不直接改写相位：清理尚未发生，stop 仍能收尾
        assert!(service.is_running(), "故障不应直接改写相位");
        rt.block_on(service.stop()).unwrap();
        assert!(!service.is_running());
        assert!(!service.build_full_status().running);

        // stop 完成后端口已释放，可重新启动并重新绑定；新实例的故障槽是空的
        service.start().unwrap();
        assert!(service.build_full_status().running, "重启后应恢复为正常运行");
        rt.block_on(service.stop()).unwrap();
        std::fs::remove_file(&path).ok();
    }

    /// 启动使用的是入口处校验过的配置快照，而非 finish_start 内再次读取的配置
    #[test]
    fn finish_start_uses_validated_snapshot() {
        let rt = test_runtime();
        let (service, path) = service_with_ip_file("snapshot");

        // 占住一个端口：快照用它作绑定地址 ⇒ 绑定必然失败并同步返回
        let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let occupied_addr = occupied.local_addr().unwrap();
        let snapshot = {
            let mut config = service.get_config();
            config.addr = occupied_addr;
            config
        };
        {
            // 与快照不同的"当前配置"：地址可用，若被使用则绑定会成功
            let mut live = service.get_config();
            live.addr = "127.0.0.1:0".parse().unwrap();
            service.update_config(live);
        }

        let _guard = rt.handle().enter();
        let guard = service.begin_start().unwrap();
        let ip_pool = Arc::new(IpPool::from_file(&snapshot.ip_file));
        let err = service.finish_start(snapshot, ip_pool, guard).unwrap_err();

        // 报的是快照里那个被占用的地址 ⇒ 使用的是快照，而非当前配置
        assert_eq!(err.category, ErrorCategory::Runtime);
        assert!(
            err.message.contains(&occupied_addr.to_string()),
            "错误应指向快照地址 {occupied_addr}，实际: {}",
            err.message
        );
        assert!(!service.is_running(), "绑定失败后相位必须回滚为 Stopped");

        drop(occupied);
        std::fs::remove_file(&path).ok();
    }

    /// 初次构建与 rebuild_client 的连接预算同源：都以负载均衡器持有的
    /// 请求超时为准，不回退到延迟阈值
    #[test]
    fn client_connect_budget_matches_request_timeout() {
        let rt = test_runtime();
        let (service, path) = service_with_ip_file("timeout_source");
        let _guard = rt.handle().enter();

        service.start().unwrap();
        let lb = service.get_loadbalancer().expect("运行中应持有负载均衡器");
        assert_eq!(lb.timeout_ms(), REQUEST_TIMEOUT_MS);

        rt.block_on(service.stop()).unwrap();
        std::fs::remove_file(&path).ok();
    }

    /// stop 的调用方被取消后，清理任务继续执行：相位收敛为 Stopped，
    /// 句柄被回收，服务可再次启动
    #[test]
    fn stop_cancellation_still_converges() {
        let rt = test_runtime();
        let (service, path) = service_with_ip_file("stop_cancel");

        rt.block_on(async {
            service.start().unwrap();
            // 注入一个对取消信号无响应的任务，拉长 await_handles 的第一轮等待
            match &*service.phase.lock() {
                Phase::Running(inst) => inst.handles.lock().push(tokio::spawn(async {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                })),
                _ => panic!("启动后相位应为 Running"),
            }

            let stop_task = tokio::spawn({
                let service = service.clone();
                async move { service.stop().await }
            });
            tokio::time::sleep(Duration::from_millis(100)).await;
            stop_task.abort();

            // 相位最终收敛为 Stopped（卡住的任务由共享预算强制终止）
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                if matches!(&*service.phase.lock(), Phase::Stopped) {
                    break;
                }
                assert!(Instant::now() < deadline, "stop 调用被取消后相位未收敛");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            assert!(!service.is_running());

            // 清理无遗留：可重新启动并正常停止
            service.start().unwrap();
            assert!(service.is_running());
            service.stop().await.unwrap();
        });
        std::fs::remove_file(&path).ok();
    }
}