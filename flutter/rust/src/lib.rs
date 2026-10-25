mod frb_generated; /* AUTO INJECTED BY flutter_rust_bridge. This line may not be accurate, and you can change it according to your needs. */
use flutter_rust_bridge::frb;
use std::net::SocketAddr;
use std::sync::OnceLock;
use std::sync::Arc;
use cfnat::core::{ServiceError, ServiceState};
use cfnat::core::ErrorCategory as CoreErrorCategory;
use cfnat::core::types::ConfigOverrides;
use tokio::runtime::Runtime;

static SERVICE: OnceLock<Arc<ServiceState>> = OnceLock::new();
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
    })
}

fn get_service() -> Arc<ServiceState> {
    SERVICE.get_or_init(|| Arc::new(ServiceState::new())).clone()
}

#[frb]
pub struct StatusInfo {
    pub running: bool,
    pub uptime_secs: u64,
    pub next_health_check: u64,
    pub health_check_interval: u64,
    pub primary_count: i32,
    pub primary_target: i32,
    pub backup_count: i32,
    pub backup_target: i32,
    pub primary_ips: Vec<IpInfo>,
    pub backup_ips: Vec<IpInfo>,
    pub sticky_ips: Vec<String>,
}

#[frb]
pub struct IpInfo {
    pub ip: String,
    pub delay: f64,
    pub loss: f64,
    pub samples: i32,
    pub colo: Option<String>,
}

#[frb]
pub struct ConfigInfo {
    pub ip_file: String,
    pub http: String,
    pub delay_limit: u64,
    pub tlr: f64,
    pub ips: i32,
    pub threads: i32,
    pub tls_port: i32,
    pub http_port: i32,
    pub max_sticky_slots: i32,
    pub addr: String,
    pub colo: Option<Vec<String>>,
}

#[frb]
pub struct LogItem {
    pub timestamp: String,
    pub level: String,
    pub message: String,
}

impl From<cfnat::log::LogEntry> for LogItem {
    fn from(entry: cfnat::log::LogEntry) -> Self {
        Self {
            timestamp: entry.timestamp,
            level: entry.level,
            message: entry.message,
        }
    }
}

/// 错误性质，与 cfnat 核心的 ErrorCategory 一一对应；
/// 少量稳定类别，具体原因由 ActionResult::message 承载
#[frb]
pub enum ErrorCategory {
    Validation,
    Lifecycle,
    Runtime,
}

impl From<cfnat::core::ErrorCategory> for ErrorCategory {
    fn from(category: cfnat::core::ErrorCategory) -> Self {
        match category {
            cfnat::core::ErrorCategory::Validation => Self::Validation,
            cfnat::core::ErrorCategory::Lifecycle => Self::Lifecycle,
            cfnat::core::ErrorCategory::Runtime => Self::Runtime,
        }
    }
}

/// 启动 / 停止的统一结果：成功时 message 与 category 均为 None
#[frb]
pub struct ActionResult {
    pub success: bool,
    pub message: Option<String>,
    pub category: Option<ErrorCategory>,
}

impl ActionResult {
    fn ok() -> Self {
        Self { success: true, message: None, category: None }
    }

    fn from_error(error: cfnat::core::ServiceError) -> Self {
        Self {
            success: false,
            message: Some(error.message),
            category: Some(error.category.into()),
        }
    }
}

/// 把 i32 参数转换为目标类型，目标类型无法表示该值时返回校验错误；
/// 接受范围与 HTTP 入口按 Option<usize> / Option<u16> 反序列化一致
fn checked<T>(name: &str, value: Option<i32>) -> Result<Option<T>, ServiceError>
where
    T: TryFrom<i32>,
{
    value
        .map(|v| {
            T::try_from(v).map_err(|_| ServiceError {
                category: CoreErrorCategory::Validation,
                message: format!("{} 超出允许范围: {}", name, v),
            })
        })
        .transpose()
}

fn build_overrides(
    ip_file: Option<String>,
    ip_content: Option<Vec<String>>,
    http: Option<String>,
    delay_limit: Option<u64>,
    tlr: Option<f64>,
    ips: Option<i32>,
    threads: Option<i32>,
    tls_port: Option<i32>,
    http_port: Option<i32>,
    max_sticky_slots: Option<i32>,
    addr: Option<String>,
    colo: Option<Vec<String>>,
) -> Result<ConfigOverrides, ServiceError> {
    let addr = match addr {
        Some(text) => Some(text.parse::<SocketAddr>().map_err(|_| ServiceError {
            category: CoreErrorCategory::Validation,
            message: format!("监听地址无效: {}", text),
        })?),
        None => None,
    };

    Ok(ConfigOverrides {
        ip_file,
        ip_content,
        http,
        delay_limit,
        tlr,
        ips: checked("ips", ips)?,
        threads: checked("threads", threads)?,
        tls_port: checked("tls_port", tls_port)?,
        http_port: checked("http_port", http_port)?,
        colo,
        addr,
        max_sticky_slots: checked("max_sticky_slots", max_sticky_slots)?,
    })
}

#[frb]
pub fn start_service(
    ip_file: Option<String>,
    ip_content: Option<Vec<String>>,
    http: Option<String>,
    delay_limit: Option<u64>,
    tlr: Option<f64>,
    ips: Option<i32>,
    threads: Option<i32>,
    tls_port: Option<i32>,
    http_port: Option<i32>,
    max_sticky_slots: Option<i32>,
    addr: Option<String>,
    colo: Option<Vec<String>>,
) -> ActionResult {
    let runtime = get_runtime();
    let _guard = runtime.enter();

    let service = get_service();
    let mut config = service.get_config();

    let overrides = match build_overrides(
        ip_file, ip_content, http, delay_limit, tlr,
        ips, threads, tls_port, http_port,
        max_sticky_slots, addr, colo,
    ) {
        Ok(overrides) => overrides,
        Err(e) => return ActionResult::from_error(e),
    };
    config.apply_overrides(&overrides);

    service.update_config(config);

    match service.start_with_ips(
        overrides.ip_file.as_deref(),
        overrides.ip_content.as_deref(),
    ) {
        Ok(()) => ActionResult::ok(),
        Err(e) => ActionResult::from_error(e),
    }
}

#[frb]
pub fn stop_service() -> ActionResult {
    let service = get_service();
    match get_runtime().block_on(service.stop()) {
        Ok(()) => ActionResult::ok(),
        Err(e) => ActionResult::from_error(e),
    }
}

#[frb]
pub fn get_status() -> StatusInfo {
    let service = get_service();
    let core_status = service.build_full_status();

    StatusInfo {
        running: core_status.running,
        uptime_secs: core_status.uptime_secs,
        next_health_check: core_status.next_health_check,
        health_check_interval: core_status.health_check_interval,
        primary_count: core_status.primary_count as i32,
        primary_target: core_status.primary_target as i32,
        backup_count: core_status.backup_count as i32,
        backup_target: core_status.backup_target as i32,
        primary_ips: core_status.primary_ips.into_iter().map(|ip| IpInfo {
            ip: ip.ip,
            delay: ip.delay,
            loss: ip.loss,
            samples: ip.samples as i32,
            colo: ip.colo,
        }).collect(),
        backup_ips: core_status.backup_ips.into_iter().map(|ip| IpInfo {
            ip: ip.ip,
            delay: ip.delay,
            loss: ip.loss,
            samples: ip.samples as i32,
            colo: ip.colo,
        }).collect(),
        sticky_ips: core_status.sticky_ips,
    }
}

#[frb]
pub fn get_config() -> ConfigInfo {
    let service = get_service();
    let config = service.get_config();
    ConfigInfo {
        ip_file: config.ip_file,
        http: config.http,
        delay_limit: config.delay_limit,
        tlr: config.tlr,
        ips: config.ips as i32,
        threads: config.threads as i32,
        tls_port: config.tls_port as i32,
        http_port: config.http_port as i32,
        max_sticky_slots: config.max_sticky_slots as i32,
        addr: config.addr.to_string(),
        colo: config.colo,
    }
}

#[frb]
pub fn get_logs() -> Vec<LogItem> {
    let buffer = cfnat::log::get_log_buffer();
    buffer.get_all().into_iter().map(LogItem::from).collect()
}

#[frb]
pub fn clear_logs() {
    let buffer = cfnat::log::get_log_buffer();
    buffer.clear();
}