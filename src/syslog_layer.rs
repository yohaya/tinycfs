//! Syslog tracing layer.
//!
//! Forwards WARN and ERROR tracing events to the system syslog (LOG_DAEMON
//! facility) so that operators can detect cluster problems via standard log
//! aggregation tools (journalctl, rsyslog, etc.) without reading stdout.
//!
//! Usage in main():
//!   SyslogLayer::init();           // call openlog once at startup
//!   SyslogLayer::startup(...)      // write a startup notice
//!   // then add SyslogLayer as a tracing subscriber layer

use std::ffi::CString;
use std::fmt;

use libc::{c_int, LOG_DAEMON, LOG_ERR, LOG_INFO, LOG_PID, LOG_WARNING};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

// ── syslog(3) wrappers ────────────────────────────────────────────────────────

/// Null-terminate a string for use with syslog(3), stripping any embedded NUL
/// bytes that would truncate the message.
fn to_cstring(s: &str) -> CString {
    CString::new(s.replace('\0', "")).unwrap_or_else(|_| CString::new("(invalid)").unwrap())
}

fn raw_syslog(priority: c_int, msg: &str) {
    let msg_c = to_cstring(msg);
    let fmt_c = b"%s\0"; // always use %s to avoid format-string injection
    unsafe {
        libc::syslog(priority, fmt_c.as_ptr() as _, msg_c.as_ptr());
    }
}

// ── Visitor — extracts the "message" field from a tracing Event ───────────────

struct MessageVisitor {
    message: String,
}

impl Visit for MessageVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message.push_str(value);
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            use fmt::Write;
            let _ = write!(self.message, "{:?}", value);
        }
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        if field.name() == "message" {
            use fmt::Write;
            let _ = write!(self.message, "{}", value);
        }
    }
}

// ── Public API ────────────────────────────────────────────────────────────────

/// A `tracing_subscriber` layer that forwards WARN and ERROR events to syslog.
pub struct SyslogLayer;

impl SyslogLayer {
    /// Open the syslog connection with identity "tinycfs" and LOG_DAEMON
    /// facility.  Call exactly once before adding this layer to a subscriber.
    pub fn init() {
        // The ident pointer passed to openlog(3) must remain valid for the
        // entire process lifetime.  A `&'static [u8]` literal satisfies this.
        static IDENT: &[u8] = b"tinycfs\0";
        unsafe {
            libc::openlog(IDENT.as_ptr() as _, LOG_PID, LOG_DAEMON);
        }
    }

    /// Write a LOG_INFO startup notice directly to syslog.
    ///
    /// Call this after `init()` and after the config is loaded, so the message
    /// can include cluster details that help operators verify the node started
    /// with the right configuration.
    pub fn startup(msg: &str) {
        raw_syslog(LOG_INFO, msg);
    }
}

impl<S: Subscriber> Layer<S> for SyslogLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let level = *event.metadata().level();
        let priority: c_int = match level {
            Level::ERROR => LOG_ERR,
            Level::WARN => LOG_WARNING,
            _ => return, // only WARN/ERROR are written to syslog
        };

        let target = event.metadata().target();
        let mut visitor = MessageVisitor { message: String::new() };
        event.record(&mut visitor);

        let text = if visitor.message.is_empty() {
            format!("[{}]", target)
        } else {
            format!("[{}] {}", target, visitor.message)
        };

        raw_syslog(priority, &text);
    }
}
