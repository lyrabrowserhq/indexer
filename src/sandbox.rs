use landlock::{
    ABI, Access, AccessFs, PathBeneath, PathFd, Ruleset, RulesetAttr, RulesetCreatedAttr,
};
use std::path::Path;
use tracing::{info, warn};

/// confine the process to the data dir (rw), the pki trust store and the
/// resolver config (ro). best-effort: on kernels without landlock we log and
/// continue unconfined, same posture as the app itself.
pub fn apply(data_dir: &Path) {
    let abi = ABI::V5;
    let status = match Ruleset::default()
        .handle_access(AccessFs::from_all(abi))
        .map(|r| r.create())
    {
        Ok(Ok(created)) => created,
        Ok(Err(e)) => {
            warn!(error = %e, "landlock ruleset unavailable, running unsandboxed");
            return;
        }
        Err(e) => {
            warn!(error = %e, "landlock unavailable, running unsandboxed");
            return;
        }
    };

    let mut rs = status;
    for dir in [
        "/etc/ssl",
        "/etc/ssl/certs",
        "/etc/ca-certificates",
        "/etc/pki",
        "/etc/resolv.conf",
        "/etc/hosts",
        "/etc/nsswitch.conf",
    ] {
        let p = Path::new(dir);
        if p.exists()
            && let Ok(fd) = PathFd::new(p)
        {
            rs = match rs.add_rule(PathBeneath::new(fd, AccessFs::ReadFile | AccessFs::ReadDir)) {
                Ok(r) => r,
                Err(e) => {
                    warn!(dir, error = %e, "landlock rule failed");
                    return;
                }
            };
        }
    }
    if let Ok(fd) = PathFd::new(data_dir) {
        rs = match rs.add_rule(PathBeneath::new(fd, AccessFs::from_all(ABI::V5))) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "landlock data dir rule failed");
                return;
            }
        };
    }
    match rs.restrict_self() {
        Ok(status) => info!(status = ?status.ruleset, "landlock applied"),
        Err(e) => warn!(error = %e, "landlock restrict failed"),
    }
}
