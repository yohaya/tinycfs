fn main() {
    let pkg_version =
        std::env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.1.0".to_string());

    // Use MAJOR.MINOR from Cargo.toml and the CI run number as PATCH so that
    // every push to main produces a unique, monotonically increasing version:
    //   CI:    0.1.42  (run 42)
    //   local: 0.1.0-dev
    let major_minor = pkg_version
        .rsplitn(2, '.')
        .nth(1)
        .unwrap_or("0.1")
        .to_string();

    let (version, build_time) = match std::env::var("TINYCFS_BUILD_NUMBER") {
        Ok(n) => {
            let t = std::env::var("TINYCFS_BUILD_TIME")
                .unwrap_or_else(|_| "unknown".to_string());
            (format!("{major_minor}.{n}"), t)
        }
        Err(_) => (format!("{major_minor}.0-dev"), "local".to_string()),
    };

    println!("cargo:rustc-env=TINYCFS_VERSION={version}");
    println!("cargo:rustc-env=TINYCFS_BUILD_TIME={build_time}");
    println!("cargo:rerun-if-env-changed=TINYCFS_BUILD_NUMBER");
    println!("cargo:rerun-if-env-changed=TINYCFS_BUILD_TIME");
}
