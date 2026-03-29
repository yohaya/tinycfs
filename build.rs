fn main() {
    let pkg_version =
        std::env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.0.0".to_string());

    let build_num = std::env::var("TINYCFS_BUILD_NUMBER");
    let git_sha = std::env::var("TINYCFS_GIT_SHA")
        .map(|s| s[..8.min(s.len())].to_string())
        .unwrap_or_else(|_| "local".to_string());

    let full_version = match build_num {
        Ok(n) => format!("{pkg_version}+build.{n}.{git_sha}"),
        Err(_) => format!("{pkg_version}+dev"),
    };

    println!("cargo:rustc-env=TINYCFS_VERSION={full_version}");
    println!("cargo:rerun-if-env-changed=TINYCFS_BUILD_NUMBER");
    println!("cargo:rerun-if-env-changed=TINYCFS_GIT_SHA");
}
