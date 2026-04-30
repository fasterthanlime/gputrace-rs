use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

fn main() {
    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("install") => {
            reject_extra_args(args);
            install();
        }
        Some(command) => {
            eprintln!("unknown command: {command}");
            usage();
            std::process::exit(1);
        }
        None => {
            usage();
            std::process::exit(1);
        }
    }
}

fn usage() {
    eprintln!("usage: cargo xtask <command>");
    eprintln!("available commands:");
    eprintln!("  install");
}

fn reject_extra_args(args: impl Iterator<Item = String>) {
    let args: Vec<String> = args.collect();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        usage();
        std::process::exit(0);
    }
    if let Some(arg) = args.first() {
        eprintln!("unknown install option: {arg}");
        usage();
        std::process::exit(1);
    }
}

fn install() {
    let repo = workspace_root();
    let release_binary = repo.join("target/release/gputrace");
    let installed_binary = cargo_bin_dir().join("gputrace");

    run(
        Command::new("cargo")
            .current_dir(&repo)
            .args(["build", "--release", "--bin", "gputrace"]),
        "build release gputrace binary",
    );

    fs::create_dir_all(
        installed_binary
            .parent()
            .expect("installed binary has parent"),
    )
    .expect("failed to create ~/.cargo/bin");
    fs::copy(&release_binary, &installed_binary).unwrap_or_else(|error| {
        panic!(
            "failed to copy {} to {}: {error}",
            release_binary.display(),
            installed_binary.display()
        )
    });
    println!("copied gputrace to {}", installed_binary.display());

    sign_installed_binary(&installed_binary);
    if !verify_installed_binary(&installed_binary) {
        std::process::exit(1);
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("xtask lives under crates/xtask")
        .to_owned()
}

fn cargo_bin_dir() -> PathBuf {
    if let Some(path) = env::var_os("CARGO_HOME") {
        return PathBuf::from(path).join("bin");
    }
    PathBuf::from(env::var_os("HOME").expect("HOME is not set"))
        .join(".cargo")
        .join("bin")
}

fn verify_installed_binary(binary: &Path) -> bool {
    println!("verifying installation...");
    let output = Command::new(binary)
        .arg("--version")
        .output()
        .unwrap_or_else(|error| panic!("failed to run {} --version: {error}", binary.display()));
    if !output.status.success() {
        eprintln!("gputrace --version failed");
        eprintln!("{}", String::from_utf8_lossy(&output.stderr));
        return false;
    }
    print!("{}", String::from_utf8_lossy(&output.stdout));
    true
}

#[cfg(target_os = "macos")]
fn sign_installed_binary(binary: &Path) {
    let identity = codesign_identity();
    let identity_arg = identity.as_deref().unwrap_or("-");
    if let Some(identity) = identity.as_deref() {
        println!("signing installed binary with identity: {identity}");
    } else {
        println!("signing installed binary ad-hoc");
    }

    let mut command = Command::new("codesign");
    command.args([
        "--force",
        "--sign",
        identity_arg,
        "--identifier",
        "com.tmc.gputrace",
    ]);
    if identity.is_some() {
        command.arg("--timestamp");
    }
    command.arg(binary);
    run(&mut command, "codesign installed gputrace binary");
}

#[cfg(not(target_os = "macos"))]
fn sign_installed_binary(_binary: &Path) {}

#[cfg(target_os = "macos")]
fn codesign_identity() -> Option<String> {
    let output = Command::new("security")
        .args(["find-identity", "-v", "-p", "codesigning"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .find_map(|line| {
            line.split_once('"')
                .and_then(|(_, rest)| rest.split_once('"'))
        })
        .map(|(identity, _)| identity.to_owned())
}

fn run(command: &mut Command, description: &str) {
    let status = command
        .stdin(Stdio::null())
        .status()
        .unwrap_or_else(|error| panic!("failed to {description}: {error}"));
    if !status.success() {
        eprintln!("{description} failed");
        std::process::exit(status.code().unwrap_or(1));
    }
}
