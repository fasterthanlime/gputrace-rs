use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

fn main() {
    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("install") => install(InstallOptions::parse(args)),
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
    eprintln!("  install [--metal-entitlements]");
}

#[derive(Debug, Default)]
struct InstallOptions {
    metal_entitlements: bool,
}

impl InstallOptions {
    fn parse(args: impl Iterator<Item = String>) -> Self {
        let mut options = Self::default();
        for arg in args {
            match arg.as_str() {
                "--metal-entitlements" => options.metal_entitlements = true,
                "--help" | "-h" => {
                    usage();
                    std::process::exit(0);
                }
                _ => {
                    eprintln!("unknown install option: {arg}");
                    usage();
                    std::process::exit(1);
                }
            }
        }
        options
    }
}

fn install(options: InstallOptions) {
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

    sign_installed_binary(&repo, &installed_binary, options.metal_entitlements);
    if !verify_installed_binary(&installed_binary) {
        if options.metal_entitlements {
            eprintln!(
                "gputrace did not run after Metal entitlement signing; restoring a usable signature without those entitlements"
            );
            sign_installed_binary(&repo, &installed_binary, false);
            if verify_installed_binary(&installed_binary) {
                eprintln!(
                    "installed binary is usable, but Metal entitlement signing is not supported by this local signing setup"
                );
            } else {
                std::process::exit(1);
            }
        } else {
            std::process::exit(1);
        }
    }
    check_accessibility_permission(&installed_binary);
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
fn sign_installed_binary(repo: &Path, binary: &Path, metal_entitlements: bool) {
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
    if metal_entitlements {
        let entitlements = repo.join("gputrace.entitlements");
        println!(
            "adding Metal performance entitlements from {}",
            entitlements.display()
        );
        command.arg("--entitlements").arg(entitlements);
    }
    command.arg(binary);
    run(&mut command, "codesign installed gputrace binary");
}

#[cfg(not(target_os = "macos"))]
fn sign_installed_binary(_repo: &Path, _binary: &Path, _metal_entitlements: bool) {}

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

#[cfg(target_os = "macos")]
fn check_accessibility_permission(binary: &Path) {
    println!("checking Accessibility permission...");
    let output = Command::new(binary)
        .args(["xcode-check-permissions", "--no-prompt"])
        .output()
        .unwrap_or_else(|error| {
            panic!(
                "failed to run {} xcode-check-permissions --no-prompt: {error}",
                binary.display()
            )
        });
    if output.status.success() {
        print!("{}", String::from_utf8_lossy(&output.stdout));
        return;
    }

    eprintln!("Accessibility is not granted for {}.", binary.display());
    eprintln!(
        "Grant it in System Settings > Privacy & Security > Accessibility, or run: {} xcode-check-permissions",
        binary.display()
    );
}

#[cfg(not(target_os = "macos"))]
fn check_accessibility_permission(_binary: &Path) {}

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
