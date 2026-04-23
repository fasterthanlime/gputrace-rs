fn main() {
    if let Err(error) = gputrace_rs::cli::run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
