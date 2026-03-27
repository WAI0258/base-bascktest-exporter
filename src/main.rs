fn main() {
    if let Err(error) = base_backtest_exporter::cli::run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
