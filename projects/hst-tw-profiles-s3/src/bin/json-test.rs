use hst_tw_profiles_s3::json::Counter;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stdin = std::io::stdin();
    let mut handle = stdin.lock();

    let jc = Counter(0);

    Ok(())
}
