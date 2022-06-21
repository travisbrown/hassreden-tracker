use clap::Parser;
use hst_tw_images::{Error, Image};
use std::path::Path;

fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    let _ = hst_cli::init_logging(opts.verbose);
    let output_base_path = Path::new(&opts.output);

    for entry in std::fs::read_dir(opts.input)? {
        let entry = entry?;
        if let Some(image) = entry
            .path()
            .file_name()
            .and_then(|value| value.to_str())
            .and_then(Image::parse_file_name)
        {
            if entry.metadata()?.len() == 0 {
                println!("{}", image);
            } else {
                let output_path = output_base_path.join(image.path());

                if let Some(parent) = output_path.parent() {
                    std::fs::create_dir_all(&parent)?;
                }

                std::fs::rename(entry.path(), &output_path)?;
            }
        } else {
            log::error!("Invalid image path: {:?}", entry.path());
        }
    }

    Ok(())
}

/// Copy images from a flat directory to a structured store.
///
/// Empty files are not copied, but their URLs are printed to stdout.
#[derive(Debug, Parser)]
#[clap(name = "import", version, author)]
struct Opts {
    /// Level of verbosity
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
    /// Input directory
    #[clap(short, long)]
    input: String,
    /// Output directory
    #[clap(short, long)]
    output: String,
}
