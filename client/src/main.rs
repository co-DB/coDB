use clap::Parser;

#[derive(Parser)]
struct Args {
    database_name: String,

    #[arg(short, long)]
    query: Option<String>,
}
fn main() {
    let args = Args::parse();
}
