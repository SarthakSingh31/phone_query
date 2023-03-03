use std::path::PathBuf;

use cassandra_cpp::*;
use clap::Parser;

#[derive(Parser, Debug)]
struct Args {
    /// Sets/Appends contact points. This *MUST* be set
    /// Ex. "127.0.0.1" "127.0.0.1,127.0.0.2", "server1.domain.com"
    #[arg(short)]
    contact_points: String,
    /// SSL cert to use while connecting
    #[arg(short, long)]
    ssl_cert_path: Option<PathBuf>,
    /// Phone number to filter on
    #[arg(long)]
    filter_user_phone_number: Option<String>,
    /// Minimum of the duration to filter on in the format "hours:minutes:seconds"
    #[arg(long)]
    duration_min: Option<String>,
    /// Maximum of the duration to filter on in the format "hours:minutes:seconds"
    #[arg(long)]
    duration_max: Option<String>,
}

fn from_string_duration(duration: String) -> Option<u64> {
    let mut parts = duration.split(':');

    let hours = parts.next()?.parse::<u64>().ok()?;
    let minutes = parts.next()?.parse::<u64>().ok()?;
    let seconds = parts.next()?.parse::<u64>().ok()?;

    Some(hours * 3600 + minutes * 60 + seconds)
}

fn main() {
    let args = Args::parse();
    let duration_min = args
        .duration_min
        .map(|duration| {
            from_string_duration(duration)
                .expect("Failed to parse duration from the input `duration_min`")
        })
        .unwrap_or(0);
    let duration_max = args
        .duration_max
        .map(|duration| {
            from_string_duration(duration)
                .expect("Failed to parse duration from the input `duration_max`")
        })
        .unwrap_or(u64::MAX);

    let query = stmt!("SELECT * FROM calldrop.userdata;");

    let mut cluster = Cluster::default();
    cluster.set_contact_points(&args.contact_points).unwrap();
    cluster.set_load_balance_round_robin();

    if let Some(ssl_cert_path) = args.ssl_cert_path {
        let mut ssl = Ssl::default();
        let cert = std::fs::read_to_string(ssl_cert_path).expect("Failed to read file");

        ssl.add_trusted_cert(cert.as_str())
            .expect("Failed to add ssl cert");

        ssl.set_verify_flags(&[SslVerifyFlag::PEER_IDENTITY]);

        cluster.set_ssl(&mut ssl);
    }

    match cluster.connect() {
        Ok(ref mut session) => {
            let result = session.execute(&query).wait().unwrap();

            let mut in_duration_range = 0;
            let mut successful_calls = 0;

            for row in result.iter() {
                let duration = row
                    .get_by_name("duration")
                    .expect("Column `duration` not found in the table");
                let duration =
                    from_string_duration(duration).expect("Failed to parse duration from table");

                if duration >= duration_min && duration <= duration_max {
                    if let Some(filter_number) = &args.filter_user_phone_number {
                        let user_phone_number: String = row
                            .get_by_name("user_phone_number")
                            .expect("Column `user_phone_number` not found in the table");

                        if *filter_number != user_phone_number {
                            continue;
                        }
                    }

                    in_duration_range += 1;

                    let call_successful: bool = row
                        .get_by_name("call_successfully_completed")
                        .expect("Column `call_successfully_completed` not found in the table");

                    if call_successful {
                        successful_calls += 1;
                    }
                }
            }

            println!(
                "{successful_calls} / {in_duration_range} ({:.2}) calls were successful",
                successful_calls as f32 / in_duration_range as f32
            );
        }
        err => println!("{:?}", err),
    }
}
