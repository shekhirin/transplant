use std::env;

use actix_web::HttpServer;
use main_error::MainError;
use meilisearch_http::{create_app, Data, Opt};
use structopt::StructOpt;

//mod analytics;

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[actix_web::main]
async fn main() -> Result<(), MainError> {
    let opt = Opt::from_args();

    #[cfg(all(not(debug_assertions), feature = "sentry"))]
    let _sentry = sentry::init((
        if !opt.no_sentry {
            Some(opt.sentry_dsn.clone())
        } else {
            None
        },
        sentry::ClientOptions {
            release: sentry::release_name!(),
            ..Default::default()
        },
    ));

    match opt.env.as_ref() {
        "production" => {
            if opt.master_key.is_none() {
                return Err(
                    "In production mode, the environment variable MEILI_MASTER_KEY is mandatory"
                        .into(),
                );
            }

            #[cfg(all(not(debug_assertions), feature = "sentry"))]
            if !opt.no_sentry && _sentry.is_enabled() {
                sentry::integrations::panic::register_panic_handler(); // TODO: This shouldn't be needed when upgrading to sentry 0.19.0. These integrations are turned on by default when using `sentry::init`.
                sentry::integrations::env_logger::init(None, Default::default());
            }
        }
        "development" => {
            env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
                .init();
        }
        _ => unreachable!(),
    }

    //if let Some(path) = &opt.import_snapshot {
    //snapshot::load_snapshot(&opt.db_path, path, opt.ignore_snapshot_if_db_exists, opt.ignore_missing_snapshot)?;
    //}

    let data = Data::new(opt.clone())?;

    //if !opt.no_analytics {
    //let analytics_data = data.clone();
    //let analytics_opt = opt.clone();
    //thread::spawn(move || analytics::analytics_sender(analytics_data, analytics_opt));
    //}

    //if let Some(path) = &opt.import_dump {
    //dump::import_dump(&data, path, opt.dump_batch_size)?;
    //}

    //if opt.schedule_snapshot {
    //snapshot::schedule_snapshot(data.clone(), &opt.snapshot_dir, opt.snapshot_interval_sec.unwrap_or(86400))?;
    //}

    print_launch_resume(&opt, &data);

    let enable_frontend = opt.env != "production";

    run_http(data, opt, enable_frontend).await?;

    Ok(())
}

async fn run_http(
    data: Data,
    opt: Opt,
    enable_frontend: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let http_server = HttpServer::new(move || create_app!(&data, enable_frontend))
        // Disable signals allows the server to terminate immediately when a user enter CTRL-C
        .disable_signals();

    if let Some(config) = opt.get_ssl_config()? {
        http_server
            .bind_rustls(opt.http_addr, config)?
            .run()
            .await?;
    } else {
        http_server.bind(opt.http_addr)?.run().await?;
    }
    Ok(())
}

pub fn print_launch_resume(opt: &Opt, data: &Data) {
    let commit_sha = match option_env!("COMMIT_SHA") {
        Some("") | None => env!("VERGEN_SHA"),
        Some(commit_sha) => commit_sha
    };
    let commit_date = match option_env!("COMMIT_DATE") {
        Some("") | None => env!("VERGEN_COMMIT_DATE"),
        Some(commit_date) => commit_date
    };

    let ascii_name = r#"
888b     d888          d8b 888 d8b  .d8888b.                                    888
8888b   d8888          Y8P 888 Y8P d88P  Y88b                                   888
88888b.d88888              888     Y88b.                                        888
888Y88888P888  .d88b.  888 888 888  "Y888b.    .d88b.   8888b.  888d888 .d8888b 88888b.
888 Y888P 888 d8P  Y8b 888 888 888     "Y88b. d8P  Y8b     "88b 888P"  d88P"    888 "88b
888  Y8P  888 88888888 888 888 888       "888 88888888 .d888888 888    888      888  888
888   "   888 Y8b.     888 888 888 Y88b  d88P Y8b.     888  888 888    Y88b.    888  888
888       888  "Y8888  888 888 888  "Y8888P"   "Y8888  "Y888888 888     "Y8888P 888  888
"#;

    eprintln!("{}", ascii_name);

    eprintln!("Database path:\t\t{:?}", opt.db_path);
    eprintln!("Server listening on:\t\"http://{}\"", opt.http_addr);
    eprintln!("Environment:\t\t{:?}", opt.env);
    eprintln!("Commit SHA:\t\t{:?}", commit_sha.to_string());
    eprintln!("Commit date:\t\t{:?}", commit_date.to_string());
    eprintln!(
        "Package version:\t{:?}",
        env!("CARGO_PKG_VERSION").to_string()
    );

    #[cfg(all(not(debug_assertions), feature = "sentry"))]
    eprintln!(
        "Sentry DSN:\t\t{:?}",
        if !opt.no_sentry {
            &opt.sentry_dsn
        } else {
            "Disabled"
        }
    );

    eprintln!(
        "Anonymous telemetry:\t{:?}",
        if !opt.no_analytics {
            "Enabled"
        } else {
            "Disabled"
        }
    );

    eprintln!();

    if data.api_keys().master.is_some() {
        eprintln!("A Master Key has been set. Requests to MeiliSearch won't be authorized unless you provide an authentication key.");
    } else {
        eprintln!("No master key found; The server will accept unidentified requests. \
            If you need some protection in development mode, please export a key: export MEILI_MASTER_KEY=xxx");
    }

    eprintln!();
    eprintln!("Documentation:\t\thttps://docs.meilisearch.com");
    eprintln!("Source code:\t\thttps://github.com/meilisearch/meilisearch");
    eprintln!("Contact:\t\thttps://docs.meilisearch.com/resources/contact.html or bonjour@meilisearch.com");
    eprintln!();
}
