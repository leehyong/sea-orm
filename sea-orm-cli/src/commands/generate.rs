use crate::{DateTimeCrate, GenerateSubcommands};
use core::time;
use sea_orm_codegen::{
    DateTimeCrate as CodegenDateTimeCrate, EntityTransformer, EntityWriterContext, OutputFile,
    WithPrelude, WithSerde,
};
use sea_schema::mysql::MySql;
use sea_schema::postgres::Postgres;
use sea_schema::sqlite::Sqlite;
use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::{error::Error, fs, io::Write, path::Path, process::Command, str::FromStr};
use tracing_subscriber::{prelude::*, EnvFilter};
use url::Url;

pub async fn run_generate_command(
    command: GenerateSubcommands,
    verbose: bool,
) -> Result<(), Box<dyn Error>> {
    match command {
        GenerateSubcommands::Entity {
            compact_format: _,
            expanded_format,
            include_hidden_tables,
            tables,
            ignore_tables,
            max_connections,
            acquire_timeout,
            output_dir,
            proto_dir,
            crud_dir,
            database_schema,
            database_url,
            with_prelude,
            with_serde,
            serde_skip_deserializing_primary_key,
            serde_skip_hidden_column,
            with_copy_enums,
            date_time_crate,
            lib,
            model_extra_derives,
            model_extra_attributes,
            enum_extra_derives,
            enum_extra_attributes,
            seaography,
            impl_active_model_behavior,
        } => {
            if verbose {
                let _ = tracing_subscriber::fmt()
                    .with_max_level(tracing::Level::DEBUG)
                    .with_test_writer()
                    .try_init();
            } else {
                let filter_layer = EnvFilter::try_new("sea_orm_codegen=info").unwrap();
                let fmt_layer = tracing_subscriber::fmt::layer()
                    .with_target(false)
                    .with_level(false)
                    .without_time();

                let _ = tracing_subscriber::registry()
                    .with(filter_layer)
                    .with(fmt_layer)
                    .try_init();
            }

            // The database should be a valid URL that can be parsed
            // protocol://username:password@host/database_name
            let url = Url::parse(&database_url)?;

            // Make sure we have all the required url components
            //
            // Missing scheme will have been caught by the Url::parse() call
            // above
            let is_sqlite = url.scheme() == "sqlite";

            // Closures for filtering tables
            let filter_tables =
                |table: &String| -> bool { tables.is_empty() || tables.contains(table) };

            let filter_hidden_tables = |table: &str| -> bool {
                if include_hidden_tables {
                    true
                } else {
                    !table.starts_with('_')
                }
            };

            let filter_skip_tables = |table: &String| -> bool { !ignore_tables.contains(table) };

            let database_name = if !is_sqlite {
                // The database name should be the first element of the path string
                //
                // Throwing an error if there is no database name since it might be
                // accepted by the database without it, while we're looking to dump
                // information from a particular database
                let database_name = url
                    .path_segments()
                    .unwrap_or_else(|| {
                        panic!(
                            "There is no database name as part of the url path: {}",
                            url.as_str()
                        )
                    })
                    .next()
                    .unwrap();

                // An empty string as the database name is also an error
                if database_name.is_empty() {
                    panic!(
                        "There is no database name as part of the url path: {}",
                        url.as_str()
                    );
                }

                database_name
            } else {
                Default::default()
            };

            let (schema_name, table_stmts) = match url.scheme() {
                "mysql" => {
                    use sea_schema::mysql::discovery::SchemaDiscovery;
                    use sqlx::MySql;

                    println!("Connecting to MySQL ...");
                    let connection =
                        sqlx_connect::<MySql>(max_connections, acquire_timeout, url.as_str(), None)
                            .await?;

                    println!("Discovering schema ...");
                    let schema_discovery = SchemaDiscovery::new(connection, database_name);
                    let schema = schema_discovery.discover().await?;
                    let table_stmts = schema
                        .tables
                        .into_iter()
                        .filter(|schema| filter_tables(&schema.info.name))
                        .filter(|schema| filter_hidden_tables(&schema.info.name))
                        .filter(|schema| filter_skip_tables(&schema.info.name))
                        .map(|schema| schema.write())
                        .collect();
                    (None, table_stmts)
                }
                "sqlite" => {
                    use sea_schema::sqlite::discovery::SchemaDiscovery;
                    use sqlx::Sqlite;

                    println!("Connecting to SQLite ...");
                    let connection = sqlx_connect::<Sqlite>(
                        max_connections,
                        acquire_timeout,
                        url.as_str(),
                        None,
                    )
                    .await?;

                    println!("Discovering schema ...");
                    let schema_discovery = SchemaDiscovery::new(connection);
                    let schema = schema_discovery
                        .discover()
                        .await?
                        .merge_indexes_into_table();
                    let table_stmts = schema
                        .tables
                        .into_iter()
                        .filter(|schema| filter_tables(&schema.name))
                        .filter(|schema| filter_hidden_tables(&schema.name))
                        .filter(|schema| filter_skip_tables(&schema.name))
                        .map(|schema| schema.write())
                        .collect();
                    (None, table_stmts)
                }
                "postgres" | "postgresql" => {
                    use sea_schema::postgres::discovery::SchemaDiscovery;
                    use sqlx::Postgres;

                    println!("Connecting to Postgres ...");
                    let schema = database_schema.as_deref().unwrap_or("public");
                    let connection = sqlx_connect::<Postgres>(
                        max_connections,
                        acquire_timeout,
                        url.as_str(),
                        Some(schema),
                    )
                    .await?;
                    println!("Discovering schema ...");
                    let schema_discovery = SchemaDiscovery::new(connection, schema);
                    let schema = schema_discovery.discover().await?;
                    let table_stmts = schema
                        .tables
                        .into_iter()
                        .filter(|schema| filter_tables(&schema.info.name))
                        .filter(|schema| filter_hidden_tables(&schema.info.name))
                        .filter(|schema| filter_skip_tables(&schema.info.name))
                        .map(|schema| schema.write())
                        .collect();
                    (database_schema, table_stmts)
                }
                _ => unimplemented!("{} is not supported", url.scheme()),
            };
            println!("... discovered.");

            let writer_context = EntityWriterContext::new(
                expanded_format,
                WithPrelude::from_str(&with_prelude).expect("Invalid prelude option"),
                WithSerde::from_str(&with_serde).expect("Invalid serde derive option"),
                with_copy_enums,
                date_time_crate.into(),
                schema_name,
                lib,
                serde_skip_deserializing_primary_key,
                serde_skip_hidden_column,
                model_extra_derives,
                model_extra_attributes,
                enum_extra_derives,
                enum_extra_attributes,
                seaography,
                impl_active_model_behavior,
            );
            let entity_writer = EntityTransformer::transform(table_stmts)?;

            if !proto_dir.is_empty() {
                let dir = Path::new(&proto_dir);
                fs::create_dir_all(dir)?;
                // 通过 这个json 文件以保证 生成的proto文件的字段值不变化
                let meta_json_file_path = dir.join("generated.meta.json");
                // 打开文件

                // 创建一个字符串变量来存储文件内容
                let mut contents = String::new();
                if fs::exists(&meta_json_file_path).unwrap() {
                    let mut file = fs::File::open(&meta_json_file_path)?;
                    file.read_to_string(&mut contents)?;
                }
                // 读取文件内容到字符串
                let mut meta: BTreeMap<String, BTreeMap<String, usize>>;
                if contents.is_empty() {
                    meta = BTreeMap::new();
                } else {
                    meta = serde_json::from_str(contents.as_str()).unwrap();
                }
                let output = entity_writer.generate_proto(&writer_context, &mut meta);
                for OutputFile { name, content } in output.files.iter() {
                    let file_path = dir.join(name);
                    println!("Writing {}", file_path.display());
                    let mut file = fs::File::create(file_path)?;
                    file.write_all(content.as_bytes())?;
                }
                if !meta.is_empty() {
                    let mut file = fs::File::create(meta_json_file_path)?;
                    file.write_all(serde_json::to_string(&meta).unwrap().as_bytes())?;
                }
            }

            if !crud_dir.is_empty() {
                let dir = Path::new(&crud_dir);
                fs::create_dir_all(dir)?;
                let table_validate_json_path = dir.join("table_crud_validate.json");
                let mut table_validate_map: HashMap<String, String> = HashMap::new();
                println!("table_crud_validate.json:{}", table_validate_json_path.display());
                if fs::exists(&table_validate_json_path).unwrap() {
                    let mut table_validate_json_file = fs::File::open(&table_validate_json_path)?;
                    let mut contents = String::new();
                    table_validate_json_file.read_to_string(&mut contents)?;
                    if !contents.is_empty() {
                        table_validate_map = serde_json::from_str(contents.as_str()).unwrap();
                        println!("{}", contents);
                    }

                }
                let output = entity_writer
                    .generate_derive_auto_simple_curd_api(&writer_context, &table_validate_map);
                for OutputFile { name, content } in output.files.iter() {
                    let file_path = dir.join(name);
                    println!("Writing {}", file_path.display());
                    let mut file = fs::File::create(file_path)?;
                    file.write_all(content.as_bytes())?;
                }

                // 输出那些使用左右值编码的表
                for OutputFile { name, content } in entity_writer
                    .generate_tree_view_tables(&writer_context).files.iter() {
                    let file_path = dir.join(name);
                    println!("Writing {}", file_path.display());
                    let mut file = fs::File::create(file_path)?;
                    file.write_all(content.as_bytes())?;
                }
            }
            let output = entity_writer.generate(&writer_context);
            let dir = Path::new(&output_dir);
            fs::create_dir_all(dir)?;

            for OutputFile { name, content } in output.files.iter() {
                let file_path = dir.join(name);
                println!("Writing {}", file_path.display());
                let mut file = fs::File::create(file_path)?;
                file.write_all(content.as_bytes())?;
            }

            // Format each of the files
            for OutputFile { name, .. } in output.files.iter() {
                let exit_status = Command::new("rustfmt").arg(dir.join(name)).status()?; // Get the status code
                if !exit_status.success() {
                    // Propagate the error if any
                    return Err(format!("Fail to format file `{name}`").into());
                }
            }

            println!("... Done.");
        }
    }

    Ok(())
}

async fn sqlx_connect<DB>(
    max_connections: u32,
    acquire_timeout: u64,
    url: &str,
    schema: Option<&str>,
) -> Result<sqlx::Pool<DB>, Box<dyn Error>>
where
    DB: sqlx::Database,
    for<'a> &'a mut <DB as sqlx::Database>::Connection: sqlx::Executor<'a>,
{
    let mut pool_options = sqlx::pool::PoolOptions::<DB>::new()
        .max_connections(max_connections)
        .acquire_timeout(time::Duration::from_secs(acquire_timeout));
    // Set search_path for Postgres, E.g. Some("public") by default
    // MySQL & SQLite connection initialize with schema `None`
    if let Some(schema) = schema {
        let sql = format!("SET search_path = '{schema}'");
        pool_options = pool_options.after_connect(move |conn, _| {
            let sql = sql.clone();
            Box::pin(async move {
                sqlx::Executor::execute(conn, sql.as_str())
                    .await
                    .map(|_| ())
            })
        });
    }
    pool_options.connect(url).await.map_err(Into::into)
}

impl From<DateTimeCrate> for CodegenDateTimeCrate {
    fn from(date_time_crate: DateTimeCrate) -> CodegenDateTimeCrate {
        match date_time_crate {
            DateTimeCrate::Chrono => CodegenDateTimeCrate::Chrono,
            DateTimeCrate::Time => CodegenDateTimeCrate::Time,
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use crate::{Cli, Commands};

    #[test]
    #[should_panic(
        expected = "called `Result::unwrap()` on an `Err` value: RelativeUrlWithoutBase"
    )]
    fn test_generate_entity_no_protocol() {
        let cli = Cli::parse_from([
            "sea-orm-cli",
            "generate",
            "entity",
            "--database-url",
            "://root:root@localhost:3306/database",
        ]);

        match cli.command {
            Commands::Generate { command } => {
                smol::block_on(run_generate_command(command, cli.verbose)).unwrap();
            }
            _ => unreachable!(),
        }
    }

    #[test]
    #[should_panic(
        expected = "There is no database name as part of the url path: postgresql://root:root@localhost:3306"
    )]
    fn test_generate_entity_no_database_section() {
        let cli = Cli::parse_from([
            "sea-orm-cli",
            "generate",
            "entity",
            "--database-url",
            "postgresql://root:root@localhost:3306",
        ]);

        match cli.command {
            Commands::Generate { command } => {
                smol::block_on(run_generate_command(command, cli.verbose)).unwrap();
            }
            _ => unreachable!(),
        }
    }

    #[test]
    #[should_panic(
        expected = "There is no database name as part of the url path: mysql://root:root@localhost:3306/"
    )]
    fn test_generate_entity_no_database_path() {
        let cli = Cli::parse_from([
            "sea-orm-cli",
            "generate",
            "entity",
            "--database-url",
            "mysql://root:root@localhost:3306/",
        ]);

        match cli.command {
            Commands::Generate { command } => {
                smol::block_on(run_generate_command(command, cli.verbose)).unwrap();
            }
            _ => unreachable!(),
        }
    }

    #[test]
    #[should_panic(expected = "called `Result::unwrap()` on an `Err` value: PoolTimedOut")]
    fn test_generate_entity_no_password() {
        let cli = Cli::parse_from([
            "sea-orm-cli",
            "generate",
            "entity",
            "--database-url",
            "mysql://root:@localhost:3306/database",
        ]);

        match cli.command {
            Commands::Generate { command } => {
                smol::block_on(run_generate_command(command, cli.verbose)).unwrap();
            }
            _ => unreachable!(),
        }
    }

    #[test]
    #[should_panic(expected = "called `Result::unwrap()` on an `Err` value: EmptyHost")]
    fn test_generate_entity_no_host() {
        let cli = Cli::parse_from([
            "sea-orm-cli",
            "generate",
            "entity",
            "--database-url",
            "postgres://root:root@/database",
        ]);

        match cli.command {
            Commands::Generate { command } => {
                smol::block_on(run_generate_command(command, cli.verbose)).unwrap();
            }
            _ => unreachable!(),
        }
    }
}
