use {
    anyhow::{
        anyhow,
        Context,
        Result,
    },
    clap::Parser,
    solana_sdk::pubkey::Pubkey,
    std::{
        fs::File,
        io::{
            Read,
            Write,
        },
        path::PathBuf,
        str::FromStr,
    },
    toml_edit::{
        value,
        DocumentMut,
        Item,
    },
};

#[derive(Parser, Debug)]
#[command(author, version, about = "1.x.x -> 2.0.0 pyth-agent config migrator")]
struct Args {
    /// Config path to be migrated
    #[arg(short, long)]
    config: PathBuf,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    eprintln!("Loading old config from {}", args.config.display());

    let mut f = File::open(args.config).context("Could not open the config file")?;

    let mut old_cfg_contents = String::new();

    f.read_to_string(&mut old_cfg_contents)?;

    let mut doc: DocumentMut = old_cfg_contents
        .parse()
        .context("Could not parse config file contents as TOML")?;

    let primary_network = doc
        .get_mut("primary_network")
        .ok_or_else(|| anyhow::anyhow!("Could not read mandatory primary_network section"))?;

    eprint!("Migrating primary_network...");
    std::io::stderr().flush()?;
    migrate_network(primary_network)?;
    eprintln!("OK");

    if let Some(secondary_network) = doc.get_mut("secondary_network") {
        eprint!("Migrating secondary_network...");
        std::io::stdout().flush()?;
        migrate_network(secondary_network)?;
        eprintln!("OK");
    } else {
        eprintln!("secondary_network not defined, moving on");
    }

    eprintln!("Migration OK. Result:");
    std::io::stderr().flush()?;

    println!("{}", doc);

    Ok(())
}

/// Generalized migration routine for primary/secondary_network TOML
/// sections. v1.x.x defaults are supplied if unspecified in order to
/// reach the file-based pubkeys on disk.
pub fn migrate_network(network_config: &mut Item) -> Result<()> {
    // Retrieve all key store (sub)paths or supply defaults
    let key_store_root_path: PathBuf = {
        let root_item = network_config
            .get("key_store")
            .and_then(|ks| ks.get("root_path"))
            .cloned()
            // v1.4.0 used PathBuf::default(), meaning current working directory, if unspecified.
            .unwrap_or(value("."));

        let root_str = root_item
            .as_str()
            .ok_or(anyhow!("Could not parse key_store.root_path"))?;

        PathBuf::from(root_str.to_owned())
    };

    let publish_keypair_relpath: PathBuf = {
        let publish_item = network_config
            .get("key_store")
            .and_then(|ks| ks.get("publish_keypair_path"))
            .cloned()
            .unwrap_or(value("publish_key_pair.json"));

        let publish_str = publish_item
            .as_str()
            .ok_or(anyhow!("Could not parse key_store.publish_keypair"))?;

        PathBuf::from(publish_str)
    };

    let program_key_relpath: PathBuf = {
        let program_item = network_config
            .get("key_store")
            .and_then(|ks| ks.get("program_key_path"))
            .cloned()
            .unwrap_or(value("program_key.json"));

        let program_str = program_item
            .as_str()
            .ok_or(anyhow!("Could not parse key_store.program_key"))?;

        PathBuf::from(program_str)
    };

    let mapping_key_relpath: PathBuf = {
        let mapping_item = network_config
            .get("key_store")
            .and_then(|ks| ks.get("mapping_key_path"))
            .cloned()
            .unwrap_or(value("mapping_key.json"));

        let mapping_str = mapping_item
            .as_str()
            .ok_or(anyhow!("Could not parse key_store.mapping_key"))?;

        PathBuf::from(mapping_str)
    };

    let accumulator_key_relpath: Option<PathBuf> = {
        let maybe_item = network_config
            .get("key_store")
            .and_then(|ks| ks.get("accumulator_key_path"));

        match maybe_item {
            Some(item) => {
                let item_str = item.as_str().ok_or(anyhow!(
                    "Could not parse existing key_store.accumulator_key_path"
                ))?;
                Some(PathBuf::from(item_str))
            }
            None => None,
        }
    };

    // We're done reading legacy key store values, remove the
    // subsection from network config if present.
    if let Some(ks_table_like) = network_config
        .get_mut("key_store")
        .and_then(|ks| ks.as_table_like_mut())
    {
        ks_table_like.clear();
    }

    // Attach publish keypair path to legacy key store root path
    let mut publish_keypair_path = key_store_root_path.clone();
    publish_keypair_path.push(publish_keypair_relpath);

    // Extract pubkeys from legacy file paths for other key store values
    let mut program_key_path = key_store_root_path.clone();
    program_key_path.push(program_key_relpath);
    let mut program_key_str = String::new();
    File::open(&program_key_path)
        .context(format!(
            "Could not open program key file at {}",
            program_key_path.display()
        ))?
        .read_to_string(&mut program_key_str)?;
    let program_key =
        Pubkey::from_str(program_key_str.trim()).context("Could not parse program key")?;

    let mut mapping_key_path = key_store_root_path.clone();
    mapping_key_path.push(mapping_key_relpath);
    let mut mapping_key_str = String::new();
    File::open(mapping_key_path)
        .context("Could not open mapping key file")?
        .read_to_string(&mut mapping_key_str)?;
    let mapping_key =
        Pubkey::from_str(mapping_key_str.trim()).context("Could not parse mapping key")?;

    let accumulator_key = if let Some(relpath) = accumulator_key_relpath {
        let mut accumulator_key_path = key_store_root_path.clone();
        accumulator_key_path.push(relpath);
        let mut accumulator_key_str = String::new();
        File::open(accumulator_key_path)
            .context("Could not open accumulator key file")?
            .read_to_string(&mut accumulator_key_str)?;
        let accumulator_key = Pubkey::from_str(accumulator_key_str.trim())
            .context("Could not parse accumulator key")?;

        Some(accumulator_key)
    } else {
        None
    };

    // Inline new key store pubkeys in the section
    network_config["key_store"]["publish_keypair_path"] =
        value(publish_keypair_path.display().to_string());
    network_config["key_store"]["program_key"] = value(program_key.to_string());
    network_config["key_store"]["mapping_key"] = value(mapping_key.to_string());

    if let Some(k) = accumulator_key {
        network_config["key_store"]["accumulator_key"] = value(k.to_string());
    }

    Ok(())
}
