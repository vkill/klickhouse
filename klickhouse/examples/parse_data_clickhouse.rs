/*
Refer to https://clickhouse.com/docs/en/integrations/data-formats/binary-native#exporting-in-a-native-clickhouse-format

wget https://clickhouse.com/docs/assets/files/data-bf32b3ff4cbe28d7b4ae0b810eef9861.clickhouse -O /tmp/data.clickhouse

cargo run -p klickhouse --example parse_data_clickhouse -- /tmp/data.clickhouse
*/

use std::env;

use klickhouse::{block::Block, RawRow, Row as _};
use tokio::fs;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .parse_env(env_logger::Env::default().default_filter_or("info"))
        .init();

    //
    let path = env::args().nth(1).ok_or("path missing")?;
    let mut file = fs::OpenOptions::new().read(true).open(path).await?;

    //
    let mut block = Block::read(&mut file, 0).await?;

    let rows = block
        .take_iter_rows()
        .filter(|x| !x.is_empty())
        .map(|m| RawRow::deserialize_row(m))
        .collect::<Result<Vec<_>, _>>()?;

    println!("rows len:{}", rows.len());
    println!("rows first:{:?}", rows.first());

    Ok(())
}
