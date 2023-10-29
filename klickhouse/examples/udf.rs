/*
cargo build --release -p klickhouse --example udf --target x86_64-unknown-linux-gnu

sudo cp target/x86_64-unknown-linux-gnu/release/examples/udf /var/lib/clickhouse/user_scripts/klickhouse_udf
sudo chmod +x /var/lib/clickhouse/user_scripts/klickhouse_udf
sudo chown clickhouse:clickhouse /var/lib/clickhouse/user_scripts/klickhouse_udf
sudo ls -al /var/lib/clickhouse/user_scripts

clickhouse-client
 :) SELECT * FROM executable('klickhouse_udf', Native, 'count UInt32', (SELECT * FROM numbers(10)));
 :) SELECT * FROM executable('klickhouse_udf', Native, 'count UInt32', (SELECT * FROM numbers(10)), (SELECT * FROM numbers(20)));
 :) SELECT * FROM executable('klickhouse_udf', Native, 'count UInt32', (SELECT * FROM numbers(10)), (SELECT * FROM numbers(20)), (SELECT * FROM numbers(30)));
 :) SELECT * FROM executable('klickhouse_udf', Native, 'count UInt32', (SELECT * FROM numbers(10)), (SELECT * FROM numbers(20)), (SELECT * FROM numbers(30)), (SELECT * FROM numbers(40)));

*/

use std::os::fd::FromRawFd as _;

use indexmap::IndexMap;
use klickhouse::{block::Block, KlickhouseError, RawRow, Row as _, Type, Value};
use tokio::{fs, io};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    //
    let mut column_data_count_values = vec![];

    //
    let mut block = Block::read(&mut io::stdin(), 0).await?;
    let rows = block
        .take_iter_rows()
        .filter(|x| !x.is_empty())
        .map(|m| RawRow::deserialize_row(m))
        .collect::<Result<Vec<_>, _>>()?;
    column_data_count_values.push(Value::UInt32(rows.len() as u32));

    //
    // 0 = stdin
    // 1 = stdout
    // 2 = stderr
    for fd in 3..=13 {
        match Block::read(&mut unsafe { fs::File::from_raw_fd(fd) }, 0).await {
            Ok(mut block) => {
                let rows = block
                    .take_iter_rows()
                    .filter(|x| !x.is_empty())
                    .map(|m| RawRow::deserialize_row(m))
                    .collect::<Result<Vec<_>, _>>()?;
                column_data_count_values.push(Value::UInt32(rows.len() as u32));
            }
            Err(KlickhouseError::Io(io_err))
                if io_err.kind() == std::io::ErrorKind::InvalidInput =>
            {
                break;
            }
            Err(KlickhouseError::Io(io_err))
                if io_err.kind() as u32 > std::io::ErrorKind::Other as u32 =>
            {
                break;
            }
            Err(err) => {
                panic!("{err}")
            }
        }
    }

    //
    let block = Block {
        info: Default::default(),
        rows: column_data_count_values.len() as u64,
        column_types: IndexMap::from([("count".to_owned(), Type::UInt32)]),
        column_data: IndexMap::from([("count".to_owned(), column_data_count_values)]),
    };
    block.write(&mut io::stdout(), 0).await?;

    Ok(())
}
