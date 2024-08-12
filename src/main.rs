use bitcoincore_rpc::{Auth, Client, RpcApi};
use lazy_static::lazy_static;
use std::{
    env,
    sync::{Arc, Mutex},
    thread,
    time::Instant,
};

lazy_static! {
    static ref RPC_URL: String = env::var("BITCOIN_RPC_URL").expect("BITCOIN_RPC_URL must be set");
    static ref RPC_USER: String =
        env::var("BITCOIN_RPC_USER").expect("BITCOIN_RPC_USER must be set");
    static ref RPC_PASSWORD: String =
        env::var("BITCOIN_RPC_PASSWORD").expect("BITCOIN_RPC_PASSWORD must be set");
    static ref RPC_CLIENT: Client = {
        dotenv::dotenv().ok();
        Client::new(
            &RPC_URL,
            Auth::UserPass(RPC_USER.clone(), RPC_PASSWORD.clone()),
        )
        .unwrap()
    };
}

fn new_client() -> Result<Client, bitcoincore_rpc::Error> {
    Client::new(
        &RPC_URL,
        Auth::UserPass(RPC_USER.clone(), RPC_PASSWORD.clone()),
    )
}

fn num_transactions_in_block(
    height: u64,
    client: Option<&Client>,
) -> Result<usize, bitcoincore_rpc::Error> {
    let client = match client {
        Some(client) => client,
        None => &new_client()?,
    };
    let block_stats = client.get_block_stats(height)?;
    Ok(block_stats.txs)
}

fn count_total_transactions(
    start_height: u64,
    end_height: u64,
) -> Result<usize, bitcoincore_rpc::Error> {
    let mut count = 0;
    for block_height in start_height..=end_height {
        let num_transactions = num_transactions_in_block(block_height, Some(&RPC_CLIENT))?;
        count += num_transactions
    }
    Ok(count)
}

fn parallel_count_total_transactions(
    start_height: u64,
    end_height: u64,
) -> Result<usize, bitcoincore_rpc::Error> {
    let num_threads = 16;
    let mut handles = Vec::new();
    let count = Arc::new(Mutex::new(0));

    // create `num_threads` threads
    for i in 0..num_threads {
        let count = count.clone();

        let handle = thread::spawn(move || -> Result<(), bitcoincore_rpc::Error> {
            let mut thread_count = 0;
            let client = new_client()?;
            for height in start_height..=end_height {
                if height % num_threads == i {
                    let num_txs = num_transactions_in_block(height, Some(&client))?;
                    thread_count += num_txs;
                }
            }
            let mut count = count.lock().unwrap();
            *count += thread_count;
            Ok(())
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap()?;
    }

    let res = count.lock().unwrap().clone();
    Ok(res)
}

fn main() {
    let start_height = 600_000;
    let end_height = 601_000;

    println!("Counting transactions with single thread...");

    let start_time = Instant::now();
    let total_transactions = count_total_transactions(start_height, end_height).unwrap();
    let duration = start_time.elapsed();

    println!("Total transactions: {}", total_transactions);
    println!("Time taken: {:?}", duration);

    println!("---");

    println!("Counting transactions with threads...");

    let start_time = Instant::now();
    let total_transactions = parallel_count_total_transactions(start_height, end_height).unwrap();
    let duration = start_time.elapsed();

    println!("Total transactions: {}", total_transactions);
    println!("Time taken: {:?}", duration);
}
