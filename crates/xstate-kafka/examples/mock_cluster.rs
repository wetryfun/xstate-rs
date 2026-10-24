//! Runs librdkafka's in-process mock cluster for clients outside this
//! process (the JavaScript broker packages' tests): prints the bootstrap
//! servers on the first line of stdout and serves until stdin closes.
//!
//! `cargo run -p xstate-kafka --features kafka --example mock_cluster -- [brokers] [topic:partitions ...]`

use std::io::Read;

use rdkafka::mocking::MockCluster;

fn main() {
    let mut args = std::env::args().skip(1).peekable();
    let brokers = args.next_if(|a| a.parse::<i32>().is_ok()).map_or(1, |a| a.parse().unwrap());
    let cluster = MockCluster::new(brokers).expect("a mock cluster");
    for topic in args {
        let (name, partitions) = topic.split_once(':').unwrap_or((&topic, "1"));
        cluster.create_topic(name, partitions.parse().expect("a partition count"), 1).expect("a topic");
    }
    println!("{}", cluster.bootstrap_servers());
    // Serve until the parent closes our stdin (or exits).
    let _ = std::io::stdin().read_to_end(&mut Vec::new());
}
