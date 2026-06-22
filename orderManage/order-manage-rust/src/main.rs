use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
mod market_connect;
use crate::market_connect::{
    KafkaProducer, MarketOrder, MarketState, register_with_market, send_order_to_market, start_listening_to_order_responses
};

fn main() {
    let mut kafka_producer = KafkaProducer::new();
    let correlation_id_generator = market_connect::AtomicCounter::new();
    let market_state = Arc::new(MarketState::new());
    let exchange_name = "MarketExchange";
    let amqpuri = "amqp://localhost:9092";
    let mut connection =
        amiquip::Connection::insecure_open(amqpuri).expect("Connection should open");

    let username = if std::env::args()
        .collect::<Vec<_>>()
        .contains(&"-n".to_owned())
    {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System time is before 1970")
            .as_secs()
            .to_string()
    } else {
        println!("What is your username?");
        let mut username = String::new();
        let _ = std::io::stdin().read_line(&mut username);
        username
    };
    let mut channel = connection.open_channel(None).unwrap();
    //   kafka::Properties props({{"bootstrap.servers", {"localhost:9092"}},
    //                            {"enable.idempotence", {"true"}}});
    //   kafka::clients::producer::KafkaProducer kafkaProducer(props);
    let marekt_regestration =
        register_with_market(&mut channel, exchange_name, &mut kafka_producer, &username)
            .expect("Should register with market");
    println!("{}", marekt_regestration.market_id);
    let id = marekt_regestration.market_id.to_string();
    let _listen_handles = start_listening_to_order_responses(
        exchange_name,
        &mut connection,
        &mut kafka_producer,
        &id,
        &username,
        market_state.clone(),
    );
    send_order_to_market(
        &marekt_regestration.exchange,
        &mut kafka_producer,
        &id,
        &username,
        &correlation_id_generator,
        MarketOrder::new("slugs",10,1000,false),
        &market_state,
    )
    .expect("order should be sent to market");
    send_order_to_market(
        &marekt_regestration.exchange,
        &mut kafka_producer,
        &id,
        &username,
        &correlation_id_generator,
        MarketOrder::new("slugs",10,1000,true),
        &market_state,
    )
    .expect("order should be sent to market");
    loop {
        std::thread::sleep(std::time::Duration::from_secs(5));
    }
}
