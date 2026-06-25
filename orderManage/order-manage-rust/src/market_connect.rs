include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));

use amiquip::{
    AmqpProperties, Channel, Confirm, Connection,
    ConsumerMessage::{self},
    ConsumerOptions, Exchange, Publish, QueueDeclareOptions, Result,
};
use crossbeam_channel::Receiver;
use protobuf::Message;
use std::{
    collections::HashMap,
    ops::Deref,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{
            self, AtomicI64,
            Ordering::{self},
        },
    },
    thread::JoinHandle,
    time::Duration,
};

use crate::market_connect::marketMessages::{OrderFillMSG, OrderResponseMSG};
pub struct KafkaProducer {}

impl KafkaProducer {
    pub fn new() -> KafkaProducer {
        KafkaProducer {}
    }
}

pub struct OrderListenhandle {
    _order_response_thread: JoinHandle<()>,
    _fill_message_thread: JoinHandle<()>,
}

pub const MAX_SYMBOL_SIZE: usize = 5;

struct USCents(i64);
impl std::fmt::Display for USCents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0 < 0 {
            write!(f, "-")?;
        }
        let abs = self.0.unsigned_abs();
        let dollars = abs / 100;
        let cents = abs % 100;
        write!(f, "${dollars}")?;
        if cents != 0 {
            write!(f, ".{:0>8}", cents)?;
        }
        std::fmt::Result::Ok(())
    }
}
pub struct MarketOrder {
    proto: marketMessages::OrderMSG,
}
impl Deref for MarketOrder {
    type Target = marketMessages::OrderMSG;

    fn deref(&self) -> &Self::Target {
        &self.proto
    }
}
// impl protobuf::
impl MarketOrder {
    pub fn new(symbol: &str, quantity: i64, price: i64, buy_side: bool) -> MarketOrder {
        assert!(quantity > 0);
        assert!(price >= 0);
        assert!(symbol.len() <= MAX_SYMBOL_SIZE);
        let mut proto = marketMessages::OrderMSG::new();
        proto.symbol = String::from(symbol);
        proto.quantity = quantity;
        proto.price = price;
        proto.buySide = buy_side;
        MarketOrder { proto }
    }
}
pub struct AtomicCounter {
    n: AtomicI64,
}
impl AtomicCounter {
    pub fn new() -> AtomicCounter {
        AtomicCounter {
            n: AtomicI64::new(0),
        }
    }
    pub fn get(&self) ->i64 {
        //Relaxed is okay because it doesn't matter what order ids are given, just that they are not repeated.
        self.n.fetch_add(1, Ordering::Relaxed)
    }
}
struct ActiveMarketOrder {
    order: MarketOrder,
    filled: atomic::AtomicI64,
}
impl ActiveMarketOrder {
    fn new(order: MarketOrder, filled: i64) -> ActiveMarketOrder {
        ActiveMarketOrder {
            order,
            filled: filled.into(),
        }
    }
}
impl std::fmt::Display for MarketOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}ing {} of {} for {}",
            if self.buySide { "Buy" } else { "Sell" },
            self.quantity,
            self.symbol,
            USCents(self.price)
        )
    }
}

pub struct MarketState {
    orders_in_flight: Mutex<HashMap<i64, MarketOrder>>,
    orders_on_market: RwLock<HashMap<i64, Arc<ActiveMarketOrder>>>,
}
impl MarketState {
    pub fn new() -> MarketState {
        MarketState {
            orders_in_flight: Default::default(),
            orders_on_market: Default::default(),
        }
    }
    pub fn process_order_to_market(&self, correlation_id: i64, order: MarketOrder) {
        self.orders_in_flight
            .lock()
            .unwrap()
            .insert(correlation_id, order);
    }
    pub fn process_order_response(
        &self,
        correlation_id: i64,
        resp_mesg: &marketMessages::OrderResponseMSG,
    ) {
        let origin_order = self
            .orders_in_flight
            .lock()
            .unwrap()
            .remove(&correlation_id)
            .expect("Must pass correlation id to an inflight order");
        if !resp_mesg.successful {
            println!("order received. correlation id: {correlation_id}");
            println!("The order was not successful");
            return;
        }
        if let Some(order_id) = resp_mesg.orderID {
            assert!(resp_mesg.amountFilled < origin_order.quantity);
            println!("Order id: {}", order_id);
            self.orders_on_market.write().unwrap().insert(
                order_id,
                Arc::new(ActiveMarketOrder::new(origin_order, resp_mesg.amountFilled)),
            );
        } else {
            assert!(resp_mesg.amountFilled == origin_order.quantity);
        }
    }
    pub fn process_order_fill(&self, order_fill_msg: &marketMessages::OrderFillMSG) {
        assert!(order_fill_msg.filled > 0);
        let order_found = self
            .orders_on_market
            .read()
            .unwrap()
            .get(&order_fill_msg.orderID)
            .expect("To do")
            .clone();
        //Relaxed is okay because it doesn't acutally matter who removes the order as lifetime is protected by Arc, just that exactly one does.
        let new_fill = order_found
            .filled
            .fetch_add(order_fill_msg.filled, Ordering::Relaxed);
        if new_fill == order_found.order.quantity {
            self.orders_on_market
                .write()
                .unwrap()
                .remove(&order_fill_msg.orderID);
        }
    }

    // // Accessors for testing and diagnostics.
    // pub fn has_order_in_flight(&self, corr_id: i64) -> bool {
    //     self.orders_in_flight.lock().unwrap().contains_key(&corr_id)
    // }
    // pub fn has_order_on_market(&self, corr_id: i64) -> bool {
    //     self.orders_on_market.read().unwrap().contains_key(&corr_id)
    // }
    // pub fn get_order_filled(&self, order_id: i64) -> Option<i64> {
    //     self.orders_on_market
    //         .read()
    //         .unwrap()
    //         .get(&order_id)
    //         .map(|o| o.filled.load(Ordering::SeqCst))
    // }
    // pub fn orders_on_market_empty(&self) -> bool {
    //     self.orders_on_market.read().unwrap().is_empty()
    // }
}
#[derive(Debug)]
#[allow(dead_code)] // Errors are just printed.
pub enum RegisterError {
    Amiquip(amiquip::Error),
    SendTimeout(crossbeam_channel::RecvTimeoutError),
    Nacked,
    ConsumerFail,
}
impl From<amiquip::Error> for RegisterError {
    fn from(value: amiquip::Error) -> Self {
        RegisterError::Amiquip(value)
    }
}
impl From<crossbeam_channel::RecvTimeoutError> for RegisterError {
    fn from(value: crossbeam_channel::RecvTimeoutError) -> Self {
        RegisterError::SendTimeout(value)
    }
}
pub struct ConnectionInfo<'a> {
    pub market_id: i64,
    pub publisher_confirms: Receiver<Confirm>,
    pub exchange: Exchange<'a>,
}
pub fn register_with_market<'a>(
    channel: &'a mut Channel,
    exchange_name: &str,
    kafka_poducer: &mut KafkaProducer,
    username: &str,
) -> Result<ConnectionInfo<'a>, RegisterError> {
    let _ = kafka_poducer; //To do Add kafka logging
    // let channel = connection.open_channel(None)?;
    let publisher_confirms = channel.listen_for_publisher_confirms()?;
    channel.enable_publisher_confirms()?;
    let exchange = channel.exchange_declare_passive(exchange_name)?;
    let mut signup_msg: marketMessages::SignupMSG = marketMessages::SignupMSG::new();
    signup_msg.name = username.to_owned();
    let queue = channel.queue_declare(
        "",
        QueueDeclareOptions {
            auto_delete: true,
            ..Default::default()
        },
    )?;
    let consumer = queue.consume(ConsumerOptions {
        exclusive: true,
        ..Default::default()
    })?;
    let signup_msg = signup_msg.write_to_bytes().unwrap();
    let mut signup_msg = Publish::new(&signup_msg, "signup");
    signup_msg.properties = AmqpProperties::default().with_reply_to(queue.name().to_owned());
    exchange.publish(signup_msg)?;
    if let amiquip::Confirm::Nack(_) = publisher_confirms.recv_timeout(Duration::from_secs(5))? {
        return Err(RegisterError::Nacked);
    }
    let response = consumer.receiver().recv_timeout(Duration::from_secs(5))?;
    let response = if let ConsumerMessage::Delivery(val) = response {
        val
    } else {
        return Err(RegisterError::ConsumerFail);
    };
    let response_proto = marketMessages::SignupResponseMSG::parse_from_bytes(&response.body)
        .expect("the market sends valid messages");
    Ok(ConnectionInfo {
        market_id: response_proto.assignedId,
        publisher_confirms,
        exchange,
    })
}
pub fn start_listening_to_order_responses(
    exchange: &str,
    connection: &mut Connection,
    kafka_poducer: &mut KafkaProducer,
    id: &str,
    username: &str,
    market_state: Arc<MarketState>,
) -> OrderListenhandle {
    let id_copy = id.to_owned();
    let exit_barrier = Arc::new(std::sync::Barrier::new(3));
    let exit_barrier1 = exit_barrier.clone();
    let exit_barrier2 = exit_barrier.clone();
    let exchange_copy = exchange.to_owned();
    let order_response_channel = connection.open_channel(None).expect("Channel must open");
    let fill_message_channel = connection.open_channel(None).expect("Channel must open");
    let market_state_fill_message = market_state.clone();
    let _order_response_thread = std::thread::spawn(move || {
        let _ = kafka_poducer; //To do Add kafka logging
        let _ = username;
        let response_queue_key = id_copy + ".orderResponse";
        println!("{response_queue_key}");
        let exchange = order_response_channel
            .exchange_declare_passive(exchange_copy)
            .expect("exchange must already exist");
        let response_queue = order_response_channel
            .queue_declare(
                "",
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
            )
            .expect("Queue must be made");
        response_queue
            .bind(
                &exchange,
                response_queue_key,
                std::collections::BTreeMap::new(),
            )
            .expect("Queue should bind to exchange");
        exit_barrier1.wait();
        let consumer = response_queue
            .consume(Default::default())
            .expect("Should be able to consume");
        let reciver = consumer.receiver();
        for message in reciver.iter() {
            let message = if let ConsumerMessage::Delivery(delivery) = message {
                delivery
            } else {
                println!("failed to recive: {:?}", message);
                std::process::exit(0)
            };
            let correlation_id = message
                .properties
                .correlation_id()
                .as_ref()
                .expect("Must have correlation_id")
                .parse::<i64>()
                .expect("correlation id must fit in i64");
            let resp_mesg =
                OrderResponseMSG::parse_from_bytes(&message.body).expect("Must send valid message");
            // //To do deal with kafka here.
            market_state.process_order_response(correlation_id, &resp_mesg);
            let _ = message.ack(&order_response_channel);
        }
    });

    let id_copy = id.to_owned();
    let exchange_copy = exchange.to_owned();
    let market_state = market_state_fill_message;
    let fill_message_thread = std::thread::spawn(move || {
        let response_queue_key = id_copy + ".orderResponse";
        let exchange = fill_message_channel
            .exchange_declare_passive(exchange_copy)
            .expect("exchange must already exist");
        let response_queue = fill_message_channel
            .queue_declare(
                "",
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
            )
            .expect("Queue must be made");
        response_queue
            .bind(
                &exchange,
                response_queue_key,
                std::collections::BTreeMap::new(),
            )
            .expect("Queue should bind to exchange");
        let consumer = response_queue
            .consume(Default::default())
            .expect("Should be able to consume");
        let reciver = consumer.receiver();
        exit_barrier2.wait();
        for message in reciver.iter() {
            let message = if let ConsumerMessage::Delivery(delivery) = message {
                delivery
            } else {
                println!("failed to recive: {:?}", message);
                std::process::exit(0)
            };
            let fill_mesg =
                OrderFillMSG::parse_from_bytes(&message.body).expect("Must send valid message");
            // //To do deal with kafka here.
            market_state.process_order_fill(&fill_mesg);
            let _ = message.ack(&fill_message_channel);
        }
    });
    exit_barrier.wait();
    OrderListenhandle {
        _order_response_thread,
        _fill_message_thread: fill_message_thread,
    }
}
pub fn send_order_to_market(exchange: &Exchange, kafka_poducer: &mut KafkaProducer,  id : &str, username : &str, corr_id_gen : &AtomicCounter, order : MarketOrder, market_state: &MarketState) -> amiquip::Result<()> {
  let correlation_id = corr_id_gen.get();
  let _ = kafka_poducer; //To do add Kafka stuff
  let _ = username;
//   LogProto::AuditRecord auditRecord;
//   auto *intent = auditRecord.mutable_intent();
//   *intent->mutable_order() = order.toOrderMSG();
//   auto rawData = messageToArray(intent->order());
    let order_message = order.proto.write_to_bytes().unwrap();
    let mut order_message = Publish::new(&order_message, "order");
    order_message.properties = AmqpProperties::default().with_correlation_id(correlation_id.to_string()).with_reply_to(id.to_owned());
//   intent->set_guid(message.guid().data(), 16);
//   logKafkaMessage(KafkaTopic::Audit, username, auditRecord, logProducer);
    market_state.process_order_to_market(correlation_id, order);
    exchange.publish(order_message)?;
//   LogProto::AuditRecord successAudit;
//   successAudit.mutable_success()->set_guid(message.guid().data(), 16);
//   logKafkaMessage(KafkaTopic::Audit, username, successAudit, logProducer);
    Ok(())
}


// template <class T>
// bool logKafkaMessage(const kafka::Topic& topic, const kafka::Key key, const T& message, kafka::clients::producer::KafkaProducer &logProducer){
//   auto rawLogData = messageToArray(message);
//   kafka::Value value(rawLogData->data(), rawLogData->size());
//   try {
//     logProducer.send(kafka::clients::producer::ProducerRecord(
//                          topic, key, value),
//                      kafkaDeliveryCallback,
//                      kafka::clients::producer::KafkaProducer::SendOption::ToCopyRecordValue);
//   } catch (const kafka::KafkaException &e) {
//     std::cout << e.what() << '\n';
//     return false;
//   }
//   return true;
// }

// void kafkaDeliveryCallback(const kafka::clients::producer::RecordMetadata &metadata,
//                            const kafka::Error &error) {
//   if (!error) {
//     std::cout << "Message delivered: " << metadata.toString() << '\n';
//   } else {
//     std::cerr << "Message failed to be delivered: " << error.message()
//               << '\n';
//   }
// }
