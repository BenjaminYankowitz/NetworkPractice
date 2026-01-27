#include <bslstl_sharedptr.h>
#include <cstdint>
#include <rmqa_connectionstring.h>
#include <rmqa_producer.h>
#include <rmqa_consumer.h>
#include <rmqa_rabbitcontext.h>
#include <rmqa_topology.h>
#include <rmqa_vhost.h>
#include <rmqp_producer.h>
#include <rmqp_consumer.h>
#include <rmqt_confirmresponse.h>
#include <rmqt_exchange.h>
#include <rmqt_exchangetype.h>
#include <rmqt_fieldvalue.h>
#include <rmqt_message.h>
#include <rmqt_properties.h>
#include <rmqt_result.h>
#include <rmqt_vhostinfo.h>

#include <bsl_memory.h>
#include <bsl_optional.h>
#include <bsl_vector.h>

#include "marketMessages.pb.h"
#include <cstdlib>
#include <iostream>
#include<random>
#include <sys/types.h>
using namespace BloombergLP;
constexpr const char *ExchangeName = "MarketExchange";

bool sendMessage(rmqa::Producer &producer, const rmqt::Message &message) {
  const bsl::string routingKey = "signup";
  auto confirmCallback = [](const rmqt::Message &message, const bsl::string &routingKey,
         const rmqt::ConfirmResponse &response) {
        // https://www.rabbitmq.com/confirms.html#when-publishes-are-confirmed
        if (response.status() == rmqt::ConfirmResponse::ACK) {
          // Message is now guaranteed to be safe with the broker.
          // Now is the time to reply to the request, commit the
          // database transaction, or ack the RabbitMQ message which
          // triggered this publish
        } else {
          // Send error response, rollback transaction, nack message
          // and/or raise an alarm for investigation - your message is not
          // delivered to all (or perhaps any) of the queues it was
          // intended for.
          std::cerr << "Message not confirmed: " << message.guid()
                    << " for routing key " << routingKey << " " << response
                    << "\n";
        }};
  const rmqp::Producer::SendStatus sendResult = producer.send(message,routingKey,confirmCallback);

  if (sendResult != rmqp::Producer::SENDING) {
    if (sendResult == rmqp::Producer::DUPLICATE) {
      std::cerr << "Failed to send message: " << message.guid()
                << " because an identical GUID is already outstanding\n";
    } else {
      std::cerr << "Unknown send failure for message: " << message.guid()
                << "\n";
    }
    return false;
  }
  return true;
}
template <class T>
void messageToArray(T message, bsl::vector<uint8_t>& buffer){
  const std::size_t size = message.ByteSizeLong();
  buffer.clear();
  buffer.resize(size);
  message.SerializeWithCachedSizesToArray(buffer.data());
}

int64_t getId(){
  std::random_device dev;
  std::uniform_int_distribution<int64_t> dist;
  return dist(dev);
}

bool handleInitialResponse(const rmqt::Message& message){
        SignupResponseMSG signupResponseMSG;
        if (!signupResponseMSG.ParseFromArray(message.payload(),
                                         message.payloadSize())) {
          std::cerr << "Failed to parse message\n";
          return false;
        } else if(signupResponseMSG.result()==success){
          std::cout << "Connected\n";
          return true;
        } else if (signupResponseMSG.result() == tooLong) {
          std::cerr << "Username too long\n";
          return false;
        } else if(signupResponseMSG.result() == taken){
            std::cerr << "Username taken\n";
            return false;
        } else {
            std::cerr << "Unknown error\n";
            return false;
        }
}

int main(int argc, char **argv) {
  // if (argc < 2) {
  //   std::cerr << "USAGE: " << argv[0] << " <amqp uri>\n";
  //   return 1;
  // }
  // amqp://localhost
  const bsl::string id = bsl::to_string(getId());
  const char *const amqpuri = "amqp://localhost";//argv[1];
  rmqa::RabbitContext rabbit;

  bsl::optional<rmqt::VHostInfo> vhostInfo =
      rmqa::ConnectionString::parse(amqpuri);

  if (!vhostInfo) {
    std::cerr << "Failed to parse connection string: " << amqpuri << "\n";
    return 1;
  }

  std::cout << "What is your name?\n";
  const std::string username = []() {
    std::string ret;
    if (!std::getline(std::cin, ret)) {
      std::cerr << "Failed to read username\n";
      std::exit(1);
    }
    return ret;
  }();

  // Returns immediately, setup performed on a different thread
  bsl::shared_ptr<rmqa::VHost> vhost = rabbit.createVHostConnection(
      username, // Connection Name Visible in management UI
      vhostInfo.value());

  // How many messages can be awaiting confirmation before `send` blocks
  constexpr uint16_t maxOutstandingConfirms = 10;

  rmqa::Topology topology;
  rmqt::ExchangeHandle exch =
      topology.addPassiveExchange(ExchangeName);
  if(exch.expired()){
    std::cerr << "Server does not exist yet\n";
    return 1;
  }
  rmqt::Result<rmqa::Producer> producerResult =
      vhost->createProducer(topology, exch, maxOutstandingConfirms);
  if (!producerResult) {
    std::cerr << "Error creating connection: " << producerResult.error()
              << "\n";
    return 1;
  }
  rmqt::QueueHandle responseQueue = topology.addQueue(id, rmqt::AutoDelete::OFF,rmqt::Durable::ON);
  topology.bind(exch, responseQueue, id);
  
  bsl::shared_ptr<rmqa::Producer> producer = producerResult.value();
  SignupMSG signupMSG;
  signupMSG.set_name(username);
  bsl::shared_ptr<bsl::vector<uint8_t>> rawData = bsl::make_shared<bsl::vector<uint8_t>>();
  messageToArray(signupMSG,*rawData);
  rmqt::Properties properties;
  properties.correlationId=id;
  properties.replyTo="activate_"+id;
  std::cout << properties.correlationId << '\n';
  rmqt::Message message = rmqt::Message(rawData,properties);
  if (!sendMessage(*producer, message)) {
    return 1;
  }
  bool activatd = false; 
  rmqt::Result<rmqa::Consumer> activateConsumerResult = vhost->createConsumer(
      topology, responseQueue, [&activatd](rmqp::MessageGuard &messageGuard) {
        const rmqt::Message &message = messageGuard.message();
          bool success = handleInitialResponse(message);
          messageGuard.ack();
          if(!success){
            std::exit(1);
          }
          activatd = true;
      });
  if (!activateConsumerResult) {
    // A fatal error such as `exch` not being present in `topology`
    // A disconnection, or  will never permanently fail an operation
    std::cerr << "Error creating connection: " << activateConsumerResult.error()
              << "\n";
    return 1;
  }
  [[maybe_unused]] bsl::shared_ptr<rmqa::Consumer> activateConsumer =
      activateConsumerResult.value();

  if (!producer->waitForConfirms(bsls::TimeInterval(5))) {
    std::cerr << "Timeout expired\n";
  }
  activateConsumer->cancelAndDrain();
  return 0;
}
