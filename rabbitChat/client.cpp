#include <bslstl_sharedptr.h>
#include <cstdint>
#include <rmqa_connectionstring.h>
#include <rmqa_producer.h>
#include <rmqa_rabbitcontext.h>
#include <rmqa_topology.h>
#include <rmqa_vhost.h>
#include <rmqp_producer.h>
#include <rmqt_confirmresponse.h>
#include <rmqt_exchange.h>
#include <rmqt_exchangetype.h>
#include <rmqt_message.h>
#include <rmqt_result.h>
#include <rmqt_vhostinfo.h>

#include <bsl_memory.h>
#include <bsl_optional.h>
#include <bsl_vector.h>

#include "message.pb.h"
#include <cstdlib>
#include <iostream>
#include <limits>
#include <string>
using namespace BloombergLP;
constexpr const char *ExchangeName = "RabbitChatExchange";

bool sendMessage(rmqa::Producer &producer, const rmqt::Message &message) {
  const rmqp::Producer::SendStatus sendResult = producer.send(
      message, "routingkey",
      [](const rmqt::Message &message, const bsl::string &routingKey,
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
        }
      });

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

int main(int argc, char **argv) {
  if (argc < 2) {
    std::cerr << "USAGE: " << argv[0] << " <amqp uri>\n";
    return 1;
  }
  // amqp://localhost
  const char *const amqpuri = argv[1];
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
      topology.addExchange(ExchangeName, rmqt::ExchangeType::FANOUT);
  rmqt::QueueHandle queue = topology.addQueue("", rmqt::AutoDelete::ON);
  topology.bind(exch, queue, "routingkey");
  rmqt::Result<rmqa::Consumer> consumerResult = vhost->createConsumer(
      topology, queue, [&username](rmqp::MessageGuard &messageGuard) {
        const rmqt::Message &message = messageGuard.message();
        DefaultMessage protoMessage;
        if (!protoMessage.ParseFromArray(message.payload(),
                                         message.payloadSize())) {
          std::cerr << "Failed to parse message\n";
          messageGuard.ack();
          return;
        }
        if (protoMessage.name() == username) {
          messageGuard.ack();
          return;
        }
        std::cout << protoMessage.name() << ": " << protoMessage.messagetext()
                  << '\n';
        messageGuard.ack();
      });
  if (!consumerResult) {
    // A fatal error such as `exch` not being present in `topology`
    // A disconnection, or  will never permanently fail an operation
    std::cerr << "Error creating connection: " << consumerResult.error()
              << "\n";
    return 1;
  }
  [[maybe_unused]] bsl::shared_ptr<rmqa::Consumer> consumer =
      consumerResult.value();

  rmqt::Result<rmqa::Producer> producerResult =
      vhost->createProducer(topology, exch, maxOutstandingConfirms);
  if (!producerResult) {
    // A fatal error such as `exch` not being present in `topology`
    // A disconnection, or  will never permanently fail an operation
    std::cerr << "Error creating connection: " << producerResult.error()
              << "\n";
    return 1;
  }

  bsl::shared_ptr<rmqa::Producer> producer = producerResult.value();

  // At this point, `rmqcpp` will attempt to send `message` to the broker. If
  // a disconnection occurs, the message will be retried until a confirmation
  // is received, or the producer is destructed. Applications should consider
  // their message undelivered until `receiveConfirmation` is called.

  std::string messageStr;
  while (std::getline(std::cin, messageStr)) {
    DefaultMessage protoMessage;
    protoMessage.set_messagetext(messageStr);
    protoMessage.set_name(username);
    const std::size_t messageBytes = protoMessage.ByteSizeLong();
    if (messageBytes >
        static_cast<std::size_t>(std::numeric_limits<int>::max())) {
      std::cerr << "Message too large\n";
      return 1;
    }
    auto messageData =
        bsl::make_shared<bsl::vector<std::uint8_t>>(messageBytes);
    protoMessage.SerializeToArray(messageData->data(),
                                  static_cast<int>(messageBytes));
    rmqt::Message message(std::move(messageData));
    if (!sendMessage(*producer, message)) {
      return 1;
    }
  }

  // // Wait for the confirmation to come back before exiting
  // if (!producer->waitForConfirms(bsls::TimeInterval(5))) { //Add checking for
  //     // Timeout expired
  // }
}
