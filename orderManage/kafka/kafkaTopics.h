#ifndef KAFKA_TOPICS_H
#define KAFKA_TOPICS_H
#include <kafka/Types.h>

namespace KafkaTopic {
  static const kafka::Topic Signup = "signup";
  static const kafka::Topic SignupResponse = "signup-response";
  static const kafka::Topic Order = "order";
  static const kafka::Topic OrderResponse = "order-response";
  static const kafka::Topic OrderFill = "order-fill";
} // namespace KafkaTopic

#endif // KAFKA_TOPICS_H
