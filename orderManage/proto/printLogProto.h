#ifndef PRINT_LOG_PROTO_MESSAGES
#define PRINT_LOG_PROTO_MESSAGES
#include "logMessages.pb.h"
#include "printMarketProto.h"
#include <iomanip>
#include <ostream>

inline std::ostream &operator<<(std::ostream &out,
                                const LogProto::RabbitMSGIntent &msg) {
  out << "Intent [guid=";
  for (unsigned char c : msg.guid()) {
    out << std::hex << std::setfill('0') << std::setw(2)
        << static_cast<unsigned>(c);
  }
  out << std::dec << std::setfill(' ') << "]";
  switch (msg.msg_case()) {
  case LogProto::RabbitMSGIntent::kOrder:
    out << "\n" << msg.order();
    break;
  case LogProto::RabbitMSGIntent::kSignup:
    out << "\n" << msg.signup();
    break;
  default:
    out << "\n<unknown intent type>";
    break;
  }
  return out;
}

inline std::ostream &operator<<(std::ostream &out,
                                const LogProto::RabbitMSGSuccess &msg) {
  out << "Success [guid=";
  for (unsigned char c : msg.guid()) {
    out << std::hex << std::setfill('0') << std::setw(2)
        << static_cast<unsigned>(c);
  }
  out << std::dec << std::setfill(' ') << "]";
  return out;
}

inline std::ostream &operator<<(std::ostream &out,
                                const LogProto::AuditRecord &msg) {
  switch (msg.record_case()) {
  case LogProto::AuditRecord::kIntent:
    out << msg.intent();
    break;
  case LogProto::AuditRecord::kSuccess:
    out << msg.success();
    break;
  case LogProto::AuditRecord::kSignupResponse:
    out << msg.signup_response();
    break;
  case LogProto::AuditRecord::kOrderResponse:
    out << msg.order_response();
    break;
  case LogProto::AuditRecord::kOrderFill:
    out << msg.order_fill();
    break;
  default:
    out << "<unknown audit record type>";
    break;
  }
  return out;
}

#endif // PRINT_LOG_PROTO_MESSAGES
