#ifndef  PRINT_MARKET_PROTO_MESSAGES
#define PRINT_MARKET_PROTO_MESSAGES
#include "marketMessages.pb.h"
#include <ostream>

class FormatAsMoney {
public:
  FormatAsMoney(long amnt) : amnt_(amnt) {}
  friend std::ostream &operator<<(std::ostream &out, FormatAsMoney money);

private:
  long amnt_;
};
inline std::ostream &operator<<(std::ostream &out, FormatAsMoney money) {
  int dollars = money.amnt_ / 100;
  int cents = money.amnt_ % 100;
  out << "$" << dollars;
  if (cents != 0) {
    out << '.';
    if (cents < 10) {
      out << '0';
    }
    out << cents;
  }
  return out;
}
// message OrderMSG {
//   string symbol = 1;
//   bool buySide = 2; 
//   int64 quantity = 3;
//   int64 price = 4;
// }

inline std::ostream& operator<<(std::ostream& out ,const OrderMSG& msg){
  return out << "Order:\nsymbol:" << msg.symbol() << "\nside: " << (msg.buyside() ? "buy" : "sell") << "\nquantity: " << msg.quantity() << "\nprice: " << msg.price();
}

// message OrderFillMSG {
//   int64 orderID = 1;
//   int64 filled = 2;
// }

inline std::ostream& operator<<(std::ostream& out ,const OrderFillMSG& msg){
  return out << "Order Fill:\nid: " << msg.orderid() << "\nfilled: " << msg.filled();
}

// message OrderResponseMSG {
//   bool successful = 1;
//   optional int64 orderID = 2;
//   int64 amountFilled = 3;
//   int64 price = 4;
// }

inline std::ostream& operator<<(std::ostream& out ,const OrderResponseMSG& msg){
  out << "Order Response";
  if(!msg.successful()){
    return out << "\nfailed";
  }
  if(msg.has_orderid()){
    out << "\nid: " << msg.orderid();
  } else {
    out << "\n fully filled";
  }
  return out << '\n' << msg.amountfilled() << " shares filled for " << FormatAsMoney(msg.price());
}

// message SignupMSG{
//   string name = 1;
// }

inline std::ostream& operator<<(std::ostream& out ,const SignupMSG& msg){
  return out << "Attempt to sign up with name: " << msg.name();
}

// message SignupResponseMSG {
//   int64 assignedId = 1;
// }
inline std::ostream& operator<<(std::ostream& out ,const SignupResponseMSG& msg){
  return out << "Signup sucessfull with id: " << msg.assignedid();
}


#endif // PRINT_MARKET_PROTO_MESSAGES