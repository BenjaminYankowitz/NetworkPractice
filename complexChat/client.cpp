#include "common.h"
#include "config.h"
#include "message.pb.h"
#include <iostream>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

class MessageInfo {
public:
    ServerMessage receivedMessage;
    std::vector<uint8_t> recivedMessageBuffer;
    std::size_t currentReciveSpot = 0;
    std::size_t nextReceivedMessageSize = 0;
    ServerMessage messageToSend;
    std::vector<uint8_t> outgoingMessage;
    std::size_t currentSendingSpot = 0;
};

int main() {
    int clientSocket = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in serverAddress;
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(8080);
    serverAddress.sin_addr.s_addr = INADDR_ANY;
    if (connect(clientSocket, (struct sockaddr *)&serverAddress, sizeof(serverAddress)) == -1) {
        return 1;
    }
    pollfd connections[2];
    connections[0] = {fd : clientSocket, events : POLLIN, revents : 0};
    connections[1] = {fd : fileno(stdin), events : POLLIN, revents : 0};
    std::string fullMessage = "";
    MessageInfo messageInfo;
    while (true) {
        poll(connections, sizeof(connections) / sizeof(connections[0]), -1);
        if (connections[0].revents & POLLIN) {
            ReciveMessageReturn status = reciveMessage(messageInfo,connections[0]);
            if(status.messageFinished){
                std::cout << messageInfo.receivedMessage.messagetext() << '\n';
                messageInfo.receivedMessage.Clear();
            }
            if(status.endConnection){
                exit(1);
            }
        }
        if (connections[0].revents & POLLOUT) {
            sendMessage(messageInfo, connections[0]);
        }
        if (connections[1].revents & POLLIN) {
            std::string message;
            std::getline(std::cin, message);
            if (message == "q") {
                break;
            }
            connections[0].events |= POLLOUT;
            *messageInfo.messageToSend.mutable_messagetext()+=message;
        }
    }
    while (connections[0].events & POLLOUT) {
        sendMessage(messageInfo, connections[0]);
    }
    close(clientSocket);

    return 0;
}
