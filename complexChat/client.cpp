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
    ServerMessage nextMessage;
    std::vector<uint8_t> recivedMessageBuffer;
    std::size_t currentReciveSpot;
    std::size_t nextMesageSize;
};

int main() {
    int clientSocket = socket(AF_INET, SOCK_STREAM, 0);
    std::size_t printed = 0;
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
    messageInfo.currentReciveSpot = 0;
    messageInfo.nextMesageSize = 0;
    messageInfo.recivedMessageBuffer.resize(messagePadding);
    while (true) {
        poll(connections, sizeof(connections) / sizeof(connections[0]), -1);
        if (connections[0].revents & POLLIN) {
            ReciveMessageReturn status = reciveMessage(clientSocket,messageInfo);
            if(status.messageFinished){
                std::cout << messageInfo.nextMessage.messagetext();
                messageInfo.nextMessage.Clear();
            }
            if(status.endConnection){
                exit(1);
            }
        }
        if (connections[0].revents & POLLOUT) {
            ssize_t numChar = send(clientSocket, fullMessage.c_str() + printed, fullMessage.size() - printed, MSG_NOSIGNAL);
            if (numChar != -1) {
                printed += numChar;
                if (printed == fullMessage.size()) {
                    printed = 0;
                    fullMessage = "";
                    connections[0].events -= POLLOUT;
                } else {
                    perror("Sending message failed");
                }
            }
        }
        if (connections[1].revents & POLLIN) {
            std::string message;
            std::getline(std::cin, message);
            if (message == "q") {
                break;
            }
            fullMessage += message + '\n';
            connections[0].events |= POLLOUT;
        }
    }
    if (!fullMessage.empty()) {
        send(clientSocket, fullMessage.c_str(), fullMessage.size(), 0);
    }
    close(clientSocket);

    return 0;
}
