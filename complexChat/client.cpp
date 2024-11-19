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
    ClientMessage messageToSend;
    std::vector<uint8_t> outgoingMessage;
    std::size_t currentSendingSpot = 0;
};

void handelEvents(ServerMessage& receivedMessage, std::vector<std::string>& otherUsers) {
    for (Event &event : *receivedMessage.mutable_events()) {
        const uint32_t id = event.userid();
        if(otherUsers.size()<=id){
            otherUsers.resize(id+1);
        }
        std::string& eventUserName = otherUsers[id];
        if(event.has_newusername()){
            std::string& userName = *event.mutable_newusername();
            if(eventUserName.empty()){
                std::cout << userName << " joined\n";
            } else {
                std::cout << eventUserName << " changed their name to " << userName << '\n';
            }
            eventUserName = std::move(userName);
        }
        for(const std::string& message : event.message()){
            std::cout << eventUserName << ": " << message << '\n';
        }
        if(event.exited()){
            std::cout << eventUserName << " left\n";
            eventUserName="";
        }
    }
    receivedMessage.Clear();
}

int main() {
    std::vector<std::string> otherUsers;
    int clientSocket = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in serverAddress;
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(8080);
    serverAddress.sin_addr.s_addr = INADDR_ANY;
    if (connect(clientSocket, (struct sockaddr *)&serverAddress, sizeof(serverAddress)) == -1) {
        return 1;
    }
    pollfd connections[2];
    connections[0] = {fd : clientSocket, events : POLLIN|POLLOUT, revents : 0};
    connections[1] = {fd : fileno(stdin), events : POLLIN, revents : 0};
    MessageInfo messageInfo;
    std::cout << "What is your username?\n";
    std::string userName;
    while(userName.empty()){
        std::getline(std::cin, userName);
    }
    messageInfo.messageToSend.set_newusername(std::move(userName));
    while (true) {
        poll(connections, sizeof(connections) / sizeof(connections[0]), -1);
        if (connections[0].revents & POLLIN) {
            ReciveMessageReturn status = reciveMessage(messageInfo, connections[0]);
            if (status.messageFinished) {
                handelEvents(messageInfo.receivedMessage,otherUsers);
            }
            if (status.endConnection) {
                exit(1);
            }
        }
        if (connections[0].revents & POLLOUT) {
            sendMessage(messageInfo, connections[0]);
        }
        if (connections[1].revents & POLLIN) {
            std::string userInput;
            std::getline(std::cin, userInput);
            if (userInput == "q") {
                break;
            }
            *messageInfo.messageToSend.add_message() = std::move(userInput);
            connections[0].events |= POLLOUT;
        }
    }
    while (connections[0].events & POLLOUT) {
        sendMessage(messageInfo, connections[0]);
    }
    close(clientSocket);

    return 0;
}
