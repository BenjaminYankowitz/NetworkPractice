#include "common.h"
#include "config.h"
#include "message.pb.h"
#include <fcntl.h>
#include <iostream>
#include <netinet/in.h>
#include <poll.h>
#include <queue>
#include <sys/resource.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>
#include<ranges>

pollfd getpollFd(int serverSocket) {
    pollfd toRet;
    toRet.fd = accept(serverSocket, nullptr, nullptr);
    toRet.events = POLLIN;
    toRet.revents = 0;
    return toRet;
}

class UserInfo {
public:
    std::string userName = "";
    ServerMessage messageToSend;
    std::vector<uint8_t> outgoingMessage;
    std::size_t currentSendingSpot = 0;
    ClientMessage receivedMessage;
    std::vector<uint8_t> recivedMessageBuffer;
    std::size_t currentReciveSpot = 0;
    std::size_t nextReceivedMessageSize = 0;
    uint32_t userId;
};

void bringUpToSpeed(std::vector<UserInfo> &clientInfo, pollfd& newUserSocket, std::vector<uint32_t>& freeUserids){
    clientInfo.back().userId=freeUserids.back();
    freeUserids.pop_back();
    if(freeUserids.empty()){
        freeUserids.push_back(clientInfo.back().userId+1);
    }
    if(clientInfo.size()==1){
        return;
    }
    UserInfo& newUser = clientInfo.back();
    for(const UserInfo& oldUser : clientInfo | std::views::take(clientInfo.size() - 1)){
        if(oldUser.userName.empty()){
            continue;
        }
        Event& newEvent = *newUser.messageToSend.add_events();
        newEvent.set_newusername(oldUser.userName);
        newEvent.set_userid(oldUser.userId);
    }
    newUserSocket.events |= POLLOUT;
}

void sendEvent(Event& event, std::vector<UserInfo> &clientInfo, std::vector<pollfd>& clientSockets,std::size_t sentFrom){
    for (std::size_t i = 1; i < clientInfo.size(); i++) {
        if (i == sentFrom) {
            continue;
        }
        *clientInfo[i].messageToSend.add_events() = event;
        clientSockets[i].events |= POLLOUT;
    }
}

void handleRecivedMessage(std::vector<UserInfo> &clientInfo, std::vector<pollfd>& clientSockets, std::size_t sentFrom) {
    Event newEvent;
    newEvent.set_userid(clientInfo[sentFrom].userId);
    *newEvent.mutable_message()=(std::move(*clientInfo[sentFrom].receivedMessage.mutable_message()));
    if(clientInfo[sentFrom].receivedMessage.has_newusername()){
        clientInfo[sentFrom].userName = clientInfo[sentFrom].receivedMessage.newusername();
        newEvent.set_newusername(std::move(*clientInfo[sentFrom].receivedMessage.mutable_newusername()));
    }
    newEvent.set_exited(false);
    sendEvent(newEvent,clientInfo,clientSockets,sentFrom);
    clientInfo[sentFrom].receivedMessage.Clear();
}

int main() {
    std::vector<uint32_t> freeUserIds = {0};
    rlimit rlim;
    getrlimit(RLIMIT_NOFILE, &rlim);
    rlim.rlim_cur = rlim.rlim_max;
    setrlimit(RLIMIT_NOFILE, &rlim);
    int serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket == -1) {
        return 1;
    }
    const int enable = 1;
    if (setsockopt(serverSocket, SOL_SOCKET, SO_REUSEPORT, &enable, sizeof(int)) < 0)
        std::cerr << ("setsockopt(SO_REUSEADDR) failed");
    sockaddr_in serverAddress;
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(8080);
    serverAddress.sin_addr.s_addr = INADDR_ANY;
    int didBind = bind(serverSocket, (sockaddr *)&serverAddress, sizeof(serverAddress));
    if (didBind == -1) {
        perror("Did not bind");
        return 1;
    }
    if (listen(serverSocket, 200) == -1) {
        perror("Did not listen");
        return 1;
    }
    std::vector<pollfd> clientSockets;
    std::vector<UserInfo> clientInfo(2);
    clientSockets.push_back({fd : serverSocket, events : POLLIN, revents : 0});
    clientSockets.push_back(getpollFd(serverSocket));
    bringUpToSpeed(clientInfo,clientSockets.back(),freeUserIds);
    fcntl(serverSocket, F_SETFL, fcntl(serverSocket, F_GETFL, 0) | O_NONBLOCK);
    auto removeClient = [&clientSockets, &clientInfo,&freeUserIds](std::size_t &i) {
        Event newEvent;
        newEvent.set_userid(clientInfo[i].userId);
        newEvent.set_exited(true);
        sendEvent(newEvent,clientInfo,clientSockets,i);
        freeUserIds.push_back(clientInfo[i].userId);
        clientSockets[i] = clientSockets.back();
        clientInfo[i] = clientInfo.back();
        clientSockets.pop_back();
        clientInfo.pop_back();
        i--;
    };
    while (clientSockets.size() > 1) {
        ssize_t numLeft = poll(clientSockets.data(), clientSockets.size(), -1);
        if (numLeft == -1) {
            perror("poll failed");
            return 1;
        }
        if (clientSockets[0].revents) {
            numLeft--;
        }
        if (clientSockets[0].revents & POLLIN) {
            while (true) {
                pollfd possible = getpollFd(serverSocket);
                if (possible.fd == -1) {
                    break;
                }
                clientSockets.push_back(possible);
                clientInfo.emplace_back();
                bringUpToSpeed(clientInfo,clientSockets.back(),freeUserIds);
            }
        }
        for (std::size_t i = 1; numLeft > 0; i++) {
            if (clientSockets[i].revents) {
                numLeft--;
            }
            if (clientSockets[i].revents & POLLIN) {
                ReciveMessageReturn messageInfo = reciveMessage(clientInfo[i], clientSockets[i]);
                if (messageInfo.messageFinished) {
                    handleRecivedMessage(clientInfo,clientSockets,i);
                }
                if (messageInfo.endConnection) {
                    removeClient(i);
                }
            }
            if (clientSockets[i].revents & POLLOUT) {
                sendMessage(clientInfo[i], clientSockets[i]);
            }
        }
    }
    std::cout << "Done\n";
    close(serverSocket);
    return 0;
}
