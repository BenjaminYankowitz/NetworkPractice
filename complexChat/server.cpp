
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

pollfd getpollFd(int serverSocket) {
    pollfd toRet;
    toRet.fd = accept(serverSocket, nullptr, nullptr);
    toRet.events = POLLIN;
    toRet.revents = 0;
    return toRet;
}

class UserInfo {
public:
    ServerMessage messageToSend;
    std::vector<uint8_t> outgoingMessage;
    std::size_t currentSendingSpot = 0;
    ClientMessage receivedMessage;
    std::vector<uint8_t> recivedMessageBuffer;
    std::size_t currentReciveSpot = 0;
    std::size_t nextReceivedMessageSize = 0;
};

void handleRecivedMessage(std::vector<UserInfo> &clientInfo, std::vector<pollfd>& clientSockets, std::size_t sentFrom) {
    for (std::size_t i = 1; i < clientInfo.size(); i++) {
        if (i == sentFrom) {
            continue;
        }
        *clientInfo[i].messageToSend.mutable_messagetext() += clientInfo[sentFrom].receivedMessage.messagetext();
        clientSockets[i].events |= POLLOUT;
    }
    clientInfo[sentFrom].receivedMessage.Clear();
}

int main() {
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
    fcntl(serverSocket, F_SETFL, fcntl(serverSocket, F_GETFL, 0) | O_NONBLOCK);
    auto removeClient = [&clientSockets, &clientInfo](std::size_t &i) {
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
