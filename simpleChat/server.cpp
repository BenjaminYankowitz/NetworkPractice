
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <netinet/in.h>
#include <poll.h>
#include <queue>
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
    std::queue<std::shared_ptr<std::string>> toPrint;
    std::size_t currentSpot = 0;
    std::string incomingMessage = "";
};

int main() {
    int serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if(serverSocket==-1){
        return 1;
    }
    sockaddr_in serverAddress;
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(8080);
    serverAddress.sin_addr.s_addr = INADDR_ANY;
    bind(serverSocket, (sockaddr *)&serverAddress, sizeof(serverAddress));
    if(listen(serverSocket, 5)==-1){
        return 1;
    }
    std::vector<pollfd> clientSockets;
    std::vector<UserInfo> clientInfo(2);
    clientSockets.push_back({fd : serverSocket, events : POLLIN,revents:0});
    clientSockets.push_back(getpollFd(serverSocket));
    fcntl(serverSocket, F_SETFL, fcntl(serverSocket, F_GETFL, 0) | O_NONBLOCK);
    char buffer[257];
    while (clientSockets.size() > 1) {
        poll(clientSockets.data(), clientSockets.size(), -1);
        if(clientSockets[0].revents & POLLIN){
            pollfd possible = getpollFd(serverSocket);
            if (possible.fd != -1) {
                clientSockets.push_back(possible);
                clientInfo.push_back(UserInfo());
            }
        }
        for (std::size_t i = 1; i < clientSockets.size(); i++) {
            if (clientSockets[i].revents & POLLIN) {
                ssize_t charRecive = recv(clientSockets[i].fd, buffer, sizeof(buffer) - 1, 0);
                if (charRecive <= 0) {
                    clientSockets[i] = clientSockets.back();
                    clientInfo[i] = clientInfo.back();
                    clientSockets.pop_back();
                    clientInfo.pop_back();
                    i--;
                    continue;
                }
                buffer[charRecive] = '\0';
                clientInfo[i].incomingMessage += buffer;
                if (buffer[charRecive - 1] == '\n') {
                    std::shared_ptr<std::string> strPtr = std::make_shared<std::string>(std::move(clientInfo[i].incomingMessage));
                    clientInfo[i].incomingMessage = "";
                    for (std::size_t i2 = 1; i2 < clientInfo.size(); i2++) {
                        if (i2 == i) {
                            continue;
                        }
                        clientInfo[i2].toPrint.push(strPtr);
                        clientSockets[i2].events |= POLLOUT;
                    }
                }
            }
            if (clientSockets[i].revents & POLLOUT) {
                UserInfo& cClient = clientInfo[i];
                const std::string& prntStr = *cClient.toPrint.front();
                cClient.currentSpot+=send(clientSockets[i].fd, prntStr.c_str()+cClient.currentSpot, prntStr.size()-cClient.currentSpot, 0);
                if(cClient.currentSpot==prntStr.size()){
                    clientInfo[i].toPrint.pop();
                    cClient.currentSpot = 0;
                }
                if (clientInfo[i].toPrint.empty()) {
                    clientSockets[i].events = POLLIN;
                }
            }
        }
    }
    close(serverSocket);
    return 0;
}
