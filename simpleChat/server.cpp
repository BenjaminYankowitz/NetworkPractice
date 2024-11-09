
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <netinet/in.h>
#include <poll.h>
#include <queue>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>
#include <sys/resource.h>

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
    std::string outgoingMessage = "";
    std::size_t currentSpot = 0;
    std::string incomingMessage = "";
};

int main() {
    rlimit rlim;
    getrlimit(RLIMIT_NOFILE, &rlim);
    rlim.rlim_cur=rlim.rlim_max;
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
    char buffer[4097];
    auto removeClient = [&clientSockets, &clientInfo](std::size_t &i) {
        clientSockets[i] = clientSockets.back();
        clientInfo[i] = clientInfo.back();
        clientSockets.pop_back();
        clientInfo.pop_back();
        i--;
    };
    std::size_t lastNum = 0;
    std::size_t maxBackup = 0;
    while (clientSockets.size() > 1) {
        if(lastNum!=clientSockets.size()){
            // std::cout << clientSockets.size() << '\n';
            lastNum = clientSockets.size();
        }
        ssize_t numLeft = poll(clientSockets.data(), clientSockets.size(), -1);
        if(numLeft==-1){
            perror("poll failed");
            return 1;
        }
        if(clientSockets[0].revents){
            numLeft--;
        }
        if (clientSockets[0].revents & POLLIN) {
            while(true){
                pollfd possible = getpollFd(serverSocket);
                if (possible.fd == -1) {
                    break;
                }
                clientSockets.push_back(possible);
                clientInfo.push_back(UserInfo());            
            }
        }
        for (std::size_t i = 1; numLeft>0; i++) {
            if(clientSockets[i].revents){
                numLeft--;
            }
            if (clientSockets[i].revents & POLLIN) {
                ssize_t charRecive = recv(clientSockets[i].fd, buffer, sizeof(buffer) - 1, 0);
                if (charRecive == 0) {
                    removeClient(i);
                    continue;
                } else if(charRecive == -1){
                    if(errno==EAGAIN||errno==EWOULDBLOCK||errno==EINTR){
                        continue;
                    }
                    perror("No recive ");
                    removeClient(i);
                    continue;
                }
                buffer[charRecive] = '\0';
                clientInfo[i].incomingMessage += buffer;
                if (buffer[charRecive - 1] == '\n') {
                    if (clientInfo[i].incomingMessage == "Kill -9\n") {
                        clientSockets.clear();
                        std::cout << "KILL\n";
                        break;
                    }
                    std::shared_ptr<std::string> strPtr = std::make_shared<std::string>(std::move(clientInfo[i].incomingMessage));
                    clientInfo[i].incomingMessage = "";
                    for (std::size_t i2 = 1; i2 < clientInfo.size(); i2++) {
                        if (i2 == i) {
                            continue;
                        }
                        clientInfo[i2].toPrint.push(strPtr);
                        if(clientInfo[i2].toPrint.size()>maxBackup){
                            maxBackup = clientInfo[i2].toPrint.size();
                            std::cout << "New Max: " << maxBackup << '\n';
                        }
                        clientSockets[i2].events |= POLLOUT;
                    }
                }
            }
            if (clientSockets[i].revents & POLLOUT) {
                UserInfo &cClient = clientInfo[i];
                std::string &prntStr = cClient.outgoingMessage;
                if(prntStr==""){
                    prntStr+=*cClient.toPrint.front();
                    cClient.toPrint.pop();
                    while(!cClient.toPrint.empty()&&(prntStr.size()+cClient.toPrint.front()->size()<sizeof(buffer)-1)){
                        prntStr+=*cClient.toPrint.front();
                        cClient.toPrint.pop();
                    }
                }
                ssize_t charSent = send(clientSockets[i].fd, prntStr.c_str() + cClient.currentSpot, prntStr.size() - cClient.currentSpot, MSG_NOSIGNAL);
                if(charSent==-1){
                    // perror("Send fail");
                    continue;
                }
                cClient.currentSpot += charSent;
                if (cClient.currentSpot == prntStr.size()) {
                    cClient.outgoingMessage = "";
                    cClient.currentSpot = 0;
                    if (clientInfo[i].toPrint.empty()) {
                        clientSockets[i].events = POLLIN;
                    }
                }
            }
        }
    }
    std::cout << "Done\n";
    close(serverSocket);
    return 0;
}
