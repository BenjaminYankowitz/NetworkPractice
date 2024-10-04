
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>
#include <poll.h>
#include <memory>
#include <queue>
#include <fcntl.h>

pollfd getpollFd(int serverSocket){
    pollfd toRet;
    toRet.fd = accept(serverSocket, nullptr, nullptr);
    toRet.events = POLLIN;
    return toRet;
}

int main() {
    int serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in serverAddress;
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(8080);
    serverAddress.sin_addr.s_addr = INADDR_ANY;
    bind(serverSocket, (sockaddr *)&serverAddress, sizeof(serverAddress));
    listen(serverSocket, 5);
    std::vector<pollfd> clientSockets;
    std::vector<std::queue<std::shared_ptr<std::string>>> toPrint(1);
    clientSockets.push_back(getpollFd(serverSocket));
    fcntl(serverSocket, F_SETFL, fcntl(serverSocket, F_GETFL, 0) | O_NONBLOCK);
    char buffer[256];
    while (clientSockets.size()>0) {
        int toDo = poll(clientSockets.data(),clientSockets.size(),100);
        pollfd possible = getpollFd(serverSocket);
        if(possible.fd!=-1){
            clientSockets.push_back(possible);
            toPrint.push_back(std::queue<std::shared_ptr<std::string>>());
        }
        if(toDo == 0){
            continue;
        }
        for(std::size_t i = 0; i < clientSockets.size(); i++){
            if(clientSockets[i].revents&POLLIN){
                if (recv(clientSockets[i].fd, buffer, sizeof(buffer), 0) <= 0) {
                    clientSockets[i] = clientSockets.back();
                    toPrint[i] = toPrint.back();
                    clientSockets.pop_back();
                    toPrint.pop_back();
                    i--;
                    continue;
                }
                std::shared_ptr<std::string> strPtr = std::make_shared<std::string>(buffer);
                for(std::size_t i2 = 0; i2 < toPrint.size(); i2++){
                    if(i2==i){
                        continue;
                    }
                    toPrint[i2].push(strPtr);
                    clientSockets[i2].events|=POLLOUT;
                }
            }
            if(clientSockets[i].revents&POLLOUT){
                send(clientSockets[i].fd,toPrint[i].front()->c_str(),toPrint[i].front()->size()+1,0);
                toPrint[i].pop();
                if(toPrint[i].empty()){
                    clientSockets[i].events=POLLIN;
                }
            }
        }        
    }
    close(serverSocket);
    return 0;
}
