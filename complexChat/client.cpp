#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <poll.h>
#include "message.pb.h"
#include "config.h"

int main() {
    ServerMessage currentMessage;
    int clientSocket = socket(AF_INET, SOCK_STREAM, 0);
    std::size_t printed = 0;
    sockaddr_in serverAddress;
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(8080);
    serverAddress.sin_addr.s_addr = INADDR_ANY;
    if(connect(clientSocket, (struct sockaddr *)&serverAddress, sizeof(serverAddress))==-1){
        return 1;
    }
    pollfd connections[2];
    connections[0] = {fd:clientSocket,events:POLLIN,revents:0};
    connections[1] = {fd:fileno(stdin),events:POLLIN,revents:0};
    std::string fullMessage = "";
    constexpr std::size_t paddingBytes = 1+numSizeBytes;
    bool hasMessage = false;
    std::size_t goalSize = paddingBytes;
    std::size_t recvPos = 0;
    std::vector<uint8_t> buffer(goalSize);
    while (true) {
        poll(connections,sizeof(connections)/sizeof(connections[0]),-1);
        if(connections[0].revents&POLLIN){
            ssize_t numChar = recv(clientSocket, buffer.data()+recvPos, buffer.size()-recvPos, 0);
            if(numChar!=-1){
                recvPos+=numChar;
                if(recvPos==goalSize){
                    if(hasMessage){
                        currentMessage.ParseFromArray(buffer.data(),buffer.size());
                        std::cout << currentMessage.messagetext();
                        currentMessage.Clear();
                        goalSize = paddingBytes;
                    } else {
                        goalSize = 0;
                        for(int i = 0; i < numSizeBytes; i++){
                            goalSize<<=8;
                            goalSize+=buffer[i];
                        }
                        if(buffer[numSizeBytes]!=magicNumber){
                            std::cerr << "Magic number was " << buffer[numSizeBytes] << " not " << magicNumber << '\n';
                            exit(1);
                        }
                    }
                    hasMessage = !hasMessage;
                    recvPos = 0;
                    buffer.resize(goalSize);
                }
            } else {
                perror("Reciving message failed");
            }
        }
        if(connections[0].revents&POLLOUT){
            ssize_t numChar = send(clientSocket, fullMessage.c_str()+printed, fullMessage.size()-printed, MSG_NOSIGNAL);
            if(numChar!=-1){
                printed+=numChar;
                if(printed==fullMessage.size()){
                    printed = 0;
                    fullMessage = "";
                    connections[0].events -= POLLOUT;
                }   else {
                    perror("Sending message failed");
                }
            }
        }
        if(connections[1].revents&POLLIN){
            std::string message;
            std::getline(std::cin, message);
            if (message == "q") {
                break;
            }
            fullMessage+=message+'\n';
            connections[0].events|=POLLOUT;
        }
    }
    if(!fullMessage.empty()){
        send(clientSocket, fullMessage.c_str(), fullMessage.size(), 0); 
    }
    close(clientSocket);

    return 0;
}
