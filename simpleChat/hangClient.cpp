#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <poll.h>

int main() {
    int clientSocket = socket(AF_INET, SOCK_STREAM, 0);

    sockaddr_in serverAddress;
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(8080);
    serverAddress.sin_addr.s_addr = INADDR_ANY;
    if(connect(clientSocket, (struct sockaddr *)&serverAddress, sizeof(serverAddress))==-1){
        return 1;
    }
    char buffer[21] = "fullMessage.c_str()\n";
    recv(clientSocket, buffer, 1, 0);
    send(clientSocket, buffer, 20, 0);
    recv(clientSocket, buffer, 1, 0);
    
    // pollfd connections[2];
    // connections[0] = {fd:clientSocket,events:POLLIN};
    // connections[1] = {fd:fileno(stdin),events:POLLIN};
    // std::string fullMessage = "";
    // char buffer[257];
    // while (true) {
    //     poll(connections,sizeof(connections)/sizeof(connections[0]),-1);
    //     if(connections[0].revents&POLLIN){
    //         ssize_t numChar = recv(clientSocket, buffer, sizeof(buffer)-1, 0);
    //         buffer[numChar] = '\0';
    //         std::cout << buffer;
    //     } 
    //     if(connections[0].revents&POLLOUT){
    //         send(clientSocket, fullMessage.c_str(), fullMessage.size(), 0);   
    //         fullMessage = "";
    //         connections[0].events -= POLLOUT;
    //     }
    //     if(connections[1].revents&POLLIN){
    //         std::string message;
    //         std::getline(std::cin, message);
    //         if (message == "q") {
    //             break;
    //         }
    //         fullMessage+=message+'\n';
    //         connections[0].events|=POLLOUT;
    //     }
        
    // }
    // if(!fullMessage.empty()){
    //     send(clientSocket, fullMessage.c_str(), fullMessage.size(), 0); 
    // }
    close(clientSocket);

    return 0;
}
