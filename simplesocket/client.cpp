#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

int main()
{
    int clientSocket = socket(AF_INET, SOCK_STREAM, 0);

    sockaddr_in serverAddress;
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(8080);
    serverAddress.sin_addr.s_addr = INADDR_ANY;
    connect(clientSocket, (struct sockaddr*)&serverAddress,sizeof(serverAddress));
    while(true){
        std::string message;
        std::getline(std::cin, message);
        if(message == "q"){
            break;
        }
        message+="\n";
        send(clientSocket, message.c_str(), message.size()+1, 0);
    }
    close(clientSocket);

    return 0;
}
