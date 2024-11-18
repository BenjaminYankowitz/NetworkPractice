#ifndef COMPLEXCHATCOMMON_H__
#define COMPLEXCHATCOMMON_H__
#include "config.h"
#include <iostream>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>
#include <cassert>

struct ReciveMessageReturn {
    bool messageFinished = false;
    bool endConnection = false;
};

template <typename T>
concept DeSerializable = requires(T a, uint8_t *data, std::size_t size) {
    { a.ParseFromArray(data, size) };
};

template <typename T>
concept ReciveAble = requires(T a) {
    { a.receivedMessage } -> DeSerializable;
    { a.recivedMessageBuffer } -> std::convertible_to<std::vector<uint8_t>>;
    { a.currentReciveSpot } -> std::convertible_to<std::size_t>;
    { a.nextReceivedMessageSize } -> std::convertible_to<std::size_t>;
};


template<ReciveAble reciveAble>
ReciveMessageReturn reciveMessage(reciveAble& messageInfo, const pollfd& clientSocket) {
    std::size_t goalSize = messageInfo.nextReceivedMessageSize ? messageInfo.nextReceivedMessageSize : messagePadding;
    if(messageInfo.recivedMessageBuffer.size()<goalSize){
        messageInfo.recivedMessageBuffer.resize(goalSize);
    }
    ReciveMessageReturn ret;
    ssize_t numChar = recv(clientSocket.fd, messageInfo.recivedMessageBuffer.data() + messageInfo.currentReciveSpot, messageInfo.recivedMessageBuffer.size() - messageInfo.currentReciveSpot, 0);
    if(numChar==0){
        ret.endConnection=true;
        return ret;
    }
    if (numChar != -1) {
        messageInfo.currentReciveSpot += numChar;
        if (messageInfo.currentReciveSpot == goalSize) {
            if (messageInfo.nextReceivedMessageSize) {
                messageInfo.receivedMessage.ParseFromArray(messageInfo.recivedMessageBuffer.data(), messageInfo.recivedMessageBuffer.size());
                ret.messageFinished=true;
                goalSize=0;
            } else {
                goalSize = 0;
                for (int i = 0; i < numSizeBytes; i++) {
                    goalSize <<= 8;
                    goalSize += messageInfo.recivedMessageBuffer[i];
                }
                if (messageInfo.recivedMessageBuffer[numSizeBytes] != magicNumber) {
                    std::cerr << "Magic number was " << messageInfo.recivedMessageBuffer[numSizeBytes] << " not " << magicNumber << '\n';
                    ret.endConnection=true;
                }
            }
            messageInfo.currentReciveSpot = 0;
            messageInfo.recivedMessageBuffer.resize(goalSize ? goalSize : messagePadding);
            messageInfo.nextReceivedMessageSize = goalSize;
        }
    } else {
        if(errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR){
            return ret;
        }
        perror("Reciving message failed");
        ret.endConnection=true;
    }
    return ret;
}


template<typename T>
concept Serializable = requires(T a, uint8_t* dataPtr){
    {a.ByteSizeLong()} -> std::convertible_to<std::size_t>;
    {a.SerializeWithCachedSizesToArray(dataPtr)};
    {a.Clear()};
};

template<typename T>
concept SendAble = requires(T a){
    {a.messageToSend} -> Serializable;
    {a.outgoingMessage} -> std::convertible_to<std::vector<uint8_t>>;
    {a.currentSendingSpot} -> std::convertible_to<std::size_t>;
};

template<SendAble sendAble>
bool sendMessage(sendAble &clientInfo, pollfd &clientSocket) {
    std::vector<uint8_t> &prntStr = clientInfo.outgoingMessage;
    if (prntStr.size() == 0) {
        std::size_t bytesUsed = clientInfo.messageToSend.ByteSizeLong();
        assert(bytesUsed<(1<<(numSizeBytes*8)));
        prntStr.resize(messagePadding+bytesUsed);
        std::size_t bytesUsedLeft = bytesUsed;
        for(int i = numSizeBytes-1; i >= 0; i--){
            prntStr[i] = bytesUsedLeft%(1<<8);
            bytesUsedLeft>>=8;
        }
        prntStr[numSizeBytes] = magicNumber;
        clientInfo.messageToSend.SerializeWithCachedSizesToArray(prntStr.data()+messagePadding);
        clientInfo.messageToSend.Clear();
    }
    if (prntStr.size() != 0) {
        ssize_t charSent = send(clientSocket.fd, prntStr.data() + clientInfo.currentSendingSpot, prntStr.size() - clientInfo.currentSendingSpot, MSG_NOSIGNAL);
        if (charSent == -1) {
            // perror("Send fail");
            return false;
        }
        clientInfo.currentSendingSpot += charSent;
        if (clientInfo.currentSendingSpot == prntStr.size()) {
            prntStr.clear();
            clientInfo.currentSendingSpot = 0;
            if (clientInfo.messageToSend.ByteSizeLong()==0) {
                clientSocket.events -= POLLOUT;
            }
        }
    }
    return true;
}

#endif