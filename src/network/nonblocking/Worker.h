#ifndef AFINA_NETWORK_NONBLOCKING_WORKER_H
#define AFINA_NETWORK_NONBLOCKING_WORKER_H

#include <memory>
#include <pthread.h>
#include <atomic>
#include <map>
#include <protocol/Parser.h>
#include <sys/socket.h>
#include <iostream>
#include <afina/execute/Command.h>
#include <cstring>

namespace Afina {

// Forward declaration, see afina/Storage.h
class Storage;

namespace Network {
namespace NonBlocking {

/**
 * # Thread running epoll
 * On Start spaws background thread that is doing epoll on the given server
 * socket and process incoming connections and its data
 */
class Worker {

    enum State {
            Error,
            Continue,
            Enough,
    };

    class Handler {
        static const int BUFFER_SIZE = 1024;
        char read_buf[BUFFER_SIZE];
        size_t read;
        size_t parsed;
        std::string response;
        size_t sent;
        std::shared_ptr<Afina::Storage> ps;
        Protocol::Parser parser;
        std::unique_ptr<Execute::Command> command;
        std::string body;
        uint32_t body_size;
        int body_to_read;
    public:
        int fd;
        Handler(std::shared_ptr<Afina::Storage> _ps, int _fd):read(0), sent(0),
                                                              ps(_ps), fd(_fd), parsed(0),
                                                              command(nullptr), body(""),
                                                              body_size(0), body_to_read(-1){}
        void Reset() {
            std::cout << __PRETTY_FUNCTION__ << std::endl;
            read = 0;
            sent = 0;
            parsed = 0;
            bzero(read_buf, BUFFER_SIZE);
            command = nullptr;
            body = "";
            body_size = 0;
            body_to_read = -1;
            parser.Reset();
        }
        State Read() {
            std::cout << "Reading client message "<< std::endl;

            try {
                ssize_t received = 0;
                std::cout << (command == nullptr) << std::endl;
                if (command == nullptr) {
                    while ((received = recv(fd, read_buf + read,
                                            BUFFER_SIZE - read, 0)) > 0) {
                        read += received;
                        if (parser.Parse(read_buf + parsed, read - parsed,
                                         parsed)) {
                            break;
                        }
                    }
                    if (received <= 0) {
                        if ((errno == EWOULDBLOCK || errno == EAGAIN) &&
                            received < 0) {
                            return State::Continue;
                        }
                        return State::Error;
                    }
                    command = parser.Build(body_size);
                    if (command == nullptr) {
                        return State::Error;
                    }
                    std::cout << std::string(read_buf) << std::endl;
                    std::cout << body_size << std::endl;
                }

                if (body_size > 0) {
                    if (body_to_read < 0) {
                        size_t body_read = read - parsed;
                        body.append(read_buf + parsed,
                                    std::min((int) body_read, (int) body_size));
                        body_to_read = body_size - body_read;
                    }
                    while (body_to_read > 0 && (received = recv(fd, read_buf, BUFFER_SIZE, 0)) > 0) {
                        body.append(read_buf, (body_to_read > received)? received: body_to_read);
                        body_to_read -= received;
                    }

                    if (received <= 0) {
                        if ((errno == EWOULDBLOCK || errno == EAGAIN) && received < 0) {
                            return State::Continue;
                        }
                        return State::Error;
                    }
                }
                command->Execute(*ps, body, response);
            } catch (std::runtime_error &e) {
                response = (std::string("SERVER ERROR ") + e.what() + "\r\n").c_str();
            }
            return State::Enough;
        }

        State Write() {
            std::cout << "Writing client message " << fd << std::endl;
            response += "\n";
            const char *write_buf = response.c_str();
            ssize_t written = send(fd, write_buf + sent, response.size() - sent, 0);
            if (written <= 0 && ((errno == EWOULDBLOCK || errno == EAGAIN))) {
                return State::Continue;
            }
            if (written < 0) {
                return State::Error;
            }
            sent += written;
            return State::Enough;
        }

    };

public:
    Worker(std::shared_ptr<Afina::Storage> ps);
    ~Worker();

    /**
     * Spaws new background thread that is doing epoll on the given server
     * socket. Once connection accepted it must be registered and being processed
     * on this thread
     */
    void Start(int server_socket);

    /**
     * Signal background thread to stop. After that signal thread must stop to
     * accept new connections and must stop read new commands from existing. Once
     * all readed commands are executed and results are send back to client, thread
     * must stop
     */
    void Stop();

    /**
     * Blocks calling thread until background one for this worker is actually
     * been destoryed
     */
    void Join();

protected:
    /**
     * Method executing by background thread
     */
    static void *OnRun(void *args);

private:
    pthread_t thread;
    std::shared_ptr<Afina::Storage> _ps;
    std::atomic<bool> running;
    int server_socket;

    std::map<int, Handler *> handlers;
    static const int EPOLL_CONNS = 64;
};

} // namespace NonBlocking
} // namespace Network
} // namespace Afina
#endif // AFINA_NETWORK_NONBLOCKING_WORKER_H
