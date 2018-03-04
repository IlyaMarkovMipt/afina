#include "ServerImpl.h"

#include <cassert>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>

#include <pthread.h>
#include <signal.h>

#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>

#include <afina/Storage.h>
#include <protocol/Parser.h>
#include <afina/execute/Command.h>

namespace Afina {
namespace Network {
namespace Blocking {

void *ServerImpl::RunAcceptorProxy(void *p) {
    ServerImpl *srv = reinterpret_cast<ServerImpl *>(p);
    try {
        srv->RunAcceptor();
    } catch (std::runtime_error &ex) {
        std::cerr << "Server fails: " << ex.what() << std::endl;
    }
    return 0;
}

struct server_connect {
        ServerImpl* server;
        int client_socket;
        server_connect(ServerImpl *server, int client_socket) : server(server),
                                                                client_socket(
                                                                client_socket) {}

};

void *ServerImpl::RunConnectionProxy(void *p) {
    server_connect *srv = reinterpret_cast<server_connect *>(p);
    try {
        srv->server->RunConnection(srv->client_socket);
    } catch (std::runtime_error &ex) {
        std::cerr << "Server fails: " << ex.what() << std::endl;
    }
    delete srv;
    return 0;
}


// See Server.h
ServerImpl::ServerImpl(std::shared_ptr<Afina::Storage> ps) : Server(ps) {}

// See Server.h
ServerImpl::~ServerImpl() {}

// See Server.h
void ServerImpl::Start(uint32_t port, uint16_t n_workers) {
    std::cout << "network debug: " << __PRETTY_FUNCTION__ << std::endl;

    // If a client closes a connection, this will generally produce a SIGPIPE
    // signal that will kill the process. We want to ignore this signal, so send()
    // just returns -1 when this happens.
    sigset_t sig_mask;
    sigemptyset(&sig_mask);
    sigaddset(&sig_mask, SIGPIPE);
    if (pthread_sigmask(SIG_BLOCK, &sig_mask, NULL) != 0) {
        throw std::runtime_error("Unable to mask SIGPIPE");
    }

    // Setup server parameters BEFORE thread created, that will guarantee
    // variable value visibility
    max_workers = n_workers;
    listen_port = port;

    // The pthread_create function creates a new thread.
    //
    // The first parameter is a pointer to a pthread_t variable, which we can use
    // in the remainder of the program to manage this thread.
    //
    // The second parameter is used to specify the attributes of this new thread
    // (e.g., its stack size). We can leave it NULL here.
    //
    // The third parameter is the function this thread will run. This function *must*
    // have the following prototype:
    //    void *f(void *args);
    //
    // Note how the function expects a single parameter of type void*. We are using it to
    // pass this pointer in order to proxy call to the class member function. The fourth
    // parameter to pthread_create is used to specify this parameter value.
    //
    // The thread we are creating here is the "server thread", which will be
    // responsible for listening on port 23300 for incoming connections. This thread,
    // in turn, will spawn threads to service each incoming connection, allowing
    // multiple clients to connect simultaneously.
    // Note that, in this particular example, creating a "server thread" is redundant,
    // since there will only be one server thread, and the program's main thread (the
    // one running main()) could fulfill this purpose.
    running.store(true);
    if (pthread_create(&accept_thread, NULL, ServerImpl::RunAcceptorProxy, this) < 0) {
        throw std::runtime_error("Could not create server thread");
    }
}

// See Server.h
void ServerImpl::Stop() {
    std::cout << "network debug: " << __PRETTY_FUNCTION__ << std::endl;
    running.store(false);
    shutdown(server_socket, SHUT_RDWR);
    // RunAcceptor closes socket on exit.
    //close(server_socket);
}

// See Server.h
void ServerImpl::Join() {
    std::cout << "network debug: " << __PRETTY_FUNCTION__ << std::endl;
    while (!connections.empty()) {
        std::unique_lock<std::mutex> _lock(connections_mutex);
        connections_cv.wait(_lock);
    }
    pthread_join(accept_thread, 0);
    std::cout << "network debug: " << __PRETTY_FUNCTION__ << " finished" << std::endl;
}

// See Server.h
void ServerImpl::RunAcceptor() {
    std::cout << "network debug: " << __PRETTY_FUNCTION__ << std::endl;

    // For IPv4 we use struct sockaddr_in:
    // struct sockaddr_in {
    //     short int          sin_family;  // Address family, AF_INET
    //     unsigned short int sin_port;    // Port number
    //     struct in_addr     sin_addr;    // Internet address
    //     unsigned char      sin_zero[8]; // Same size as struct sockaddr
    // };
    //
    // Note we need to convert the port to network order

    struct sockaddr_in server_addr;
    std::memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;          // IPv4
    server_addr.sin_port = htons(listen_port); // TCP port number
    server_addr.sin_addr.s_addr = INADDR_ANY;  // Bind to any address

    // Arguments are:
    // - Family: IPv4
    // - Type: Full-duplex stream (reliable)
    // - Protocol: TCP
    server_socket = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (server_socket == -1) {
        throw std::runtime_error("Failed to open socket");
    }

    // when the server closes the socket,the connection must stay in the TIME_WAIT state to
    // make sure the client received the acknowledgement that the connection has been terminated.
    // During this time, this port is unavailable to other processes, unless we specify this option
    //
    // This option let kernel knows that we are OK that multiple threads/processes are listen on the
    // same port. In a such case kernel will balance input traffic between all listeners (except those who
    // are closed already)
    int opts = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opts, sizeof(opts)) == -1) {
        close(server_socket);
        throw std::runtime_error("Socket setsockopt() failed");
    }

    // Bind the socket to the address. In other words let kernel know data for what address we'd
    // like to see in the socket
    if (bind(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
        close(server_socket);
        throw std::runtime_error("Socket bind() failed");
    }

    // Start listening. The second parameter is the "backlog", or the maximum number of
    // connections that we'll allow to queue up. Note that listen() doesn't block until
    // incoming connections arrive. It just makesthe OS aware that this process is willing
    // to accept connections on this socket (which is bound to a specific IP and port)
    if (listen(server_socket, 5) == -1) {
        close(server_socket);
        throw std::runtime_error("Socket listen() failed");
    }

    int client_socket;
    struct sockaddr_in client_addr;
    socklen_t sinSize = sizeof(struct sockaddr_in);
    while (running.load()) {
        std::cout << "network debug: waiting for connection..." << std::endl;

        // When an incoming connection arrives, accept it. The call to accept() blocks until
        // the incoming connection arrives
        if ((client_socket = accept(server_socket, (struct sockaddr *)&client_addr, &sinSize)) == -1) {
            close(server_socket);
            if (errno != EINVAL) {
                throw std::runtime_error("Socket accept() failed");
            }
            return;
        } else  {
            std::lock_guard<std::mutex> __lock(connections_mutex);
            if (connections.size() >= max_workers) {
                if (close(client_socket) < 0) {
                    std::cout << "Failed to close: " << strerror(errno)
                              << std::endl;
                }
                continue;
            }
            executor.Execute([](ServerImpl* srv, int client_socket){
                try {
                    srv->RunConnection(client_socket);
                } catch (std::runtime_error &ex) {
                    std::cerr << "Server fails: " << ex.what() << std::endl;
                }
            }, this, client_socket);
        }
    }

    // Cleanup on exit...
    close(server_socket);
}

// See Server.h
void ServerImpl::RunConnection(int cl_sock) {
    std::cout << "network debug: " << __PRETTY_FUNCTION__ << std::endl;
    Protocol::Parser parser;
    char buf[BUFFER_SIZE];
    size_t pos = 0;
    size_t parsed = 0;
    connections.insert(pthread_self());
    struct timeval tv;
    memset(&tv, 0, sizeof(tv));
    tv.tv_sec = 10;
    if (setsockopt(cl_sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(struct timeval)) < 0) {
        closeConnection(cl_sock);
        return;
    }
    while (running.load()) {
        std::cout << "Running client connection " << cl_sock << std::endl;

        std::string response;
        try {
            ssize_t read = 0;
            while ((read = recv(cl_sock, buf + pos, BUFFER_SIZE - pos, 0)) > 0) {
                pos += read;
                if (parser.Parse(buf + parsed, pos - parsed, parsed)) {
                    break;
                }
            }
            if (read <= 0) {
                closeConnection(cl_sock);
                return;
            }
            uint32_t body_size = 0;
            auto command = parser.Build(body_size);
            if (command == nullptr) {
                goto reset;
            }
            std::string body;
            if (body_size > 0) {
                size_t body_read = pos - parsed;
                body.append(buf + parsed, std::min((int) body_read, (int) body_size));
                int to_read = body_size - body_read;
                while (to_read > 0 && (read = recv(cl_sock, buf, BUFFER_SIZE, 0)) > 0) {
                    body.append(buf, (to_read > read)? read: to_read);
                    to_read -= read;
                }

                if (read < 0) {
                    closeConnection(cl_sock);
                    return;
                }
            }
            command->Execute(*pStorage, body, response);
        } catch(std::runtime_error &e) {
            response = std::string("SERVER ERROR ") + e.what() + "\r\n";
        }

        if (!response.empty()) {
            size_t sent = 0;
            response += "\n";
            size_t resp_size = response.size();
            const char *resp_buf = response.c_str();
            while (sent < resp_size) {
                size_t ret = send(cl_sock, resp_buf + sent, resp_size - sent, 0);
                if (ret < 0) {
                    closeConnection(cl_sock);
                    return;
                }
                sent += ret;
            }
        }
reset:
        pos = 0;
        parsed = 0;
        parser.Reset();
    }
    closeConnection(cl_sock);
}

void ServerImpl::closeConnection(int cl_sock) {
    std::cout << "Closing connection " << cl_sock << std::endl;
    if (close(cl_sock) < 0) {
        std::cout << "Failed to close: " << strerror(errno) << std::endl;
    }

    std::lock_guard<std::mutex> __lock(connections_mutex);
    auto it = connections.find(pthread_self());
    if (it == connections.end()) {
        std::cout << "Can't remove thread from threads: it doesn't exist in set" << std::endl;
    } else {
        connections.erase(it);
    }
    if (connections.empty()) {
        connections_cv.notify_one();
    }
}

} // namespace Blocking
} // namespace Network
} // namespace Afina
