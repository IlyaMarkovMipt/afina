#include "Worker.h"

#include <iostream>

#include <thread>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <cstring>

#include "Utils.h"

namespace Afina {
namespace Network {
namespace NonBlocking {

// See Worker.h
Worker::Worker(std::shared_ptr<Afina::Storage> ps): _ps(std::move(ps)), running(false) {
}

// See Worker.h
Worker::~Worker() {
    Stop();
}

// See Worker.h
void Worker::Start(int server_socket) {
    std::cout << "network debug: " << __PRETTY_FUNCTION__ << std::endl;
    this->server_socket = server_socket;
    running.store(true);
    if (pthread_create(&thread, NULL, OnRun, this) < 0) {
        throw std::runtime_error("Can't create pthread");
    }
}

// See Worker.h
void Worker::Stop() {
    std::cout << "network debug: " << __PRETTY_FUNCTION__ << std::endl;
    running.store(false);
    /* Shutdown can be called multiple times */
    shutdown(server_socket, SHUT_RDWR);
}

// See Worker.h
void Worker::Join() {
    std::cout << "network debug: " << __PRETTY_FUNCTION__ << std::endl;
    pthread_join(thread, NULL);
}

#define CLEANUP(epfd, handler)                          \
    epoll_ctl(epfd, EPOLL_CTL_DEL, handler->fd, NULL);  \
    worker->handlers.erase(handler->fd);                \
    delete handler;                                     \

// See Worker.h
void *Worker::OnRun(void *args) {
    std::cout << "network debug: " << __PRETTY_FUNCTION__ << std::endl;

    Worker *worker = (Worker *) args;
    int server_socket = worker->server_socket;

    int epfd;
    if ((epfd = epoll_create(EPOLL_CONNS)) < 0 ){
        throw std::runtime_error("can't create epoll context");
    }

    struct epoll_event ev;
    struct epoll_event events[EPOLL_CONNS];
    struct timeval tv;
    memset(&tv, 0, sizeof(tv));
    tv.tv_sec = 50;

    /* There is no EPOLLEXCLUSIVE on my kernel version */

    ev.events = EPOLLIN | EPOLLPRI | EPOLLERR | EPOLLHUP;
    ev.data.fd = server_socket;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, server_socket, &ev) < 0) {
        throw std::runtime_error("can't assign server socket");
    }

    while (worker->running.load()) {
        int nfds = epoll_wait(epfd, events, EPOLL_CONNS, (int) tv.tv_sec);
        for (int i = 0; i < nfds; i++) {
            if (events[i].data.fd == server_socket) {
                int fd = accept(server_socket, NULL, NULL);
                if (fd < 0) {
                    if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
                        continue;  // We have processed all incoming connections.
                    } else {
                        // Handle error or shutdown
                        close(server_socket);
                        if (worker->running.load()) {
                            throw std::runtime_error("Server failed to accept" +
                                                         std::string(std::strerror(errno)));
                        }
                    }
                }
                make_socket_non_blocking(fd);
                ev.events = EPOLLIN| EPOLLERR | EPOLLHUP;
                auto handler = new Handler(worker->_ps, fd);
                ev.data.fd = fd;
                worker->handlers.emplace(fd, handler);
                if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev) < 0) {
                    throw std::runtime_error("can't assign server socket");
                }
            } else {
                auto handler_it = worker->handlers.find(events[i].data.fd);
                if (handler_it == worker->handlers.end()) {
                    std::cout << "Handler somehow not found. Epoll sucks" << std::endl;
                    continue;
                }
                Handler *handler = handler_it->second;
                if (events[i].events & EPOLLIN) {
                    if (setsockopt(handler->fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(struct timeval)) < 0) {
                        throw std::runtime_error("failed to set timeout on fd: " + std::string(std::strerror(errno)));
                    }
                    State state = handler->Read();
                    // We need to remove old one and set new one
                    if (epoll_ctl(epfd, EPOLL_CTL_DEL, handler->fd, NULL) < 0) {
                        throw std::runtime_error(
                            "can't assign server socket: " +
                            std::string(std::strerror(errno)));
                    }
                    if (state == State::Error) {
                        std::cout << "Error in read" <<std::endl;
                        CLEANUP(epfd, handler);
                    } else {
                        ev.events = (state == State::Continue) ? EPOLLIN
                                                                   : EPOLLOUT;
                        ev.events |= EPOLLERR | EPOLLHUP;
                        ev.data.fd = handler->fd;
                        if (epoll_ctl(epfd, EPOLL_CTL_ADD, handler->fd, &ev) < 0) {
                            throw std::runtime_error(
                                "can't assign server socket: " +
                                std::string(std::strerror(errno)));
                        }
                    }
                } else if (events[i].events & EPOLLOUT) {

                    State state = handler->Write();
                    // We need to remove old one and set new one
                    if (epoll_ctl(epfd, EPOLL_CTL_DEL, handler->fd, NULL) < 0) {
                        throw std::runtime_error(
                            "can't assign socket: " +
                            std::string(std::strerror(errno)));
                    }

                    if (state == State::Error) {
                        std::cout << "Error in read" << std::endl;
                        CLEANUP(epfd, handler);

                    } else if (state == State::Enough){
                        ev.events = EPOLLIN | EPOLLERR | EPOLLHUP;
                        ev.data.fd = handler->fd;
                        if (epoll_ctl(epfd, EPOLL_CTL_ADD, handler->fd, &ev) < 0) {
                            throw std::runtime_error("can't assign server socket");
                        }
                        handler->Reset();
                    } else {
                        ev.events = EPOLLOUT | EPOLLERR | EPOLLHUP;
                        ev.data.fd = handler->fd;
                        if (epoll_ctl(epfd, EPOLL_CTL_ADD, handler->fd, &ev) < 0) {
                            throw std::runtime_error("can't assign socket"+
                                                     std::string(std::strerror(errno)));
                        }
                    }
                } else if (events[i].events & (EPOLLERR || EPOLLHUP)) {
                    CLEANUP(epfd, handler);
                }
            }
        }
    }

    for (auto &pair: worker->handlers) {
        epoll_ctl(epfd, EPOLL_CTL_DEL, pair.first, NULL);
        close(pair.first);
        delete pair.second;
    }
    worker->handlers.clear();
    // TODO: implementation here
    // 1. Create epoll_context here
    // 2. Add server_socket to context
    // 3. Accept new connections, don't forget to call make_socket_nonblocking on
    //    the client socket descriptor
    // 4. Add connections to the local context
    // 5. Process connection events
    //
    // Do not forget to use EPOLLEXCLUSIVE flag when register socket
    // for events to avoid thundering herd type behavior.
}

} // namespace NonBlocking
} // namespace Network
} // namespace Afina
