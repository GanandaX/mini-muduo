cmake_minimum_required(VERSION 3.20)

project(mini_muduo LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(mini_muduo
        src/Buffer.cc
        src/Channel.cc
        src/EpollPoller.cc
        src/EventLoop.cc
        src/Acceptor.cc
        src/TcpConnection.cc
        src/TcpServer.cc
        src/EventLoopThread.cc
        src/EventLoopThreadPool.cc
        src/TimerQueue.cc
        src/InetAddress.cc
        src/Socket.cc
        src/Connector.cc
        src/TcpClient.cc
        src/LengthHeaderCodec.cc
)

target_include_directories(mini_muduo PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/src/include
)

find_package(Threads REQUIRED)
target_link_libraries(mini_muduo PUBLIC Threads::Threads)

target_compile_options(mini_muduo PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)


function(add_example name)
        add_executable(${name} examples/${name}.cc)
        target_link_libraries(${name} PRIVATE mini_muduo)
        target_compile_options(${name} PRIVATE -Wall -Wextra -Wpedantic)
endfunction()

add_example(epoll_echo_server)
add_example(acceptor_demo)
add_example(multi_thread_echo)
add_example(timer_queue_test)
add_example(socket_test)



include(CTest)

if (BUILD_TESTING)
        function(add_mini_test name) 
                add_executable(${name} tests/${name}.cc)
                target_link_libraries(${name} PRIVATE mini_muduo)
                add_test(NAME ${name} COMMAND ${name})

        endfunction()

        add_mini_test(buffer_test )
        add_mini_test(channel_test )
        add_mini_test(epoll_poller_test )
        add_mini_test(event_loop_test )
        add_mini_test(tcp_connection_test )
        add_mini_test(event_loop_thread_test )
        add_mini_test(send_thread_test )
        add_mini_test(event_loop_thread_pool_test )
        add_mini_test(inet_address_test )
        add_mini_test(connector_test )
        add_mini_test(tcp_client_test)
        add_mini_test(length_header_codec_test)
        add_mini_test(force_close_test)
        add_mini_test(tcp_connection_read_control_test)
        add_mini_test(acceptor_test)
        add_mini_test(tcp_server_stop_test)
        add_mini_test(tcp_watermark_integration_test)
endif()