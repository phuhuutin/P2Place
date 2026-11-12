/**
 * @file ui/websocket_server.cpp
 * @brief Implementation of the browser WebSocket server.
 */
#include "ui/websocket_server.hpp"
#include <iostream>
namespace p2place::ui {
    WebSocketServer::WebSocketServer(
        net::EventLoop& loop,
        std::uint16_t port,
        CommandHandler handler
    )
        : loop_(loop),
        listener_(port), // <-- TCP server starts listening HERE
        on_command_(std::move(handler)) {

        loop_.register_listener(
            listener_.fd(),
            [this] {
                accept_ready();
            }
        );
        std::cerr
            << "[WebSocket] Listening on port "
            << port << '\n';
    }

    WebSocketServer::~WebSocketServer() {
        loop_.unregister_listener(listener_.fd());
    }
    // Next:
    // 1. Register socket with EventLoop.
    // 2. Transfer socket ownership.
    // 3. Create BrowserSession.
    // 4. Install read/write/disconnect callbacks.
    // This is called when a new browser connection is accepted. 
    void WebSocketServer::accept_ready() {
        listener_.accept_ready(
            [this](net::Socket socket) {
                const int fd = socket;

                std::cerr
                    << "[WebSocket] Accepted client fd="
                    << fd << '\n';

                // 1. Create per-browser state.
                auto [it, inserted] =
                    sessions_.try_emplace(fd);

                if (!inserted) {
                    throw std::logic_error(
                        "Duplicate WebSocket client fd"
                    );
                }

                try {
                    // 2. Transfer socket ownership to EventLoop.
                    loop_.register_raw_connection(
                        std::move(socket),

                        // EPOLLIN callback.
                        [this](
                            int client_fd,
                            std::span<const std::uint8_t> data
                        ) {
                            on_client_data(client_fd, data);
                        },

                        // Disconnect callback.
                        [this](int client_fd) {
                            on_client_disconnect(client_fd);
                        }
                    );
                }
                catch (...) {
                    sessions_.erase(fd);
                    throw;
                }
            }
        );
    }



    void WebSocketServer::on_client_disconnect(int fd) {
        sessions_.erase(fd);

        std::cerr
            << "[WebSocket] Client disconnected fd="
            << fd << '\n';
    }


    void WebSocketServer::on_client_data(
        int fd,
        std::span<const std::uint8_t> data
    ) {
        auto it = sessions_.find(fd);

        if (it == sessions_.end()) {
            return;
        }

        auto& session = it->second;

        if (session.state == ConnectionState::Closing) {
            return;
        }

        // Buffer incoming TCP bytes.
        session.input_buffer.insert(
            session.input_buffer.end(),
            data.begin(),
            data.end()
        );

        // Bound memory usage for malformed or stalled clients.
        constexpr std::size_t MAX_INPUT_BUFFER = 32768;

        if (session.input_buffer.size() > MAX_INPUT_BUFFER) {
            std::cerr << "[WebSocket] Input too large\n";
            session.state = ConnectionState::Closing;
            loop_.close_connection(fd);
            return;
        }

        try {
            if (session.state ==
                ConnectionState::Handshaking) {

                process_handshake(fd, session);
            }

            if (session.state ==
                ConnectionState::Open) {

                process_frames(fd, session);
            }
        }
        catch (const std::exception& e) {
            std::cerr
                << "[WebSocket] Protocol error: "
                << e.what() << '\n';

            session.state = ConnectionState::Closing;
            loop_.close_connection(fd);
        }
    }


    void WebSocketServer::process_handshake(
        int fd,
        BrowserSession& session
    ) {
        constexpr std::size_t MAX_HTTP_HEADER = 8192;

        const std::string request(
            session.input_buffer.begin(),
            session.input_buffer.end()
        );

        // HTTP headers finish with CRLF CRLF.
        const auto header_end = request.find("\r\n\r\n");

        if (header_end == std::string::npos) {
            if (request.size() > MAX_HTTP_HEADER) {
                throw std::runtime_error(
                    "HTTP header exceeds 8 KiB"
                );
            }

            // Incomplete HTTP request.
            return;
        }

        const auto header_size = header_end + 4;

        if (header_size > MAX_HTTP_HEADER) {
            throw std::runtime_error(
                "HTTP header exceeds 8 KiB"
            );
        }

        // Validate and generate HTTP 101 response.
        const auto response =
            net::WebSocketCodec::handshake_response(
                std::string_view(
                    request.data(),
                    header_size
                )
            );

        if (response.empty()) {
            throw std::runtime_error(
                "Invalid WebSocket handshake"
            );
        }

        // IMPORTANT:
        // This sends raw HTTP bytes, NOT a WebSocket frame.
        const auto* first =
            reinterpret_cast<const std::uint8_t*>(
                response.data()
            );

        loop_.send_raw(
            fd,
            std::span<const std::uint8_t>(
                first,
                response.size()
            )
        );

        // Remove ONLY the HTTP header.
        // Any bytes after the header might be a
        // WebSocket frame sent in the same TCP read.
        session.input_buffer.erase(
            session.input_buffer.begin(),
            session.input_buffer.begin() + header_size
        );

        session.state = ConnectionState::Open;

        std::cerr
            << "[WebSocket] Handshake successful fd="
            << fd << '\n';
    }

    
    void WebSocketServer::process_frames(
        int fd,
        BrowserSession& session
    ) {
        constexpr std::size_t MAX_FRAMES = 64;

        for (std::size_t i = 0;
            i < MAX_FRAMES;
            ++i) {

            WebSocketFrame frame;

            const FrameResult result = decode_frame(
                session.input_buffer,
                frame
            );

            if (result == FrameResult::Incomplete) {
                return;
            }

            if (result == FrameResult::Invalid) {
                std::cerr << "[WS] Invalid frame fd="
                        << fd << '\n';

                session.state = ConnectionState::Closing;
                loop_.close_connection(fd);
                return;
            }

            switch (frame.opcode) {
            case WebSocketOpcode::Text: {
                std::string json(
                    frame.payload.begin(),
                    frame.payload.end()
                );

                std::cerr
                    << "[WS] Text received: "
                    << json << '\n';

                if (on_command_) {
                    on_command_(fd, json);
                }

                break;
            }

            case WebSocketOpcode::Ping: {
                std::cerr << "[WS] Ping received\n";

                // Pong must contain the same payload.
                send_control(
                    fd,
                    WebSocketOpcode::Pong,
                    frame.payload
                );

                break;
            }

            case WebSocketOpcode::Pong: {
                std::cerr << "[WS] Pong received\n";
                break;
            }

            case WebSocketOpcode::Close: {
                std::cerr
                    << "[WS] Close received\n" << fd << '\n';

                session.state = ConnectionState::Closing;

                // Echo a valid Close payload.
                send_control(
                    fd,
                    WebSocketOpcode::Close,
                    frame.payload
                );

                // Close AFTER sending the Close frame.
                loop_.close_after_flush(fd);
                return;
            }
            }

            if (session.state != ConnectionState::Open) {
                return;
            }
        }

        // The dispatch budget was exhausted. Arrange
        // another processing pass if buffered frames
        // remain; otherwise those frames could stall.
    }



    WebSocketServer::FrameResult WebSocketServer::decode_frame(
        std::vector<std::uint8_t>& input,
        WebSocketFrame& frame
    ) {
        constexpr std::size_t MAX_MESSAGE_SIZE = 16384;

        // Need at least the first 2 bytes.
        if (input.size() < 2) {
            return FrameResult::Incomplete;
        }

        const std::uint8_t b0 = input[0];
        const std::uint8_t b1 = input[1];

        const bool fin = (b0 & 0x80) != 0;
        const std::uint8_t rsv = b0 & 0x70;
        const std::uint8_t opcode = b0 & 0x0F;

        const bool masked = (b1 & 0x80) != 0;

        std::uint64_t length = b1 & 0x7F;
        std::size_t offset = 2;

        // No negotiated extensions or fragmentation.
        if (!fin || rsv != 0 || !masked) {
            return FrameResult::Invalid;
        }

        const bool valid_opcode =
            opcode == 0x1 ||
            opcode == 0x8 ||
            opcode == 0x9 ||
            opcode == 0xA;

        if (!valid_opcode) {
            return FrameResult::Invalid;
        }

        // Extended 16-bit payload length.
        if (length == 126) {
            if (input.size() < 4) {
                return FrameResult::Incomplete;
            }

            length =
                (std::uint64_t(input[2]) << 8) |
                std::uint64_t(input[3]);

            // Reject nonminimal length encoding.
            if (length < 126) {
                return FrameResult::Invalid;
            }

            offset = 4;
        }
        else if (length == 127) {
            // Lab 03 intentionally doesn't support
            // 64-bit frame lengths.
            return FrameResult::Invalid;
        }

        // Close, Ping and Pong are control frames.
        const bool is_control = opcode >= 0x8;

        if (is_control && length > 125) {
            return FrameResult::Invalid;
        }

        if (length > MAX_MESSAGE_SIZE) {
            return FrameResult::Invalid;
        }

        // Every client-to-server frame must contain
        // a 4-byte masking key.
        constexpr std::size_t MASK_BYTES = 4;

        const std::size_t required =
            offset + MASK_BYTES +
            static_cast<std::size_t>(length);

        if (input.size() < required) {
            return FrameResult::Incomplete;
        }

        // Decode masked payload.
        frame.opcode =
            static_cast<WebSocketOpcode>(opcode);

        frame.payload.resize(
            static_cast<std::size_t>(length)
        );

        for (std::size_t i = 0;
            i < frame.payload.size();
            ++i) {

            const std::uint8_t mask =
                input[offset + (i % 4)];

            const std::uint8_t encoded =
                input[offset + MASK_BYTES + i];

            frame.payload[i] = encoded ^ mask;
        }

        // A Close frame cannot have exactly 1 byte.
        if (opcode == 0x8 && length == 1) {
            return FrameResult::Invalid;
        }

        // Remove consumed bytes; leave remaining
        // complete or partial frames in the buffer.
        input.erase(
            input.begin(),
            input.begin() + required
        );

        return FrameResult::Complete;
    }

    
    void WebSocketServer::send_text(
        int fd,
        std::string_view text
    ) {
        auto it = sessions_.find(fd);

        if (it == sessions_.end()) {
            return;
        }

        auto& session = it->second;

        if (session.state != ConnectionState::Open) {
            return;
        }

        // Convert JSON text into bytes.
        std::vector<std::uint8_t> payload(
            text.begin(),
            text.end()
        );

        std::vector<std::uint8_t> frame;

        // FIN=1, opcode=0x1, server frame unmasked.
        if (!session.codec.append_frame(
                frame,
                payload
            )) {

            throw std::runtime_error(
                "WebSocket text exceeds maximum size"
            );
        }

        // Queue encoded WebSocket bytes through epoll.
        loop_.send_raw(fd, frame);
    }


    void WebSocketServer::send_control(
        int fd,
        WebSocketOpcode opcode,
        std::span<const std::uint8_t> payload
    ) {
        if (payload.size() > 125) {
            throw std::invalid_argument(
                "WebSocket control payload too large"
            );
        }

        std::vector<std::uint8_t> frame;

        // FIN=1, plus the specified control opcode.
        frame.push_back(
            0x80 |
            static_cast<std::uint8_t>(opcode)
        );

        // Server-to-browser frames are not masked.
        frame.push_back(
            static_cast<std::uint8_t>(payload.size())
        );

        frame.insert(
            frame.end(),
            payload.begin(),
            payload.end()
        );

        loop_.send_raw(fd, frame);
    }


    void WebSocketServer::broadcast(
        std::string_view json
    ) {
        // Snapshot the fd list so an error or session
        // removal cannot invalidate the iteration.
        std::vector<int> clients;

        clients.reserve(sessions_.size());

        for (const auto& [fd, session] : sessions_) {
            if (session.state == ConnectionState::Open) {
                clients.push_back(fd);
            }
        }

        for (int fd : clients) {
            try {
                send_text(fd, json);
            }
            catch (const std::exception& e) {
                std::cerr
                    << "[WS] Broadcast failed fd="
                    << fd
                    << ": " << e.what() << '\n';

                loop_.close_connection(fd);
            }
        }
    }

    void WebSocketServer::send_to(
        int fd,
        std::string_view json
    ) {
        send_text(fd, json);
    }


}
