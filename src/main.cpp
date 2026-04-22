#include <algorithm>
#include <chrono>
#include <cstring>
#include <iostream>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "Ws2_32.lib")
using SocketType = SOCKET;
static constexpr SocketType kInvalidSocket = INVALID_SOCKET;
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>
using SocketType = int;
static constexpr SocketType kInvalidSocket = -1;
#endif

struct StringEntry {
  std::string value;
  std::optional<std::chrono::steady_clock::time_point> expiresAt;
};

static std::unordered_map<std::string, StringEntry> store;
static std::unordered_map<SocketType, std::string> connectionBuffers;

static void closeSocket(SocketType s) {
#ifdef _WIN32
  closesocket(s);
#else
  close(s);
#endif
}

static bool sendAll(SocketType s, const std::string& data) {
  size_t sentTotal = 0;
  while (sentTotal < data.size()) {
#ifdef _WIN32
    int sent = send(s, data.data() + sentTotal, static_cast<int>(data.size() - sentTotal), 0);
#else
    ssize_t sent = send(s, data.data() + sentTotal, data.size() - sentTotal, 0);
#endif
    if (sent <= 0) {
      return false;
    }
    sentTotal += static_cast<size_t>(sent);
  }
  return true;
}

static std::optional<std::string> getStringValue(const std::string& key) {
  auto it = store.find(key);
  if (it == store.end()) return std::nullopt;

  if (it->second.expiresAt.has_value() &&
      std::chrono::steady_clock::now() >= *(it->second.expiresAt)) {
    store.erase(it);
    return std::nullopt;
  }

  return it->second.value;
}

static std::optional<size_t> findCrlf(const std::string& s, size_t start) {
  size_t pos = s.find("\r\n", start);
  if (pos == std::string::npos) return std::nullopt;
  return pos;
}

static std::vector<std::vector<std::string>> parseRespCommands(std::string& buffer) {
  std::vector<std::vector<std::string>> commands;
  size_t offset = 0;

  while (offset < buffer.size()) {
    if (buffer[offset] == '\r' || buffer[offset] == '\n') {
      offset++;
      continue;
    }

    if (buffer[offset] != '*') break;

    auto lineEndOpt = findCrlf(buffer, offset);
    if (!lineEndOpt.has_value()) break;
    size_t lineEnd = *lineEndOpt;

    int count = 0;
    try {
      count = std::stoi(buffer.substr(offset + 1, lineEnd - (offset + 1)));
    } catch (...) {
      break;
    }

    size_t cursor = lineEnd + 2;
    std::vector<std::string> parts;
    bool complete = true;

    for (int i = 0; i < count; i++) {
      if (cursor >= buffer.size() || buffer[cursor] != '$') {
        complete = false;
        break;
      }

      auto bulkLenEndOpt = findCrlf(buffer, cursor);
      if (!bulkLenEndOpt.has_value()) {
        complete = false;
        break;
      }
      size_t bulkLenEnd = *bulkLenEndOpt;

      int bulkLen = 0;
      try {
        bulkLen = std::stoi(buffer.substr(cursor + 1, bulkLenEnd - (cursor + 1)));
      } catch (...) {
        complete = false;
        break;
      }

      size_t valueStart = bulkLenEnd + 2;
      size_t valueEnd = valueStart + static_cast<size_t>(bulkLen);
      if (valueEnd + 2 > buffer.size()) {
        complete = false;
        break;
      }

      if (buffer.compare(valueEnd, 2, "\r\n") != 0) {
        complete = false;
        break;
      }

      parts.push_back(buffer.substr(valueStart, static_cast<size_t>(bulkLen)));
      cursor = valueEnd + 2;
    }

    if (!complete) break;

    commands.push_back(std::move(parts));
    offset = cursor;
  }

  buffer.erase(0, offset);
  return commands;
}

static std::string encodeSimple(const std::string& value) {
  return "+" + value + "\r\n";
}

static std::string encodeError(const std::string& message) {
  return "-" + message + "\r\n";
}

static std::string encodeBulk(const std::optional<std::string>& value) {
  if (!value.has_value()) return "$-1\r\n";
  return "$" + std::to_string(value->size()) + "\r\n" + *value + "\r\n";
}

static std::string toUpper(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
  return s;
}

static std::string executeCommand(const std::vector<std::string>& cmd) {
  if (cmd.empty()) return encodeError("ERR empty command");

  std::string command = toUpper(cmd[0]);

  if (command == "PING") {
    return encodeSimple("PONG");
  }

  if (command == "ECHO") {
    if (cmd.size() < 2) return encodeError("ERR wrong number of arguments for 'echo' command");
    return encodeBulk(cmd[1]);
  }

  if (command == "SET") {
    if (cmd.size() < 3) return encodeError("ERR wrong number of arguments for 'set' command");

    const std::string& key = cmd[1];
    const std::string& value = cmd[2];

    std::optional<std::chrono::steady_clock::time_point> expiresAt = std::nullopt;
    if (cmd.size() >= 5) {
      std::string opt = toUpper(cmd[3]);
      if (opt == "PX") {
        long long ttlMs = 0;
        try {
          ttlMs = std::stoll(cmd[4]);
        } catch (...) {
          return encodeError("ERR value is not an integer or out of range");
        }
        if (ttlMs <= 0) {
          expiresAt = std::chrono::steady_clock::now();
        } else {
          expiresAt = std::chrono::steady_clock::now() + std::chrono::milliseconds(ttlMs);
        }
      }
    }

    store[key] = StringEntry{value, expiresAt};
    return encodeSimple("OK");
  }

  if (command == "GET") {
    if (cmd.size() < 2) return encodeError("ERR wrong number of arguments for 'get' command");
    return encodeBulk(getStringValue(cmd[1]));
  }

  return encodeError("ERR unknown command");
}

int main() {
#ifdef _WIN32
  WSADATA wsaData;
  if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
    std::cerr << "WSAStartup failed\n";
    return 1;
  }
#endif

  SocketType serverFd = socket(AF_INET, SOCK_STREAM, 0);
  if (serverFd == kInvalidSocket) {
    std::cerr << "Failed to create server socket\n";
    return 1;
  }

  int reuse = 1;
#ifdef _WIN32
  if (setsockopt(serverFd, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&reuse),
                 sizeof(reuse)) < 0) {
#else
  if (setsockopt(serverFd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
#endif
    std::cerr << "setsockopt failed\n";
    closeSocket(serverFd);
    return 1;
  }

  sockaddr_in serverAddr{};
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_port = htons(6379);
  serverAddr.sin_addr.s_addr = htonl(INADDR_ANY);

  if (bind(serverFd, reinterpret_cast<sockaddr*>(&serverAddr), sizeof(serverAddr)) != 0) {
    std::cerr << "Failed to bind to port 6379\n";
    closeSocket(serverFd);
    return 1;
  }

  if (listen(serverFd, 128) != 0) {
    std::cerr << "listen failed\n";
    closeSocket(serverFd);
    return 1;
  }

  std::cout << "Logs from your program will appear here!\n";

  std::vector<SocketType> clients;

  while (true) {
    fd_set readfds;
    FD_ZERO(&readfds);
    FD_SET(serverFd, &readfds);

    SocketType maxFd = serverFd;
    for (SocketType c : clients) {
      FD_SET(c, &readfds);
      if (c > maxFd) maxFd = c;
    }

    int ready = select(static_cast<int>(maxFd + 1), &readfds, nullptr, nullptr, nullptr);
    if (ready < 0) {
      continue;
    }

    if (FD_ISSET(serverFd, &readfds)) {
      sockaddr_in clientAddr{};
#ifdef _WIN32
      int addrLen = sizeof(clientAddr);
#else
      socklen_t addrLen = sizeof(clientAddr);
#endif
      SocketType conn =
          accept(serverFd, reinterpret_cast<sockaddr*>(&clientAddr), &addrLen);
      if (conn != kInvalidSocket) {
        clients.push_back(conn);
      }
    }

    for (size_t i = 0; i < clients.size();) {
      SocketType c = clients[i];
      if (!FD_ISSET(c, &readfds)) {
        i++;
        continue;
      }

      char buf[4096];
#ifdef _WIN32
      int n = recv(c, buf, sizeof(buf), 0);
#else
      ssize_t n = recv(c, buf, sizeof(buf), 0);
#endif
      if (n <= 0) {
        closeSocket(c);
        connectionBuffers.erase(c);
        clients.erase(clients.begin() + static_cast<long long>(i));
        continue;
      }

      connectionBuffers[c].append(buf, static_cast<size_t>(n));
      auto commands = parseRespCommands(connectionBuffers[c]);

      bool ok = true;
      for (const auto& command : commands) {
        std::string response = executeCommand(command);
        if (!sendAll(c, response)) {
          ok = false;
          break;
        }
      }

      if (!ok) {
        closeSocket(c);
        connectionBuffers.erase(c);
        clients.erase(clients.begin() + static_cast<long long>(i));
        continue;
      }

      i++;
    }
  }

  closeSocket(serverFd);
#ifdef _WIN32
  WSACleanup();
#endif
  return 0;
}