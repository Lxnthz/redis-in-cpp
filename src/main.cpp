#include <algorithm>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
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

enum class ValueType { kString, kList };

struct Entry {
  ValueType type;
  std::string stringValue;
  std::vector<std::string> listValue;
  std::optional<std::chrono::steady_clock::time_point> expiresAt;
};

struct BlpopRequest {
  SocketType connection;
  std::vector<std::string> keys;
  std::optional<std::chrono::steady_clock::time_point> deadline;
};

static std::unordered_map<std::string, Entry> store;
static std::unordered_map<SocketType, std::string> connectionBuffers;
static std::vector<BlpopRequest> pendingBlpopRequests;
static std::unordered_set<SocketType> socketsToClose;

struct CommandResult {
  bool hasResponse;
  std::string response;
};

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

static void markSocketForClose(SocketType s) {
  socketsToClose.insert(s);
}

static bool parseInt(const std::string& input, int& value) {
  if (input.empty()) return false;
  char* end = nullptr;
  long parsed = std::strtol(input.c_str(), &end, 10);
  if (*end != '\0') return false;
  if (parsed < INT32_MIN || parsed > INT32_MAX) return false;
  value = static_cast<int>(parsed);
  return true;
}

static bool parseDouble(const std::string& input, double& value) {
  if (input.empty()) return false;
  char* end = nullptr;
  value = std::strtod(input.c_str(), &end);
  return *end == '\0';
}

static Entry* getEntry(const std::string& key) {
  auto it = store.find(key);
  if (it == store.end()) return nullptr;

  if (it->second.expiresAt.has_value() &&
      std::chrono::steady_clock::now() >= *(it->second.expiresAt)) {
    store.erase(it);
    return nullptr;
  }

  return &store[key];
}

static std::vector<std::string>* getListForWrite(const std::string& key) {
  Entry* entry = getEntry(key);
  if (entry == nullptr) {
    store[key] = Entry{ValueType::kList, "", {}, std::nullopt};
    return &store[key].listValue;
  }

  if (entry->type != ValueType::kList) {
    return nullptr;
  }

  return &entry->listValue;
}

static std::vector<std::string>* getListForRead(const std::string& key) {
  Entry* entry = getEntry(key);
  if (entry == nullptr) {
    return nullptr;
  }

  if (entry->type != ValueType::kList) {
    return reinterpret_cast<std::vector<std::string>*>(-1);
  }

  return &entry->listValue;
}

static std::optional<std::string> getStringValue(const std::string& key) {
  Entry* entry = getEntry(key);
  if (entry == nullptr) return std::nullopt;

  if (entry->type != ValueType::kString) {
    return std::nullopt;
  }

  return entry->stringValue;
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

static std::string encodeInteger(long long value) {
  return ":" + std::to_string(value) + "\r\n";
}

static std::string encodeBulk(const std::optional<std::string>& value) {
  if (!value.has_value()) return "$-1\r\n";
  return "$" + std::to_string(value->size()) + "\r\n" + *value + "\r\n";
}

static std::string encodeArray(const std::vector<std::string>& values) {
  std::string response = "*" + std::to_string(values.size()) + "\r\n";
  for (const auto& value : values) {
    response += "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
  }
  return response;
}

static std::string encodeNullArray() {
  return "*-1\r\n";
}

static std::string toUpper(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
  return s;
}

static std::vector<std::string> trimLrange(const std::vector<std::string>& values, int start,
                                           int stop) {
  int length = static_cast<int>(values.size());
  if (length == 0) {
    return {};
  }

  if (start < 0) start += length;
  if (stop < 0) stop += length;

  start = std::max(start, 0);
  stop = std::min(stop, length - 1);

  if (start > stop || start >= length) {
    return {};
  }

  return std::vector<std::string>(values.begin() + start, values.begin() + stop + 1);
}

static std::optional<std::vector<std::string>> popFromList(const std::string& key,
                                                           std::optional<int> count) {
  std::vector<std::string>* listValues = getListForRead(key);
  if (listValues == nullptr || listValues == reinterpret_cast<std::vector<std::string>*>(-1) ||
      listValues->empty()) {
    return std::nullopt;
  }

  if (!count.has_value()) {
    std::string popped = listValues->front();
    listValues->erase(listValues->begin());
    if (listValues->empty()) {
      store.erase(key);
    }
    return std::vector<std::string>{popped};
  }

  int popCount = std::max(0, std::min(*count, static_cast<int>(listValues->size())));
  std::vector<std::string> poppedItems;
  poppedItems.reserve(popCount);
  for (int i = 0; i < popCount; i++) {
    poppedItems.push_back(listValues->front());
    listValues->erase(listValues->begin());
  }

  if (listValues->empty()) {
    store.erase(key);
  }

  return poppedItems;
}

static std::optional<std::pair<std::string, std::string>> tryPopFromKeys(
    const std::vector<std::string>& keys) {
  for (const auto& key : keys) {
    auto popped = popFromList(key, std::nullopt);
    if (popped.has_value() && !popped->empty()) {
      return std::make_pair(key, (*popped)[0]);
    }
  }

  return std::nullopt;
}

static void wakePendingBlpopRequests() {
  size_t index = 0;
  while (index < pendingBlpopRequests.size()) {
    auto response = tryPopFromKeys(pendingBlpopRequests[index].keys);
    if (!response.has_value()) {
      index++;
      continue;
    }

    std::vector<std::string> payload{response->first, response->second};
    if (!sendAll(pendingBlpopRequests[index].connection, encodeArray(payload))) {
      markSocketForClose(pendingBlpopRequests[index].connection);
    }
    pendingBlpopRequests.erase(pendingBlpopRequests.begin() + static_cast<long long>(index));
  }
}

static void expirePendingBlpopRequests() {
  auto now = std::chrono::steady_clock::now();

  size_t index = 0;
  while (index < pendingBlpopRequests.size()) {
    if (!pendingBlpopRequests[index].deadline.has_value() ||
        now < *(pendingBlpopRequests[index].deadline)) {
      index++;
      continue;
    }

    if (!sendAll(pendingBlpopRequests[index].connection, encodeNullArray())) {
      markSocketForClose(pendingBlpopRequests[index].connection);
    }
    pendingBlpopRequests.erase(pendingBlpopRequests.begin() + static_cast<long long>(index));
  }
}

static std::optional<std::chrono::steady_clock::duration> getNearestPendingDeadline() {
  std::optional<std::chrono::steady_clock::time_point> nearest;
  for (const auto& req : pendingBlpopRequests) {
    if (!req.deadline.has_value()) {
      continue;
    }

    if (!nearest.has_value() || req.deadline < nearest) {
      nearest = req.deadline;
    }
  }

  if (!nearest.has_value()) {
    return std::nullopt;
  }

  auto now = std::chrono::steady_clock::now();
  if (now >= *nearest) {
    return std::chrono::steady_clock::duration::zero();
  }

  return *nearest - now;
}

static void removePendingBlpopForConnection(SocketType connection) {
  size_t index = 0;
  while (index < pendingBlpopRequests.size()) {
    if (pendingBlpopRequests[index].connection == connection) {
      pendingBlpopRequests.erase(pendingBlpopRequests.begin() + static_cast<long long>(index));
      continue;
    }
    index++;
  }
}

static void closeClient(SocketType connection, std::vector<SocketType>& clients) {
  closeSocket(connection);
  connectionBuffers.erase(connection);
  removePendingBlpopForConnection(connection);
  socketsToClose.erase(connection);
  clients.erase(std::remove(clients.begin(), clients.end(), connection), clients.end());
}

static void flushDeadClients(std::vector<SocketType>& clients) {
  std::vector<SocketType> dead(socketsToClose.begin(), socketsToClose.end());
  for (SocketType connection : dead) {
    closeClient(connection, clients);
  }
}

static CommandResult executeCommand(SocketType connection, const std::vector<std::string>& cmd) {
  if (cmd.empty()) {
    return {true, encodeError("ERR empty command")};
  }

  std::string command = toUpper(cmd[0]);

  if (command == "PING") {
    return {true, encodeSimple("PONG")};
  }

  if (command == "ECHO") {
    if (cmd.size() < 2) {
      return {true, encodeError("ERR wrong number of arguments for 'echo' command")};
    }
    return {true, encodeBulk(cmd[1])};
  }

  if (command == "SET") {
    if (cmd.size() < 3) {
      return {true, encodeError("ERR wrong number of arguments for 'set' command")};
    }

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
          return {true, encodeError("ERR value is not an integer or out of range")};
        }
        if (ttlMs <= 0) {
          expiresAt = std::chrono::steady_clock::now();
        } else {
          expiresAt = std::chrono::steady_clock::now() + std::chrono::milliseconds(ttlMs);
        }
      }
    }

    store[key] = Entry{ValueType::kString, value, {}, expiresAt};
    return {true, encodeSimple("OK")};
  }

  if (command == "GET") {
    if (cmd.size() < 2) {
      return {true, encodeError("ERR wrong number of arguments for 'get' command")};
    }
    return {true, encodeBulk(getStringValue(cmd[1]))};
  }

  if (command == "RPUSH" && cmd.size() >= 3) {
    std::vector<std::string>* listValues = getListForWrite(cmd[1]);
    if (listValues == nullptr) {
      return {true, encodeError("WRONGTYPE Operation against a key holding the wrong kind of value")};
    }

    for (size_t i = 2; i < cmd.size(); i++) {
      listValues->push_back(cmd[i]);
    }

    wakePendingBlpopRequests();
    return {true, encodeInteger(static_cast<long long>(listValues->size()))};
  }

  if (command == "LPUSH" && cmd.size() >= 3) {
    std::vector<std::string>* listValues = getListForWrite(cmd[1]);
    if (listValues == nullptr) {
      return {true, encodeError("WRONGTYPE Operation against a key holding the wrong kind of value")};
    }

    for (size_t i = 2; i < cmd.size(); i++) {
      listValues->insert(listValues->begin(), cmd[i]);
    }

    wakePendingBlpopRequests();
    return {true, encodeInteger(static_cast<long long>(listValues->size()))};
  }

  if (command == "LRANGE" && cmd.size() >= 4) {
    int start = 0;
    int stop = 0;
    if (!parseInt(cmd[2], start) || !parseInt(cmd[3], stop)) {
      return {true, encodeError("ERR value is not an integer or out of range")};
    }

    std::vector<std::string>* listValues = getListForRead(cmd[1]);
    if (listValues == reinterpret_cast<std::vector<std::string>*>(-1)) {
      return {true, encodeError("WRONGTYPE Operation against a key holding the wrong kind of value")};
    }
    if (listValues == nullptr) {
      return {true, encodeArray({})};
    }

    return {true, encodeArray(trimLrange(*listValues, start, stop))};
  }

  if (command == "LLEN" && cmd.size() >= 2) {
    std::vector<std::string>* listValues = getListForRead(cmd[1]);
    if (listValues == reinterpret_cast<std::vector<std::string>*>(-1)) {
      return {true, encodeError("WRONGTYPE Operation against a key holding the wrong kind of value")};
    }

    if (listValues == nullptr) {
      return {true, encodeInteger(0)};
    }

    return {true, encodeInteger(static_cast<long long>(listValues->size()))};
  }

  if (command == "LPOP" && cmd.size() >= 2) {
    std::vector<std::string>* listValues = getListForRead(cmd[1]);
    if (listValues == reinterpret_cast<std::vector<std::string>*>(-1)) {
      return {true, encodeError("WRONGTYPE Operation against a key holding the wrong kind of value")};
    }

    std::optional<int> count = std::nullopt;
    if (cmd.size() >= 3) {
      int parsed = 0;
      if (!parseInt(cmd[2], parsed) || parsed < 0) {
        return {true, encodeError("ERR value is not an integer or out of range")};
      }
      count = parsed;
    }

    auto popped = popFromList(cmd[1], count);
    if (!count.has_value()) {
      if (!popped.has_value()) {
        return {true, encodeBulk(std::nullopt)};
      }
      return {true, encodeBulk((*popped)[0])};
    }

    if (!popped.has_value()) {
      return {true, encodeNullArray()};
    }
    return {true, encodeArray(*popped)};
  }

  if (command == "BLPOP" && cmd.size() >= 3) {
    std::vector<std::string> keys(cmd.begin() + 1, cmd.end() - 1);
    if (keys.empty()) {
      return {true, encodeError("ERR wrong number of arguments for 'blpop' command")};
    }

    for (const auto& key : keys) {
      std::vector<std::string>* listValues = getListForRead(key);
      if (listValues == reinterpret_cast<std::vector<std::string>*>(-1)) {
        return {true, encodeError("WRONGTYPE Operation against a key holding the wrong kind of value")};
      }
    }

    auto immediate = tryPopFromKeys(keys);
    if (immediate.has_value()) {
      return {true, encodeArray({immediate->first, immediate->second})};
    }

    double timeoutSeconds = 0;
    if (!parseDouble(cmd.back(), timeoutSeconds) || timeoutSeconds < 0) {
      return {true, encodeError("ERR timeout is not a float or out of range")};
    }

    std::optional<std::chrono::steady_clock::time_point> deadline = std::nullopt;
    if (timeoutSeconds > 0) {
      deadline = std::chrono::steady_clock::now() +
                 std::chrono::milliseconds(static_cast<long long>(timeoutSeconds * 1000.0));
    }

    pendingBlpopRequests.push_back(BlpopRequest{connection, keys, deadline});
    return {false, ""};
  }

  return {true, encodeError("ERR unknown command")};
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

    auto timeoutDuration = getNearestPendingDeadline();
    timeval timeout{};
    timeval* timeoutPtr = nullptr;
    if (timeoutDuration.has_value()) {
      auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(*timeoutDuration).count();
      if (ms < 0) ms = 0;
      timeout.tv_sec = static_cast<long>(ms / 1000);
      timeout.tv_usec = static_cast<long>((ms % 1000) * 1000);
      timeoutPtr = &timeout;
    }

    int ready = select(static_cast<int>(maxFd + 1), &readfds, nullptr, nullptr, timeoutPtr);
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
        CommandResult result = executeCommand(c, command);
        if (!result.hasResponse) {
          continue;
        }
        if (!sendAll(c, result.response)) {
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

    wakePendingBlpopRequests();
    expirePendingBlpopRequests();
    flushDeadClients(clients);
  }

  closeSocket(serverFd);
#ifdef _WIN32
  WSACleanup();
#endif
  return 0;
}