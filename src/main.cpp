#include <algorithm>
#include <cctype>
#include <climits>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <optional>
#include <string>
#include <utility>
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

enum class ValueType { kString, kList, kStream };

struct StreamItem {
  std::string id;
  std::vector<std::pair<std::string, std::string>> fields;
};

struct Entry {
  ValueType type;
  std::string stringValue;
  std::vector<std::string> listValue;
  std::vector<StreamItem> streamValue;
  long long streamLastMs;
  long long streamLastSeq;
  std::optional<std::chrono::steady_clock::time_point> expiresAt;
};

struct BlpopRequest {
  SocketType connection;
  std::vector<std::string> keys;
  std::optional<std::chrono::steady_clock::time_point> deadline;
};

struct XreadRequest {
  SocketType connection;
  std::vector<std::string> keys;
  std::vector<std::pair<long long, long long>> cursors;
  std::optional<std::chrono::steady_clock::time_point> deadline;
};

static std::unordered_map<std::string, Entry> store;
static std::unordered_map<SocketType, std::string> connectionBuffers;
static std::vector<BlpopRequest> pendingBlpopRequests;
static std::vector<XreadRequest> pendingXreadRequests;
static std::unordered_set<SocketType> socketsToClose;
static std::unordered_map<SocketType, std::vector<std::vector<std::string>>> transactionCommands;
static std::unordered_map<SocketType, std::unordered_map<std::string, int>> watchedKeys;
static std::unordered_map<std::string, int> keyVersions;

// Replication state
static std::string role = "master";  // "master" or "slave"
static std::string replication_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";  // Fixed replid for now
static long long master_repl_offset = 0;
static std::unordered_set<SocketType> replica_connections;
static std::unordered_map<SocketType, long long> replica_ack_offsets;
static std::unordered_map<SocketType, long long> replica_processed_offset;
static std::string master_host = "";
static int master_port = 0;

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

static void touch_key(const std::string& key) {
  keyVersions[key] = keyVersions.count(key) ? keyVersions[key] + 1 : 1;
}

static Entry* getEntry(const std::string& key);

static int get_key_version(const std::string& key) {
  Entry* e = getEntry(key);
  (void)e; // ensure expiry enforcement
  return keyVersions.count(key) ? keyVersions[key] : 0;
}

static void watch_keys_for_connection(SocketType connection, const std::vector<std::string>& keys) {
  auto &map = watchedKeys[connection];
  for (const auto &k : keys) {
    map[k] = get_key_version(k);
  }
}

static void clear_watched_keys(SocketType connection) {
  watchedKeys.erase(connection);
}

static std::vector<std::vector<std::string>>& get_transaction_queue(SocketType connection) {
  return transactionCommands[connection];
}

static void clear_transaction(SocketType connection) {
  transactionCommands.erase(connection);
}

static bool is_transaction_active(SocketType connection) {
  return transactionCommands.find(connection) != transactionCommands.end();
}

static bool transaction_is_dirty(SocketType connection) {
  auto it = watchedKeys.find(connection);
  if (it == watchedKeys.end()) return false;
  for (const auto &p : it->second) {
    if (get_key_version(p.first) != p.second) return true;
  }
  return false;
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
    store[key] = Entry{ValueType::kList, "", {}, {}, 0, 0, std::nullopt};
    touch_key(key);
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

static Entry* getStreamEntryForWrite(const std::string& key, bool& wrongType) {
  wrongType = false;
  Entry* entry = getEntry(key);
  if (entry == nullptr) {
    store[key] = Entry{ValueType::kStream, "", {}, {}, 0, 0, std::nullopt};
    return &store[key];
  }

  if (entry->type != ValueType::kStream) {
    wrongType = true;
    return nullptr;
  }

  return entry;
}

static Entry* getStreamEntryForRead(const std::string& key, bool& wrongType) {
  wrongType = false;
  Entry* entry = getEntry(key);
  if (entry == nullptr) {
    return nullptr;
  }

  if (entry->type != ValueType::kStream) {
    wrongType = true;
    return nullptr;
  }

  return entry;
}

static std::optional<std::pair<long long, long long>> parseStreamId(const std::string& token) {
  size_t dash = token.find('-');
  if (dash == std::string::npos || dash == 0 || dash + 1 >= token.size()) {
    return std::nullopt;
  }

  std::string msText = token.substr(0, dash);
  std::string seqText = token.substr(dash + 1);
  if (msText.empty() || seqText.empty()) {
    return std::nullopt;
  }

  char* end = nullptr;
  long long ms = std::strtoll(msText.c_str(), &end, 10);
  if (*end != '\0' || ms < 0) {
    return std::nullopt;
  }

  end = nullptr;
  long long seq = std::strtoll(seqText.c_str(), &end, 10);
  if (*end != '\0' || seq < 0) {
    return std::nullopt;
  }

  return std::make_pair(ms, seq);
}

static std::optional<long long> parseNonNegativeLongLong(const std::string& token) {
  if (token.empty()) {
    return std::nullopt;
  }

  char* end = nullptr;
  long long value = std::strtoll(token.c_str(), &end, 10);
  if (*end != '\0' || value < 0) {
    return std::nullopt;
  }

  return value;
}

static bool streamIdGreater(const std::pair<long long, long long>& lhs,
                            const std::pair<long long, long long>& rhs) {
  if (lhs.first != rhs.first) {
    return lhs.first > rhs.first;
  }
  return lhs.second > rhs.second;
}

static bool streamIdLessOrEqual(const std::pair<long long, long long>& lhs,
                                const std::pair<long long, long long>& rhs) {
  if (lhs.first != rhs.first) {
    return lhs.first < rhs.first;
  }
  return lhs.second <= rhs.second;
}

static std::string streamIdToString(const std::pair<long long, long long>& id) {
  return std::to_string(id.first) + "-" + std::to_string(id.second);
}

static std::pair<long long, long long> generateStreamId(Entry* streamEntry,
                                                        std::optional<long long> requestedMs,
                                                        bool& ok,
                                                        std::string& error) {
  long long currentMs = static_cast<long long>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
  if (requestedMs.has_value()) {
    currentMs = *requestedMs;
  }

  if (currentMs < streamEntry->streamLastMs) {
    ok = false;
    error = "ERR The ID specified in XADD is equal or smaller than the target stream top item";
    return {0, 0};
  }

  if (currentMs == streamEntry->streamLastMs) {
    streamEntry->streamLastSeq += 1;
  } else {
    streamEntry->streamLastMs = currentMs;
    streamEntry->streamLastSeq = 0;
  }

  ok = true;
  return {streamEntry->streamLastMs, streamEntry->streamLastSeq};
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

static std::string encodeStreamItem(const StreamItem& item) {
  std::string response = "*2\r\n";
  response += encodeBulk(item.id);

  std::vector<std::string> flattened;
  flattened.reserve(item.fields.size() * 2);
  for (const auto& kv : item.fields) {
    flattened.push_back(kv.first);
    flattened.push_back(kv.second);
  }
  response += encodeArray(flattened);
  return response;
}

static std::string encodeStreamItemsArray(const std::vector<StreamItem>& items) {
  std::string response = "*" + std::to_string(items.size()) + "\r\n";
  for (const auto& item : items) {
    response += encodeStreamItem(item);
  }
  return response;
}

static std::string encodeXreadResponse(
    const std::vector<std::pair<std::string, std::vector<StreamItem>>>& streamGroups) {
  std::string response = "*" + std::to_string(streamGroups.size()) + "\r\n";
  for (const auto& group : streamGroups) {
    response += "*2\r\n";
    response += encodeBulk(group.first);
    response += encodeStreamItemsArray(group.second);
  }
  return response;
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

static std::pair<long long, long long> parseXrangeBound(const std::string& token, bool isStart,
                                                        bool& valid, bool& isLowInf,
                                                        bool& isHighInf) {
  valid = true;
  isLowInf = false;
  isHighInf = false;

  if (token == "-") {
    isLowInf = true;
    return {0, 0};
  }

  if (token == "+") {
    isHighInf = true;
    return {0, 0};
  }

  size_t dash = token.find('-');
  if (dash == std::string::npos) {
    auto parsedMs = parseNonNegativeLongLong(token);
    if (!parsedMs.has_value()) {
      valid = false;
      return {0, 0};
    }
    if (isStart) {
      return {*parsedMs, 0};
    }
    return {*parsedMs, std::numeric_limits<long long>::max()};
  }

  auto parsed = parseStreamId(token);
  if (!parsed.has_value()) {
    valid = false;
    return {0, 0};
  }
  return *parsed;
}

static std::vector<StreamItem> collectXrangeEntries(const Entry* streamEntry,
                                                    const std::pair<long long, long long>& start,
                                                    const std::pair<long long, long long>& end,
                                                    bool lowInf, bool highInf) {
  std::vector<StreamItem> items;
  for (const auto& item : streamEntry->streamValue) {
    auto parsed = parseStreamId(item.id);
    if (!parsed.has_value()) {
      continue;
    }

    bool geStart = lowInf || streamIdGreater(*parsed, start) || *parsed == start;
    bool leEnd = highInf || streamIdLessOrEqual(*parsed, end);

    if (geStart && leEnd) {
      items.push_back(item);
    }
  }
  return items;
}

static bool resolveXreadCursor(const std::string& key, const std::string& token,
                               std::pair<long long, long long>& cursor, bool& wrongType,
                               bool& invalid) {
  wrongType = false;
  invalid = false;

  if (token != "$") {
    auto parsed = parseStreamId(token);
    if (!parsed.has_value()) {
      invalid = true;
      return false;
    }
    cursor = *parsed;
    return true;
  }

  Entry* streamEntry = getStreamEntryForRead(key, wrongType);
  if (wrongType) {
    return false;
  }

  if (streamEntry == nullptr || streamEntry->streamValue.empty()) {
    cursor = {0, 0};
    return true;
  }

  auto last = parseStreamId(streamEntry->streamValue.back().id);
  if (!last.has_value()) {
    cursor = {0, 0};
    return true;
  }

  cursor = *last;
  return true;
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
    touch_key(key);
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
    touch_key(key);
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

static std::vector<StreamItem> buildStreamEntriesAfterCursor(const Entry* streamEntry,
                                                             const std::pair<long long, long long>& cursor) {
  std::vector<StreamItem> result;
  for (const auto& item : streamEntry->streamValue) {
    auto parsed = parseStreamId(item.id);
    if (!parsed.has_value()) {
      continue;
    }

    if (streamIdGreater(*parsed, cursor)) {
      result.push_back(item);
    }
  }
  return result;
}

static bool buildXreadGroups(const std::vector<std::string>& keys,
                             const std::vector<std::pair<long long, long long>>& cursors,
                             std::vector<std::pair<std::string, std::vector<StreamItem>>>& groups,
                             bool& wrongType) {
  wrongType = false;
  groups.clear();

  for (size_t i = 0; i < keys.size(); i++) {
    bool localWrongType = false;
    Entry* streamEntry = getStreamEntryForRead(keys[i], localWrongType);
    if (localWrongType) {
      wrongType = true;
      return false;
    }

    if (streamEntry == nullptr) {
      continue;
    }

    std::vector<StreamItem> entries = buildStreamEntriesAfterCursor(streamEntry, cursors[i]);
    if (!entries.empty()) {
      groups.push_back({keys[i], entries});
    }
  }

  return true;
}

static void wakePendingXreadRequests() {
  size_t index = 0;
  while (index < pendingXreadRequests.size()) {
    bool wrongType = false;
    std::vector<std::pair<std::string, std::vector<StreamItem>>> groups;
    if (!buildXreadGroups(pendingXreadRequests[index].keys, pendingXreadRequests[index].cursors, groups,
                          wrongType)) {
      if (wrongType) {
        if (!sendAll(pendingXreadRequests[index].connection,
                     encodeError("WRONGTYPE Operation against a key holding the wrong kind of value"))) {
          markSocketForClose(pendingXreadRequests[index].connection);
        }
        pendingXreadRequests.erase(pendingXreadRequests.begin() + static_cast<long long>(index));
        continue;
      }
      index++;
      continue;
    }

    if (groups.empty()) {
      index++;
      continue;
    }

    if (!sendAll(pendingXreadRequests[index].connection, encodeXreadResponse(groups))) {
      markSocketForClose(pendingXreadRequests[index].connection);
    }
    pendingXreadRequests.erase(pendingXreadRequests.begin() + static_cast<long long>(index));
  }
}

static void expirePendingXreadRequests() {
  auto now = std::chrono::steady_clock::now();

  size_t index = 0;
  while (index < pendingXreadRequests.size()) {
    if (!pendingXreadRequests[index].deadline.has_value() ||
        now < *(pendingXreadRequests[index].deadline)) {
      index++;
      continue;
    }

    if (!sendAll(pendingXreadRequests[index].connection, encodeNullArray())) {
      markSocketForClose(pendingXreadRequests[index].connection);
    }
    pendingXreadRequests.erase(pendingXreadRequests.begin() + static_cast<long long>(index));
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

  for (const auto& req : pendingXreadRequests) {
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

static void removePendingXreadForConnection(SocketType connection) {
  size_t index = 0;
  while (index < pendingXreadRequests.size()) {
    if (pendingXreadRequests[index].connection == connection) {
      pendingXreadRequests.erase(pendingXreadRequests.begin() + static_cast<long long>(index));
      continue;
    }
    index++;
  }
}

static void closeClient(SocketType connection, std::vector<SocketType>& clients) {
  closeSocket(connection);
  connectionBuffers.erase(connection);
  removePendingBlpopForConnection(connection);
  removePendingXreadForConnection(connection);
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

  // While inside MULTI, all non-transaction-control commands are queued.
  if (is_transaction_active(connection)) {
    if (command != "MULTI" && command != "EXEC" && command != "DISCARD" &&
        command != "WATCH" && command != "UNWATCH") {
      get_transaction_queue(connection).push_back(cmd);
      return {true, encodeSimple("QUEUED")};
    }
  }

  if (command == "INCR" && cmd.size() >= 2) {
    const std::string& key = cmd[1];
    Entry* entry = getEntry(key);
    if (entry == nullptr) {
      store[key] = Entry{ValueType::kString, "1", {}, {}, 0, 0, std::nullopt};
      touch_key(key);
      return {true, encodeInteger(1)};
    }

    if (entry->type != ValueType::kString) {
      return {true, encodeError("ERR value is not an integer or out of range")};
    }

    try {
      long long cur = std::stoll(entry->stringValue);
      cur += 1;
      entry->stringValue = std::to_string(cur);
      touch_key(key);
      return {true, encodeInteger(cur)};
    } catch (...) {
      return {true, encodeError("ERR value is not an integer or out of range")};
    }
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

    store[key] = Entry{ValueType::kString, value, {}, {}, 0, 0, expiresAt};
    touch_key(key);
    return {true, encodeSimple("OK")};
  }

  if (command == "GET") {
    if (cmd.size() < 2) {
      return {true, encodeError("ERR wrong number of arguments for 'get' command")};
    }
    return {true, encodeBulk(getStringValue(cmd[1]))};
  }

  if (command == "INFO") {
    std::string section = "default";
    if (cmd.size() >= 2) {
      section = cmd[1];
      // Convert to lowercase for comparison
      std::transform(section.begin(), section.end(), section.begin(),
                     [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    }

    std::string info;
    if (section == "replication" || section == "all" || section == "default") {
      info = "# Replication\r\n";
      info += "role:" + role + "\r\n";
      
      if (role == "master") {
        info += "connected_slaves:0\r\n";
        info += "master_replid:" + replication_id + "\r\n";
        info += "master_repl_offset:" + std::to_string(master_repl_offset) + "\r\n";
      } else {
        // Slave-specific fields
        info += "master_host:" + master_host + "\r\n";
        info += "master_port:" + std::to_string(master_port) + "\r\n";
        info += "master_link_status:connecting\r\n";  // For now, always connecting
      }
    }

    return {true, encodeBulk(info)};
  }

  if (command == "TYPE" && cmd.size() >= 2) {
    Entry* entry = getEntry(cmd[1]);
    if (entry == nullptr) {
      return {true, encodeSimple("none")};
    }

    if (entry->type == ValueType::kString) {
      return {true, encodeSimple("string")};
    }
    if (entry->type == ValueType::kList) {
      return {true, encodeSimple("list")};
    }
    return {true, encodeSimple("stream")};
  }

  if (command == "XADD" && cmd.size() >= 5) {
    const std::string& key = cmd[1];
    const std::string& idToken = cmd[2];
    if ((cmd.size() - 3) % 2 != 0) {
      return {true, encodeError("ERR wrong number of arguments for 'xadd' command")};
    }

    bool wrongType = false;
    Entry* streamEntry = getStreamEntryForWrite(key, wrongType);
    if (wrongType || streamEntry == nullptr) {
      return {true, encodeError("WRONGTYPE Operation against a key holding the wrong kind of value")};
    }

    std::pair<long long, long long> nextId{0, 0};
    if (idToken == "*") {
      bool ok = false;
      std::string error;
      nextId = generateStreamId(streamEntry, std::nullopt, ok, error);
      if (!ok) {
        return {true, encodeError(error)};
      }
    } else {
      size_t dash = idToken.find('-');
      if (dash == std::string::npos) {
        return {true, encodeError("ERR Invalid stream ID specified as stream command argument")};
      }

      std::string msText = idToken.substr(0, dash);
      std::string seqText = idToken.substr(dash + 1);

      if (seqText == "*") {
        auto requestedMs = parseNonNegativeLongLong(msText);
        if (!requestedMs.has_value()) {
          return {true, encodeError("ERR Invalid stream ID specified as stream command argument")};
        }

        bool ok = false;
        std::string error;
        nextId = generateStreamId(streamEntry, *requestedMs, ok, error);
        if (!ok) {
          return {true, encodeError(error)};
        }
      } else {
        auto parsed = parseStreamId(idToken);
        if (!parsed.has_value()) {
          return {true, encodeError("ERR Invalid stream ID specified as stream command argument")};
        }

        nextId = *parsed;
        if (nextId.first == 0 && nextId.second == 0) {
          return {true, encodeError("ERR The ID specified in XADD must be greater than 0-0")};
        }

        std::pair<long long, long long> top{streamEntry->streamLastMs, streamEntry->streamLastSeq};
        if (!streamIdGreater(nextId, top)) {
          return {
              true,
              encodeError("ERR The ID specified in XADD is equal or smaller than the target stream top item")};
        }
        streamEntry->streamLastMs = nextId.first;
        streamEntry->streamLastSeq = nextId.second;
      }
    }

    StreamItem item;
    item.id = streamIdToString(nextId);
    for (size_t i = 3; i < cmd.size(); i += 2) {
      item.fields.push_back({cmd[i], cmd[i + 1]});
    }
    streamEntry->streamValue.push_back(item);
    touch_key(key);
    wakePendingXreadRequests();
    return {true, encodeBulk(item.id)};
  }

  if (command == "XRANGE" && cmd.size() >= 4) {
    bool wrongType = false;
    Entry* streamEntry = getStreamEntryForRead(cmd[1], wrongType);
    if (wrongType) {
      return {true, encodeError("WRONGTYPE Operation against a key holding the wrong kind of value")};
    }
    if (streamEntry == nullptr) {
      return {true, encodeArray({})};
    }

    bool startValid = false;
    bool startLowInf = false;
    bool startHighInf = false;
    auto start = parseXrangeBound(cmd[2], true, startValid, startLowInf, startHighInf);
    if (!startValid) {
      return {true, encodeError("ERR Invalid stream ID specified as stream command argument")};
    }

    bool endValid = false;
    bool endLowInf = false;
    bool endHighInf = false;
    auto end = parseXrangeBound(cmd[3], false, endValid, endLowInf, endHighInf);
    if (!endValid) {
      return {true, encodeError("ERR Invalid stream ID specified as stream command argument")};
    }

    std::vector<StreamItem> items =
        collectXrangeEntries(streamEntry, start, end, startLowInf, endHighInf);
    return {true, encodeStreamItemsArray(items)};
  }

  if (command == "XREAD") {
    size_t index = 1;
    std::optional<long long> blockMs = std::nullopt;

    if (index < cmd.size() && toUpper(cmd[index]) == "BLOCK") {
      if (index + 1 >= cmd.size()) {
        return {true, encodeError("ERR wrong number of arguments for 'xread' command")};
      }
      auto parsed = parseNonNegativeLongLong(cmd[index + 1]);
      if (!parsed.has_value()) {
        return {true, encodeError("ERR timeout is not an integer or out of range")};
      }
      blockMs = *parsed;
      index += 2;
    }

    if (index >= cmd.size() || toUpper(cmd[index]) != "STREAMS") {
      return {true, encodeError("ERR syntax error")};
    }

    std::vector<std::string> values(cmd.begin() + static_cast<long long>(index + 1), cmd.end());
    if (values.size() < 2 || values.size() % 2 != 0) {
      return {true, encodeError("ERR Unbalanced XREAD list of streams")};
    }

    size_t half = values.size() / 2;
    std::vector<std::string> keys(values.begin(), values.begin() + static_cast<long long>(half));
    std::vector<std::string> idTokens(values.begin() + static_cast<long long>(half), values.end());

    std::vector<std::pair<long long, long long>> cursors;
    cursors.reserve(keys.size());

    for (size_t i = 0; i < keys.size(); i++) {
      bool wrongType = false;
      bool invalid = false;
      std::pair<long long, long long> cursor{0, 0};
      if (!resolveXreadCursor(keys[i], idTokens[i], cursor, wrongType, invalid)) {
        if (wrongType) {
          return {true, encodeError("WRONGTYPE Operation against a key holding the wrong kind of value")};
        }
        if (invalid) {
          return {true, encodeError("ERR Invalid stream ID specified as stream command argument")};
        }
      }
      cursors.push_back(cursor);
    }

    bool wrongType = false;
    std::vector<std::pair<std::string, std::vector<StreamItem>>> groups;
    if (!buildXreadGroups(keys, cursors, groups, wrongType)) {
      if (wrongType) {
        return {true, encodeError("WRONGTYPE Operation against a key holding the wrong kind of value")};
      }
    }

    if (!groups.empty()) {
      return {true, encodeXreadResponse(groups)};
    }

    if (!blockMs.has_value()) {
      return {true, encodeNullArray()};
    }

    std::optional<std::chrono::steady_clock::time_point> deadline = std::nullopt;
    if (*blockMs > 0) {
      deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(*blockMs);
    }

    pendingXreadRequests.push_back(XreadRequest{connection, keys, cursors, deadline});
    return {false, ""};
  }

  if (command == "WATCH") {
    if (is_transaction_active(connection)) {
      return {true, encodeError("ERR WATCH inside MULTI is not allowed")};
    }
    std::vector<std::string> keys(cmd.begin() + 1, cmd.end());
    watch_keys_for_connection(connection, keys);
    return {true, encodeSimple("OK")};
  }

  if (command == "UNWATCH") {
    clear_watched_keys(connection);
    return {true, encodeSimple("OK")};
  }

  if (command == "MULTI") {
    if (!is_transaction_active(connection)) {
      get_transaction_queue(connection); // create empty queue
    }
    return {true, encodeSimple("OK")};
  }

  if (command == "DISCARD") {
    if (!is_transaction_active(connection)) {
      return {true, encodeError("ERR DISCARD without MULTI")};
    }
    clear_transaction(connection);
    clear_watched_keys(connection);
    return {true, encodeSimple("OK")};
  }

  if (command == "EXEC") {
    if (!is_transaction_active(connection)) {
      return {true, encodeError("ERR EXEC without MULTI")};
    }

    if (transaction_is_dirty(connection)) {
      clear_transaction(connection);
      clear_watched_keys(connection);
      return {true, encodeNullArray()};
    }

    auto queued = get_transaction_queue(connection);
    clear_transaction(connection);
    clear_watched_keys(connection);

    std::vector<std::string> results;
    results.reserve(queued.size());
    for (const auto& qcmd : queued) {
      CommandResult r = executeCommand(connection, qcmd);
      if (!r.hasResponse) {
        results.push_back(encodeNullArray());
      } else {
        results.push_back(r.response);
      }
    }

    // Build array reply
    std::string out = "*" + std::to_string(results.size()) + "\r\n";
    for (const auto& r : results) out += r;
    return {true, out};
  }

  if (command == "RPUSH" && cmd.size() >= 3) {
    std::vector<std::string>* listValues = getListForWrite(cmd[1]);
    if (listValues == nullptr) {
      return {true, encodeError("WRONGTYPE Operation against a key holding the wrong kind of value")};
    }

    for (size_t i = 2; i < cmd.size(); i++) {
      listValues->push_back(cmd[i]);
    }

    long long lengthAfterPush = static_cast<long long>(listValues->size());
    touch_key(cmd[1]);
    wakePendingBlpopRequests();
    return {true, encodeInteger(lengthAfterPush)};
  }

  if (command == "LPUSH" && cmd.size() >= 3) {
    std::vector<std::string>* listValues = getListForWrite(cmd[1]);
    if (listValues == nullptr) {
      return {true, encodeError("WRONGTYPE Operation against a key holding the wrong kind of value")};
    }

    for (size_t i = 2; i < cmd.size(); i++) {
      listValues->insert(listValues->begin(), cmd[i]);
    }

    long long lengthAfterPush = static_cast<long long>(listValues->size());
    touch_key(cmd[1]);
    wakePendingBlpopRequests();
    return {true, encodeInteger(lengthAfterPush)};
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

  if (command == "WAIT" && cmd.size() >= 3) {
    // WAIT numreplicas timeout
    int numReplicas = 0;
    if (!parseInt(cmd[1], numReplicas) || numReplicas < 0) {
      return {true, encodeError("ERR numreplicas is not an integer or out of range")};
    }

    double timeoutSeconds = 0;
    if (!parseDouble(cmd[2], timeoutSeconds) || timeoutSeconds < 0) {
      return {true, encodeError("ERR timeout is not a float or out of range")};
    }

    // For now, with no replicas connected, return 0
    // In full replication, this would track ACKs from connected replicas
    return {true, encodeInteger(static_cast<long long>(replica_connections.size()))};
  }

  return {true, encodeError("ERR unknown command")};
}

int main(int argc, char* argv[]) {
#ifdef _WIN32
  WSADATA wsaData;
  if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
    std::cerr << "WSAStartup failed\n";
    return 1;
  }
#endif

  // Parse command-line arguments
  int port = 6379;  // Default port
  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if (arg == "--port" && i + 1 < argc) {
      try {
        port = std::stoi(argv[i + 1]);
        i++;  // Skip next arg since we consumed it
      } catch (...) {
        std::cerr << "Invalid port number\n";
        return 1;
      }
    } else if (arg == "--replicaof" && i + 2 < argc) {
      // Parse master host and port
      master_host = argv[i + 1];
      try {
        master_port = std::stoi(argv[i + 2]);
        role = "slave";
        i += 2;  // Skip next two args
      } catch (...) {
        std::cerr << "Invalid master port\n";
        return 1;
      }
    }
  }

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
  serverAddr.sin_port = htons(static_cast<uint16_t>(port));
  serverAddr.sin_addr.s_addr = htonl(INADDR_ANY);

  if (bind(serverFd, reinterpret_cast<sockaddr*>(&serverAddr), sizeof(serverAddr)) != 0) {
    std::cerr << "Failed to bind to port " << port << "\n";
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
        closeClient(c, clients);
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
        closeClient(c, clients);
        continue;
      }

      i++;
    }

    wakePendingBlpopRequests();
    expirePendingBlpopRequests();
    wakePendingXreadRequests();
    expirePendingXreadRequests();
    flushDeadClients(clients);
  }

  closeSocket(serverFd);
#ifdef _WIN32
  WSACleanup();
#endif
  return 0;
}