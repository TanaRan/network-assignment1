"""
server.py - CSC3002F Networks Assignment Central Server
=======================================================
CSC3002F Networks Assignment - Stage 2

Four communication paradigms:
  1. Client-Server / TCP    Authentication, messaging, group management, P2P brokering
                            (persistent TCP connection per client on port 5072)
  2. Client-Server / UDP    Presence heartbeats received from clients every 10 seconds
                            (UDP datagrams on port 5073 — loss is tolerable)
  3. P2P / TCP              File/media transfer brokered via PEER_INFO, then direct
                            (server only supplies the peer's IP + port)
  4. P2P / UDP              Not used — noted in design report §3.4

Protocol:  CSC3002F Networks Assignment/1.0  (HTTP-inspired text headers + binary body)

Message Format:
    METHOD|CSC3002F Networks Assignment/1.0\r\n
    Header-Key:\tHeader-Value\r\n
    Content-Length:\t<n>\r\n
    \r\n
    <body bytes>

TCP Framing (solves the byte-stream boundary problem):
    [4-byte big-endian uint32 length][message bytes ...]
    Receiver reads 4 bytes → learns N → reads exactly N bytes.
    UDP does not need this: each recvfrom() is one complete datagram.

Design Rationale:
    - TCP for all coordination: guarantees delivery and ordering
    - UDP for heartbeats: lightweight, loss acceptable every 10 s
    - Length-prefix framing: avoids delimiter-escaping and partial-read issues
    - Threaded client handling: concurrent clients without blocking the accept loop
"""

import socket
import threading
import struct
import json
import time
import sqlite3

# ============================================================================
# Server Configuration
# ============================================================================

TCP_PORT   = 5072
UDP_PORT   = 5073
DB_PATH    = "chat.db"
SERVER_IP  = socket.gethostbyname(socket.gethostname())
TCP_ADDR   = ("0.0.0.0", TCP_PORT)
UDP_ADDR   = ("0.0.0.0", UDP_PORT)
FORMAT     = "utf-8"
CRLF       = "\r\n"
VERSION    = "CSC3002F Networks Assignment/1.0"

# ============================================================================
# Protocol Constants  (message method names matching the spec)
# ============================================================================

# Command Messages - session lifecycle and group membership
REGISTER     = "REGISTER"
LOGIN        = "LOGIN"
LOGOUT       = "LOGOUT"
CREATE_GROUP = "CREATE_GROUP"
JOIN_GROUP   = "JOIN_GROUP"
LEAVE_GROUP  = "LEAVE_GROUP"
LIST_USERS   = "LIST_USERS"
LIST_GROUPS  = "LIST_GROUPS"

# Control Messages - acknowledgements, errors, coordination
ACK          = "ACK"
ERROR        = "ERROR"
PEER_INFO    = "PEER_INFO"    # Broker P2P: return peer's IP + TCP/UDP port
PING         = "PING"
PONG         = "PONG"
HEARTBEAT    = "HEARTBEAT"    # UDP presence signal

# Data Messages - chat content
MSG          = "MSG"          # One-to-one text message (relayed via server)
GROUP_MSG    = "GROUP_MSG"    # Group text message
MSG_HISTORY  = "MSG_HISTORY"  # Request message history between two users
MEDIA        = "MEDIA"        # Server-relayed binary file transfer (P2P fallback)
TYPING       = "TYPING"       # UDP typing indicator (sender -> server -> recipient via TCP)
READ         = "READ"         # Read receipt: recipient opened the chat
LEAVE_CHAT   = "LEAVE_CHAT"  # Recipient closed the chat view

# ============================================================================
# Database
# ============================================================================

def init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY,
        password TEXT NOT NULL
    )""")
    con.execute("""CREATE TABLE IF NOT EXISTS messages (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        sender     TEXT NOT NULL,
        recipient  TEXT,
        group_name TEXT,
        text       TEXT NOT NULL,
        timestamp  REAL NOT NULL
    )""")
    con.commit()
    con.close()


def load_users_from_db():
    """Populate the in-memory clients dict from persisted users on startup."""
    con = sqlite3.connect(DB_PATH)
    for username, password in con.execute("SELECT username, password FROM users"):
        clients[username] = {
            "password":       password,
            "connection":     None,
            "address":        ("", 0),
            "visibility":     "Public",
            "status":         "Offline",
            "p2p_tcp_port":   0,
            "last_heartbeat": 0,
        }
    con.close()


def db_save_user(username, password):
    con = sqlite3.connect(DB_PATH)
    con.execute("INSERT INTO users (username, password) VALUES (?, ?)", (username, password))
    con.commit()
    con.close()


def db_save_message(sender, text, timestamp, recipient=None, group_name=None):
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "INSERT INTO messages (sender, recipient, group_name, text, timestamp) VALUES (?, ?, ?, ?, ?)",
        (sender, recipient, group_name, text, timestamp)
    )
    con.commit()
    con.close()


# ============================================================================
# Shared Server State (thread-safe via lock)
# ============================================================================

clients_lock = threading.Lock()
groups_lock  = threading.Lock()

# clients[username] = {
#   "password": str,
#   "connection": socket | None,
#   "address": (ip, port),
#   "visibility": "Public" | "Hidden",
#   "status": "Available" | "Busy",
#   "p2p_tcp_port": int,    # port client listens on for P2P TCP (media/file transfer)
#   "last_heartbeat": float
# }
# NOTE: UDP is only used for Client-Server heartbeats (HEARTBEAT/PING/PONG on
# port 5073). Media and file transfer uses P2P/TCP exclusively.
clients = {}

# groups[group_name] = {"creator": str, "members": set()}
groups = {}

# ============================================================================
# TCP Framing - 4-byte big-endian length prefix
# ============================================================================

def tcp_send(connection, raw_bytes):
    """Prefix message with 4-byte big-endian length and send over TCP."""
    length_prefix = struct.pack(">I", len(raw_bytes))
    connection.sendall(length_prefix + raw_bytes)


def tcp_recv(connection):
    """Read exactly one framed message from a TCP stream.
    Returns None if the connection is closed cleanly.
    """
    raw_len = _recv_exact(connection, 4)
    if raw_len is None:
        return None
    message_length = struct.unpack(">I", raw_len)[0]
    return _recv_exact(connection, message_length)


def _recv_exact(connection, n):
    """Read exactly n bytes from a socket, handling partial reads."""
    buf = b""
    while len(buf) < n:
        chunk = connection.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf

# ============================================================================
# CSC3002F Networks Assignment/1.0 Message Encoding / Decoding
# ============================================================================

def encode_message(method, headers=None, body=b""):
    """Encode a CSC3002F Networks Assignment/1.0 message into bytes ready for TCP framing.

    Wire format:
        METHOD|CSC3002F Networks Assignment/1.0\r\n
        Key:\tValue\r\n
        Content-Length:\t<n>\r\n
        \r\n
        <body bytes>
    """
    if headers is None:
        headers = {}
    if isinstance(body, str):
        body_bytes = body.encode(FORMAT)
    else:
        body_bytes = body

    headers["Content-Length"] = str(len(body_bytes))
    request_line  = f"{method}|{VERSION}{CRLF}"
    header_lines  = "".join(f"{k}:\t{v}{CRLF}" for k, v in headers.items())
    header_block  = (request_line + header_lines + CRLF).encode(FORMAT)
    return header_block + body_bytes


def decode_message(raw):
    """Decode a CSC3002F Networks Assignment/1.0 message from raw bytes.

    Returns:
        {"method": str, "version": str, "headers": dict, "body": bytes}
    """
    separator = (CRLF + CRLF).encode(FORMAT)
    sep_idx = raw.find(separator)
    if sep_idx == -1:
        raise ValueError("Malformed message: missing header/body separator")

    header_block = raw[:sep_idx].decode(FORMAT)
    body         = raw[sep_idx + len(separator):]
    lines        = header_block.split(CRLF)

    if not lines:
        raise ValueError("Malformed message: empty header block")

    # Parse request line: METHOD|VERSION
    request_line = lines[0]
    if "|" not in request_line:
        raise ValueError(f"Malformed request line: {request_line!r}")
    method, version = request_line.split("|", 1)

    # Parse headers
    headers = {}
    for line in lines[1:]:
        if not line:
            continue
        if ":\t" in line:
            key, value = line.split(":\t", 1)
        elif ":" in line:
            key, value = line.split(":", 1)
            value = value.strip()
        else:
            continue
        headers[key] = value

    return {"method": method, "version": version, "headers": headers, "body": body}

# ============================================================================
# High-level send helpers
# ============================================================================

def send_to(connection, method, headers=None, body=b""):
    """Encode and frame-send a CSC3002F Networks Assignment/1.0 message over TCP."""
    raw = encode_message(method, headers or {}, body)
    tcp_send(connection, raw)


def send_ack(connection, seq="0"):
    send_to(connection, ACK, {"Sequence_Num": seq})


def send_error(connection, code, text):
    send_to(connection, ERROR, {"Status": code, "Error-Text": text})

# ============================================================================
# User / group list helpers
# ============================================================================

def build_user_list():
    """Return a JSON array of publicly visible, available users."""
    with clients_lock:
        visible = [
            {
                "username":     u,
                "ip":           data["address"][0],
                "p2p_tcp_port": data["p2p_tcp_port"],
                "status":       data["status"],
            }
            for u, data in clients.items()
            if data["visibility"] == "Public" and data["status"] == "Available"
        ]
    return json.dumps(visible)


def build_group_list():
    """Return a JSON array of existing groups and their member counts."""
    with groups_lock:
        result = [
            {
                "group_name":   g,
                "creator":      data["creator"],
                "member_count": len(data["members"]),
            }
            for g, data in groups.items()
        ]
    return json.dumps(result)

# ============================================================================
# Entry sequence - REGISTER or LOGIN
# ============================================================================

def entry_sequence(connection, address):
    """Handle initial client handshake.

    Loops until the client authenticates successfully or disconnects.
    Returns the authenticated username, or None if the client disconnected.

    Exchange (REGISTER):
        Client -> REGISTER|CSC3002F Networks Assignment/1.0  body: JSON {username, password}
        Server -> ACK  Status: 201
    Exchange (LOGIN):
        Client -> LOGIN|CSC3002F Networks Assignment/1.0  body: JSON {username, password}
        Server -> ACK  Status: 200
        Server -> ERROR  Status: 401/404/409 on failure  (client may retry)
    """
    while True:
        raw = tcp_recv(connection)
        if raw is None:
            return None  # client disconnected

        try:
            msg = decode_message(raw)
        except ValueError as e:
            send_error(connection, "400", f"Bad request: {e}")
            continue

        method = msg["method"]
        try:
            creds    = json.loads(msg["body"].decode(FORMAT))
            username = creds["username"]
            password = creds["password"]
        except (json.JSONDecodeError, KeyError):
            send_error(connection, "400", "Body must be JSON with 'username' and 'password'")
            continue

        with clients_lock:
            if method == REGISTER:
                if username in clients:
                    send_error(connection, "409", "Username already taken")
                    continue
                clients[username] = {
                    "password":       password,
                    "connection":     connection,
                    "address":        address,
                    "visibility":     "Public",
                    "status":         "Available",
                    "p2p_tcp_port":   0,
                    "last_heartbeat": time.time(),
                }
                db_save_user(username, password)
                send_to(connection, ACK, {"Status": "201", "Status-Text": "Account created"})
                print(f"[REGISTER] New user: {username} from {address}")
                return username

            elif method == LOGIN:
                if username not in clients:
                    send_error(connection, "404", "User not found. Use REGISTER first.")
                    continue
                if clients[username]["password"] != password:
                    send_error(connection, "401", "Wrong password")
                    continue
                if clients[username]["connection"] is not None:
                    send_error(connection, "409", "Already logged in from another session")
                    continue
                clients[username]["connection"]     = connection
                clients[username]["address"]        = address
                clients[username]["last_heartbeat"] = time.time()
                send_to(connection, ACK, {"Status": "200", "Status-Text": "Login successful"})
                print(f"[LOGIN] {username} from {address}")
                return username

            else:
                send_error(connection, "400", f"Expected REGISTER or LOGIN, got {method}")
                continue

# ============================================================================
# Per-client TCP message handler
# ============================================================================

def handle_client(connection, address):
    """Main per-client loop - runs in its own thread.

    Lifecycle:
        1. Entry sequence (REGISTER / LOGIN)
        2. Command / data message loop
        3. Cleanup on disconnect
    """
    username = None
    print(f"[CONNECT] New connection from {address}")

    try:
        # 1. Authentication 
        username = entry_sequence(connection, address)
        if username is None:
            return

        #  2. Main command loop 
        while True:
            raw = tcp_recv(connection)
            if raw is None:
                print(f"[DISCONNECT] {username} (connection closed)")
                break

            try:
                msg = decode_message(raw)
            except ValueError as e:
                send_error(connection, "400", str(e))
                continue

            method  = msg["method"]
            headers = msg["headers"]
            body    = msg["body"]

            #Logout
            if method == LOGOUT:
                print(f"[LOGOUT] {username}")
                send_ack(connection)
                break

            # Discovery 
            elif method == LIST_USERS:
                user_list = build_user_list()
                send_to(connection, LIST_USERS,
                        {"Content-Type": "application/json"}, user_list)

            elif method == LIST_GROUPS:
                group_list = build_group_list()
                send_to(connection, LIST_GROUPS,
                        {"Content-Type": "application/json"}, group_list)

            # P2P port registration
            # Client registers its P2P TCP listener port so that peers can look
            # it up via PEER_INFO and connect directly for file/media transfer.
            elif method == "REGISTER_PORTS":
                try:
                    port_data = json.loads(body.decode(FORMAT))
                    with clients_lock:
                        clients[username]["p2p_tcp_port"] = int(port_data.get("p2p_tcp_port", 0))
                    send_ack(connection)
                    print(f"[PORTS] {username}: P2P TCP={clients[username]['p2p_tcp_port']}")
                except (json.JSONDecodeError, KeyError, ValueError):
                    send_error(connection, "400",
                               "REGISTER_PORTS body must be JSON {p2p_tcp_port}")

            # P2P brokering
            # Client A asks for Client B's IP and P2P port so they can connect
            # directly (P2P/TCP for file transfer).
            elif method == PEER_INFO:
                target = headers.get("To", "").strip()
                with clients_lock:
                    if target not in clients or clients[target]["connection"] is None:
                        send_error(connection, "404",
                                   f"User '{target}' not found or offline")
                    else:
                        peer = clients[target]
                        peer_data = {
                            "username":     target,
                            "ip":           peer["address"][0],
                            "p2p_tcp_port": peer["p2p_tcp_port"],
                        }
                        send_to(connection, PEER_INFO,
                                {"Status": "200", "Content-Type": "application/json"},
                                json.dumps(peer_data))

            # Group management

            elif method == CREATE_GROUP:
                try:
                    data       = json.loads(body.decode(FORMAT))
                    group_name = data["group_name"]
                except (json.JSONDecodeError, KeyError):
                    send_error(connection, "400",
                               "CREATE_GROUP body must be JSON {group_name}")
                    continue
                with groups_lock:
                    if group_name in groups:
                        send_error(connection, "409",
                                   f"Group '{group_name}' already exists")
                    else:
                        groups[group_name] = {
                            "creator": username,
                            "members": {username}
                        }
                        send_ack(connection)
                        print(f"[GROUP CREATED] '{group_name}' by {username}")

            elif method == JOIN_GROUP:
                try:
                    data       = json.loads(body.decode(FORMAT))
                    group_name = data["group_name"]
                except (json.JSONDecodeError, KeyError):
                    send_error(connection, "400",
                               "JOIN_GROUP body must be JSON {group_name}")
                    continue
                with groups_lock:
                    if group_name not in groups:
                        send_error(connection, "404",
                                   f"Group '{group_name}' not found")
                    else:
                        groups[group_name]["members"].add(username)
                        send_ack(connection)
                        print(f"[GROUP JOIN] {username} -> '{group_name}'")

            elif method == LEAVE_GROUP:
                try:
                    data       = json.loads(body.decode(FORMAT))
                    group_name = data["group_name"]
                except (json.JSONDecodeError, KeyError):
                    send_error(connection, "400",
                               "LEAVE_GROUP body must be JSON {group_name}")
                    continue
                with groups_lock:
                    if (group_name in groups and
                            username in groups[group_name]["members"]):
                        groups[group_name]["members"].discard(username)
                        send_ack(connection)
                    else:
                        send_error(connection, "404",
                                   "Not a member of that group")

            # One-to-one message relay (Client-Server / TCP) 
            # Design note: for direct text messages the server acts as relay.
            # For large media, clients use P2P TCP (PEER_INFO to get address,
            # then direct socket connection - not handled here).
            elif method == MSG:
                to_user  = headers.get("To", "").strip()
                seq      = headers.get("Sequence_Num", "0")
                msg_body = body.decode(FORMAT)
                with clients_lock:
                    if to_user not in clients or clients[to_user]["connection"] is None:
                        send_error(connection, "404",
                                   f"User '{to_user}' is offline")
                    else:
                        ts = str(time.time())
                        fwd_headers = {
                            "From":         username,
                            "To":           to_user,
                            "Timestamp":    ts,
                            "Sequence_Num": seq,
                        }
                        send_to(clients[to_user]["connection"],
                                MSG, fwd_headers, msg_body)
                        db_save_message(username, msg_body, float(ts), recipient=to_user)
                        send_ack(connection, seq=seq)

            # Group message relay (Client-Server / TCP)
            elif method == GROUP_MSG:
                group_name = headers.get("Group", "").strip()
                seq        = headers.get("Sequence_Num", "0")
                msg_body   = body.decode(FORMAT)
                with groups_lock:
                    if group_name not in groups:
                        send_error(connection, "404",
                                   f"Group '{group_name}' not found")
                        continue
                    members = set(groups[group_name]["members"])

                delivered = 0
                with clients_lock:
                    for member in members:
                        if member == username:
                            continue
                        if member in clients and clients[member]["connection"]:
                            fwd_headers = {
                                "From":      username,
                                "Group":     group_name,
                                "Timestamp": str(time.time()),
                            }
                            try:
                                send_to(clients[member]["connection"],
                                        GROUP_MSG, fwd_headers, msg_body)
                                delivered += 1
                            except OSError:
                                pass  # Member disconnected; cleaned up on their thread
                db_save_message(username, msg_body, time.time(), group_name=group_name)
                send_to(connection, ACK, {"Sequence_Num": seq, "Delivered": str(delivered)})

            # Leave-chat notification: forward so partner clears active status
            elif method == LEAVE_CHAT:
                target = headers.get("To", "").strip()
                with clients_lock:
                    conn = clients[target]["connection"] if target in clients else None
                if conn:
                    try:
                        send_to(conn, LEAVE_CHAT, {"From": username})
                    except OSError:
                        pass

            # Read receipt: forward to the other user so they can show ✓✓
            elif method == READ:
                target = headers.get("To", "").strip()
                with clients_lock:
                    conn = clients[target]["connection"] if target in clients else None
                if conn:
                    try:
                        send_to(conn, READ, {"From": username})
                    except OSError:
                        pass

            # Server-relayed media fallback (when P2P TCP transfer fails)
            elif method == MEDIA:
                to_user = headers.get("To", "").strip()
                with clients_lock:
                    conn = clients[to_user]["connection"] if to_user in clients else None
                if conn:
                    try:
                        fwd_headers = {
                            "From":         username,
                            "Filename":     headers.get("Filename", "file"),
                            "Content-Type": headers.get("Content-Type", "application/octet-stream"),
                        }
                        send_to(conn, MEDIA, fwd_headers, body)
                        send_ack(connection)
                        print(f"[MEDIA RELAY] {username} -> {to_user} ({len(body)} bytes)")
                    except OSError:
                        send_error(connection, "503", "Could not relay media to target")
                else:
                    send_error(connection, "404", f"User '{to_user}' is offline")

            # Message history request
            elif method == MSG_HISTORY:
                target = headers.get("To", "").strip()
                con = sqlite3.connect(DB_PATH)
                rows = con.execute(
                    """SELECT sender, text, timestamp FROM messages
                       WHERE (sender=? AND recipient=?) OR (sender=? AND recipient=?)
                       ORDER BY timestamp""",
                    (username, target, target, username)
                ).fetchall()
                con.close()
                history = [{"sender": r[0], "text": r[1], "timestamp": r[2]} for r in rows]
                send_to(connection, MSG_HISTORY, {"Status": "200"}, json.dumps(history))

            else:
                send_error(connection, "405", f"Unknown method: {method}")

    except (ConnectionResetError, BrokenPipeError, OSError):
        print(f"[DISCONNECT] {username or address} (connection error)")
    finally:
        if username:
            with clients_lock:
                if username in clients:
                    clients[username]["connection"] = None
                    clients[username]["status"]     = "Offline"
        connection.close()
        print(f"[CLEANUP] Connection closed for {username or address}")

# ============================================================================
# UDP Heartbeat Listener (Client-Server / UDP)
#
# Design rationale: presence/status updates do not require reliability.
# Using UDP avoids the overhead of a full TCP connection just for heartbeats
# and tolerates occasional packet loss gracefully .
# ============================================================================

def udp_listener():
    """Listen for UDP datagrams from clients on port 5073.

    UDP preserves datagram boundaries naturally — no length-prefix framing needed.
    Each recvfrom() call returns exactly one complete message.

    Two message types handled:

    HEARTBEAT — periodic presence signal sent by every client every 10 s.
        Reads the From header, updates clients[sender]["last_heartbeat"] to the
        current time, and sets status to "Available".
        Wire format:
            HEARTBEAT|CSC3002F Networks Assignment/1.0\\r\\n
            From:\\t<username>\\r\\n
            Content-Length:\\t0\\r\\n
            \\r\\n

    PING — stateless connectivity check.
        Immediately sends a PONG datagram back to the same address.
        No client lookup or state update required.

    TYPING — forwarded as TCP to the target client's connection (chat presence).
    """
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock.bind(UDP_ADDR)
    print(f"[UDP] Heartbeat listener on {SERVER_IP}:{UDP_PORT}")

    while True:
        try:
            data, addr = udp_sock.recvfrom(4096)
            msg = decode_message(data)

            if msg["method"] == HEARTBEAT:
                sender = msg["headers"].get("From", "").strip()
                with clients_lock:
                    if sender in clients:
                        clients[sender]["last_heartbeat"] = time.time()
                        clients[sender]["status"]         = "Available"

            elif msg["method"] == TYPING:
                sender = msg["headers"].get("From", "").strip()
                target = msg["headers"].get("To",   "").strip()
                with clients_lock:
                    conn = clients[target]["connection"] if target in clients else None
                if conn:
                    try:
                        tcp_send(conn, encode_message(TYPING, {"From": sender}))
                    except OSError:
                        pass

            elif msg["method"] == PING:
                # Stateless connectivity check — reply with PONG, no state update
                sender = msg["headers"].get("From", "")
                pong   = encode_message(PONG, {"To": sender})
                udp_sock.sendto(pong, addr)

        except Exception as e:
            print(f"[UDP ERROR] {e}")

# ============================================================================
# Stale client reaper
# ============================================================================

HEARTBEAT_TIMEOUT = 30  # seconds before marking a client Idle

def heartbeat_reaper():
    """Background thread — marks clients Idle when HEARTBEAT signals stop arriving.

    Wakes every 10 seconds. If a client has an active connection but its
    last_heartbeat timestamp is more than HEARTBEAT_TIMEOUT (30) seconds old,
    its status is set to "Idle". This detects crashed or silently-disconnected
    clients that never sent a formal LOGOUT.

    A crashed client is marked Idle within 10–40 seconds of the crash.
    """
    while True:
        time.sleep(10)
        now = time.time()
        with clients_lock:
            for uname, data in clients.items():
                if data["connection"] is None:
                    continue
                if now - data["last_heartbeat"] > HEARTBEAT_TIMEOUT:
                    data["status"] = "Idle"

# ============================================================================
# TCP Accept Loop
# ============================================================================

def start_tcp_server():
    tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    tcp_sock.bind(TCP_ADDR)
    tcp_sock.listen()
    print(f"[TCP] Server listening on {SERVER_IP}:{TCP_PORT}")

    while True:
        connection, address = tcp_sock.accept()
        thread = threading.Thread(
            target=handle_client,
            args=(connection, address),
            daemon=True
        )
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")

# ============================================================================
# Entry Point
# ============================================================================

def main():
    print("[STARTING] CSC3002F Networks Assignment Server")
    init_db()
    load_users_from_db()
    print(f"[DB] Loaded {len(clients)} registered user(s) from {DB_PATH}")
    threading.Thread(target=udp_listener,    daemon=True).start()
    threading.Thread(target=heartbeat_reaper, daemon=True).start()
    start_tcp_server()


if __name__ == "__main__":
    main()