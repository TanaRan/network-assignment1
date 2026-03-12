"""
server_final.py - The server used for all Client-Server operations.

Consists of 4 communication paradigms:
1. Client-Server & TCP -    The server listens for incoming TCP connections from clients. 
                            Each client connection is handled in a separate thread, allowing for concurrent communication. 
                            The server can receive messages from clients and send responses back to them. Uses port 5072.
2. Client-Server & UDP -    The server listens for incoming UDP datagrams from clients. 
                            Presence heartbeats received from clients every 10 seconds.
                            Uses port 5073.
3. P2P & TCP -              The server can also act as a broker in a P2P network, facilitating direct TCP connections between clients via PEER_INFO.
                            Files and media can be shared directly between clients without routing through the server.
4. P2P & UDP -              Not used in this implementation.

Protocol Description:
CSC3002F Networks Assignment/1.0 (HTTP inspired protocol utilising text headers and binary body)

Message Format:
    METHOD|CSC3002F Networks Assignment/1.0\r\n
    Header-Key:\tHeader-Value\r\n
    Content-Length:\t<n>\r\n
    \r\n
    <body bytes>

TCP Framing (solves the byte-stream boundary problem):
    [4-byte big-endian uint32 length][message bytes...]
    Receiver reads 4 bytes then learns N then reads exactly N bytes.
    UDP doesn't require framing as each datagram is a discrete message (each recvfrom() call returns one complete datagram).

Design Rationale:
    - TCP for all coordination: guarantees delivery and ordering
    - UDP for heartbeats: lightweight, loss acceptable every 10 s
    - Length-prefix framing: avoids delimiter-escaping and partial-read issues
    - Threaded client handling: concurrent clients without blocking the accept loop

"""

import socket       # For TCP and UDP communication via sockets
import threading    # For handling multiple clients concurrently
import json         # For message body serialization and deserialization
import struct       # For packing and unpacking message lengths in TCP framing
import time         # For handling heartbeats and timeouts and message timestamps
import sqlite3     # Embedded SQL database for database operations to persist users, groups, and messages  

### SERVER CONFIGURATION
FORMAT = "utf-8"    #text encoding string
CRLF = "\r\n"       #line terminator used in message formatting, consistent with HTTP standards.
PROTOCOL_VERSION = "CSC3002F Networks Assignment/1.0"   #protocol versions string embedded in every message's request line.
TCP_PORT = 5072 #Port number on which the server listens for persistent client TCP connections. 
                #Clients connect to this port for all command and message exchanges.
                #5072 chosen as it is not commonly used by other applications and is in the registered port range (1024-49151), minimizing potential conflicts. (Ports 5000 and up recommended by lecturer)
TCP_ADDRESS = ("0.0.0.0", TCP_PORT) #0.0.0.0 means bind to all available network interfaces, allowing clients to connect from any IP address that can reach the server.
UDP_PORT = 5073 #Port number on which the server listens for UDP datagrams, specifically for presence heartbeats from clients and pings.
UDP_ADDRESS = ("0.0.0.0", UDP_PORT) #0.0.0.0 means bind to all available network interfaces, allowing clients to connect from any IP address that can reach the server.
SERVER_IP = socket.gethostbyname(socket.gethostname())  #resolves the machine's hostname to its primary IP address at startup. 
                                                        #Used in log messages so the operator knows which address clients should connect to. 
                                                        #Note that this may not always give the correct IP if the machine has multiple interfaces or is behind NAT, but is sufficient for local testing and demonstration purposes.

DB_PATH = "chat.db" #filename of the sqlite3 database file used to persist user accounts, group information, and message history.

###PROTOCOL CONSTANTS

#Command Messages
REGISTER = "REGISTER"
LOGIN = "LOGIN"
LOGOUT = "LOGOUT"
CREATE_GROUP = "CREATE_GROUP"
JOIN_GROUP = "JOIN_GROUP"
LEAVE_GROUP = "LEAVE_GROUP"
LIST_USERS = "LIST_USERS"
LIST_GROUPS  = "LIST_GROUPS"

#Control Messages
PEER_INFO = "PEER_INFO"
HEARTBEAT = "HEARTBEAT"
ACK = "ACK"
ERROR = "ERROR"
PING = "PING"
PONG = "PONG"

#Data Messages
MSG = "MSG"
MSG_HISTORY = "MSG_HISTORY"
GROUP_MSG = "GROUP_MSG"
GROUP_MSG_HISTORY = "GROUP_MSG_HISTORY"
READ = "READ"
MEDIA = "MEDIA"
LEAVE_CHAT = "LEAVE_CHAT"

###SHARED SERVER STATE
#============================================================================
clients = {} #In-memory dictionary to track connected clients and their information. Keyed by username, values are dicts with connection, address, visibility, status, p2p_tcp_port, last_heartbeat.

# clients[username] = {
#   "password": str,
#   "connection": socket | None,
#   "address": (ip, port),
#   "visibility": "Public" | "Hidden",
#   "status": "Available" | "Busy",
#   "p2p_tcp_port": int,    # port client listens on for P2P TCP (media/file transfer)
#   "last_heartbeat": float
# }

#   password:       stored for login validation.
#   connection:     the active socket object, or None when offline.
#   address:        (ip_str, port_int) tuple from accept().
#   visibility:     "Public" or "Hidden". Currently always "Public".
#   status:         "Available", "Offline", or "Idle".
#   p2p_tcp_port:   port this client is listening on for direct P2P file transfers.
#   last_heartbeat: Unix timestamp of the last received HEARTBEAT datagram.

clients_lock = threading.Lock()
#creates a mutual exclusion lock. 
# When one thread holds the lock (acquired via with clients_lock:), all other threads that try to acquire it block until the first thread releases it (exits the with block). 
# This prevents data races and ensures that only one thread can read or modify the clients dict at a time, maintaining data integrity in a concurrent environment.
#============================================================================
groups = {} #In-memory dictionary to track groups and their members. Keyed by group_name, values are dicts with creator and members (a set of usernames).
# groups[group_name] = {"creator": str, "members": set()}
groups_lock  = threading.Lock()
#creates a mutual exclusion lock for the groups dict, ensuring thread-safe access and modifications to group information in a concurrent environment.
#============================================================================


###DATABASE CODE

#Opens or creates the sqlite3 database file and initializes the required tables for users, messages, groups, and group memberships.
#username is the primary key for the users table, ensuring uniqueness. Messages have an auto-incrementing integer ID as the primary key.
#id is an auto-incrementing integer used as a stable row identifier.
#sender is the username of the author.
#recipient is set for 1:1 messages; NULL for group messages.
#group_name is set for group messages; NULL for 1:1 messages.
#text is the message body as a UTF-8 string.
#timestamp is Unix epoch as a floating-point number.

def init_db():
    con = sqlite3.connect(DB_PATH)  #returns a Connection object representing the database connection. If the file does not exist, it will be created.
    con.execute("""CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT NOT NULL)""")
    con.execute("""CREATE TABLE IF NOT EXISTS messages (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        sender     TEXT NOT NULL,
        recipient  TEXT,
        group_name TEXT,
        text       TEXT NOT NULL,
        timestamp  REAL NOT NULL
    )""")
    con.execute("""CREATE TABLE IF NOT EXISTS groups (group_name TEXT PRIMARY KEY, creator TEXT NOT NULL)""")
    con.execute("""CREATE TABLE IF NOT EXISTS group_members (group_name TEXT NOT NULL,username TEXT NOT NULL, PRIMARY KEY (group_name, username))""")
    
    #SQLite connections are not thread safe across threads by default, so the server opens and closes a connection for each operation. 
    con.commit()    
    con.close()

#Database Methods

def load_users_from_db():
    """
    Called once at startup to load all persisted users into the in-memory clients dict.
    Each user gets an entry with connection = None and status = "Offline". They are known to the system but not yet connected. 
    When a user logs in, entry_sequence() fills in connection, address, and last_heartbeat.
    """
    con = sqlite3.connect(DB_PATH)
    for username, password in con.execute("SELECT username, password FROM users"):
        clients[username] = {
            "password": password,
            "connection": None,
            "address": ("", 0),
            "visibility": "Public",
            "status": "Offline",
            "p2p_tcp_port": 0,
            "last_heartbeat": 0,
        }
    con.close()

def load_groups_from_db():
    """
    Called once at startup, after load_users_from_db(). For each persisted group, fetches
    the member list and reconstructs the in-memory groups dict. {r[0] for r in member_rows}
    creates a Python set of usernames, which discards duplicates automatically.
    """
    con = sqlite3.connect(DB_PATH)
    for group_name, creator in con.execute("SELECT group_name, creator FROM groups").fetchall():
        member_rows = con.execute(
            "SELECT username FROM group_members WHERE group_name=?", (group_name,)
        ).fetchall()
        groups[group_name] = {
            "creator": creator,
            "members": {r[0] for r in member_rows},
        }
    con.close()

def db_save_user(username, password):
    """
    Called immediately when a user registers.
    The ? placeholders are parameterised queries (username and password respectively); SQLite substitutes the values safely while preventing SQL injection attacks. 
    """
    con = sqlite3.connect(DB_PATH)
    con.execute("INSERT INTO users (username, password) VALUES (?, ?)", (username, password))
    
    con.commit()
    con.close()

def db_save_group(group_name, creator):
    """
    INSERT OR IGNORE silently does nothing if a row with the same primary key already exists.
    This makes the call safe to call multiple times without raising an error.
    """
    con = sqlite3.connect(DB_PATH)
    con.execute("INSERT OR IGNORE INTO groups (group_name, creator) VALUES (?, ?)", (group_name, creator))
   
    con.commit()
    con.close()


def db_add_group_member(group_name, uname):
    """
    INSERT OR IGNORE silently does nothing if a row with the same primary key already exists.
    This makes the call safe to call multiple times without raising an error.
    """
    con = sqlite3.connect(DB_PATH)
    con.execute("INSERT OR IGNORE INTO group_members (group_name, username) VALUES (?, ?)", (group_name, uname))
    
    con.commit()
    con.close()


def db_remove_group_member(group_name, uname):
    """
    Removes one row from the join table. 
    If the row does not exist, the DELETE succeeds with zero rows affected and no error.
    """
    con = sqlite3.connect(DB_PATH)
    con.execute("DELETE FROM group_members WHERE group_name=? AND username=?",(group_name, uname))
    
    con.commit()
    con.close()


def db_save_message(sender, text, timestamp, recipient=None, group_name=None):
    """
    For a 1:1 message: recipient="dave", group_name=None.
    For a group message: recipient=None, group_name="awesome_group".
    """
    con = sqlite3.connect(DB_PATH)
    con.execute("INSERT INTO messages (sender, recipient, group_name, text, timestamp) VALUES (?, ?, ?, ?, ?)", (sender, recipient, group_name, text, timestamp))
    
    con.commit()
    con.close()

###TCP FRAMING: 4-byte big-endian uint32 length prefix followed by message bytes.
#This allows the receiver to know exactly how many bytes to read for each complete message, solving the byte-stream boundary problem inherent in TCP.

def tcp_send(connection, raw_bytes):
    """Prefix message with 4-byte big-endian length and send over TCP."""
    length_prefix = struct.pack(">I", len(raw_bytes))   #converts an integer n to exactly 4 bytes in big-endian byte order (> = big-endian, I = unsigned 32-bit int). 
    connection.sendall(length_prefix + raw_bytes)       #sends all bytes, retrying internally if the OS only accepts part of the data in one system call 
                                                        #sendall guarantees the entire payload is submitted to the kernel before returning.

def tcp_recv(connection):
    """
    Read exactly one framed message from a TCP stream.
    Returns None if the connection is closed cleanly.
    """
    raw_len = _recv_exact(connection, 4)
    if raw_len is None:
        return None
    message_length = struct.unpack(">I", raw_len)[0]
    return _recv_exact(connection, message_length)


def _recv_exact(connection, n):
    """
    Read exactly n bytes from a socket, handling partial reads.
    The loop keeps reading until exactly n bytes have been accumulated. n - len(buf) calculates the remaining bytes needed on each iteration.
    If recv() returns an empty bytes object b"", the connection has been closed and returns 'None' to signal this to the caller.
    """
    buf = b""
    while len(buf) < n:
        chunk = connection.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf

###MESSAGE ENCODING AND DECODING

def encode_message(method, headers=None, body=b""):
    """
    Encode a message into bytes ready for TCP framing.
    
      Message format:
        METHOD|CSC3002F Networks Assignment/1.0\r\n
        Key:\tValue\r\n
        Content-Length:\t<n>\r\n
        \r\n
        <body bytes>
    """

    #The body can be either a string (for text messages, JSON) or raw bytes (for files). 
    # The isinstance check converts strings to bytes; bytes are used as-is.

    if headers is None:
        headers = {}
    if isinstance(body, str):
        body_bytes = body.encode(FORMAT)
    else:
        body_bytes = body

    headers["Content-Length"] = str(len(body_bytes))    #Adds the Content-Length header after encoding the body, so the count is exact.
    request_line  = f"{method}|{PROTOCOL_VERSION}{CRLF}"
    #"".join(...) is a generator expression that builds each Key:\tValue\r\n header line and concatenates them all into one string. 
    #The trailing + CRLF after all headers produces the empty line that separates headers from the body. 
    # The entire header block is encoded to bytes, then concatenated with the raw body bytes.
    header_lines  = "".join(f"{k}:\t{v}{CRLF}" for k, v in headers.items())
    header_block  = (request_line + header_lines + CRLF).encode(FORMAT)
    return header_block + body_bytes


def decode_message(raw):
    """
    Decode a message from raw bytes.
    Returns:
        {"method": str, "version": str, "headers": dict, "body": bytes}
    """
    separator = (CRLF + CRLF).encode(FORMAT) #CRLF + CRLF is "\r\n\r\n", which marks the end of the header block and the start of the body.
    sep_idx = raw.find(separator) #searches separator sequence in the raw bytes and returns its byte offset, or -1 if not found.
    
    if sep_idx == -1:   #separator sequence not found
        raise ValueError("Malformed message: missing header/body separator")

    header_block = raw[:sep_idx].decode(FORMAT)     #Everything before the separator 
    body= raw[sep_idx + len(separator):]   #Everything after the separator.
    lines= header_block.split(CRLF)

    if not lines:
        raise ValueError("Malformed message: empty header block")

    # Parse request line: METHOD|VERSION
    request_line = lines[0]
    if "|" not in request_line:
        raise ValueError(f"Malformed request line: {request_line!r}")
    method, version = request_line.split("|", 1) #Splits the request line into method and version at the first occurrence of "|". The maxsplit=1 argument ensures that if the version string contains a "|", it won't be split further.

    # Parse headers
    headers = {}
    for line in lines[1:]:
        if not line:
            continue
        if ":\t" in line:
            key, value = line.split(":\t", 1)
        elif ":" in line:                       #handles clients that may omit the tab and only use a colon

            key, value = line.split(":", 1)
            value = value.strip()
        else:
            continue
        headers[key] = value

    return {"method": method, "version": version, "headers": headers, "body": body}

###HIGH LEVEL SEND HELPERS

def send_to(connection, method, headers=None, body=b""):
    """
    Encode first and then frame-send a message over TCP.
    headers or {} converts None to an empty dict so encode_message always receives a dict.
    """
    raw = encode_message(method, headers or {}, body)
    tcp_send(connection, raw)


def send_ack(connection, seq="0"):
    """
    Sends a positive acknowledgement with the sequence number echoed back.
    The client matches this to its sent message to confirm delivery.
    """
    send_to(connection, ACK, {"Sequence_Num": seq})


def send_error(connection, code, text):
    """
    Sends an error response code.
    Follows HTTP conventions: 
    "400" bad request 
    "401" unauthorised 
    "404" not found 
    "409" conflict
    "500" server error
    """
    send_to(connection, ERROR, {"Status": code, "Error-Text": text})

###USER AND GROUP LIST HELPERS

def build_user_list():
    """
    Return a JSON array of publicly visible, available users.
    The entire iteration runs while the lock is held, producing a consistent snapshot of the clients dict.
    Only users that are both "Public" and "Available" are included 
    """
    with clients_lock:
        visible = [
            {
                "username": u,
                "ip": data["address"][0],
                "p2p_tcp_port": data["p2p_tcp_port"],
                "status": data["status"],
            }
            for u, data in clients.items()
            if data["visibility"] == "Public" and data["status"] == "Available"
        ]
    return json.dumps(visible) #serialises the list to a JSON string, which will become the message body.


def build_group_list():
    """
    Return a JSON array of existing groups and their member counts.
    """
    with groups_lock:
        result = [
            {
                "group_name": g,
                "creator": data["creator"],
                "member_count": len(data["members"]),
            }
            for g, data in groups.items()
        ]
    return json.dumps(result) #serialises the list to a JSON string, which will become the message body.


###ENTRY SEQUENCE (REGISTER AND LOGIN)

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
    
    #Allows client to retry authentication if they make a mistake, without disconnecting and reconnecting.
    while True:        
        raw = tcp_recv(connection)
        if raw is None:
            return None  # client disconnected

        try:
            msg = decode_message(raw)
        except ValueError as e:
            send_error(connection, "400", f"Bad request: {e}")
            continue

    #Parses the JSON body. 
    #If the JSON is malformed or the keys are missing, an error is sent and the loop continues.
    #The client can retry with a valid request.
        method = msg["method"]  #
        try:
            creds    = json.loads(msg["body"].decode(FORMAT))
            username = creds["username"]
            password = creds["password"]
        except (json.JSONDecodeError, KeyError):
            send_error(connection, "400", "Body must be JSON with 'username' and 'password'")
            continue

#Register Path======================================================================================
#Check-then-set structure within a lock to prevent race conditions.        
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
#Login Path=========================================================================================  
            elif method == LOGIN:
                if username not in clients: #Existence check
                    send_error(connection, "404", "User not found. Use REGISTER first.")
                    continue
                if clients[username]["password"] != password:   #Password check
                    send_error(connection, "401", "Wrong password")
                    continue
                if clients[username]["connection"] is not None: #Already-logged-in check
                    send_error(connection, "409", "Already logged in from another session")
                    continue
                clients[username]["connection"] = connection
                clients[username]["address"] = address
                clients[username]["last_heartbeat"] = time.time()
                send_to(connection, ACK, {"Status": "200", "Status-Text": "Login successful"})
                print(f"[LOGIN] {username} from {address}")
                return username

            else:
                send_error(connection, "400", f"Expected REGISTER or LOGIN, got {method}")
                continue

###TCP MESSAGE HANDLER (PER CLIENT)

def handle_client(connection, address):
    """
    Main per-client loop - runs in its own thread for every connected client.

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

            # P2P port registration.
            # Client registers its P2P TCP listener port so that peers can look it up via PEER_INFO and connect directly for file/media transfer.
            # Client binds a random port (port 0 = OS assigns), then sends this number to the server immediately after login.
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

            # P2P brokering.
            # Client A asks for Client B's IP and P2P port so they can connect directly (P2P/TCP for file transfer).
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
            # The creator is automatically added as the first member.
            # Both the in-memory dict and the database are updated atomically within the lock.
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
                        db_save_group(group_name, username)
                        db_add_group_member(group_name, username)
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
                        db_add_group_member(group_name, username)
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
                        db_remove_group_member(group_name, username)
                        send_ack(connection)
                    else:
                        send_error(connection, "404",
                                   "Not a member of that group")

            # One-to-one message relay (Client-Server / TCP) 
            # Design note: for direct text messages the server acts as relay.
            # For large media, clients use P2P TCP (PEER_INFO to get address, then direct socket connection).
            #The server looks up the recipient's socket in clients, forwards the message with additional headers (From, Timestamp), saves to the database, then ACKs the sender.
            #All three steps happen while clients_lock is held, ensuring the recipient's connection cannot change mid-operation.
            elif method == MSG:
                to_user = headers.get("To", "").strip()
                seq = headers.get("Sequence_Num", "0")
                msg_body = body.decode(FORMAT)
                with clients_lock:
                    if to_user not in clients or clients[to_user]["connection"] is None:
                        send_error(connection, "404",
                                   f"User '{to_user}' is offline")
                    else:
                        ts = str(time.time())
                        fwd_headers = {
                            "From": username,
                            "To": to_user,
                            "Timestamp": ts,
                            "Sequence_Num": seq,
                        }
                        send_to(clients[to_user]["connection"],
                                MSG, fwd_headers, msg_body)
                        db_save_message(username, msg_body, float(ts), recipient=to_user)
                        send_ack(connection, seq=seq)

            # Group message relay (Client-Server / TCP)
            #groups_lock is acquired to snapshot the member set and is then released.
            #clients_lock is acquired to iterate over member connections.
            #Both locks are not held simultaneously to avoid deadlocks, but this means the member list could change between the two steps.
            elif method == GROUP_MSG:
                group_name = headers.get("Group", "").strip()
                seq = headers.get("Sequence_Num", "0")
                msg_body = body.decode(FORMAT)
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
                                "From": username,
                                "Group": group_name,
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
                    except OSError:     #handles the case where the target disconnected between the lookup and the send.
                        pass

            # Read receipt: notify sender that recipient has opened the chat
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
                            "From": username,
                            "Filename": headers.get("Filename", "file"),
                            "Content-Type": headers.get("Content-Type", "application/octet-stream"),
                        }
                        send_to(conn, MEDIA, fwd_headers, body)
                        send_ack(connection)
                        print(f"[MEDIA RELAY] {username} -> {to_user} ({len(body)} bytes)")
                    except OSError:
                        send_error(connection, "503", "Could not relay media to target")
                else:
                    send_error(connection, "404", f"User '{to_user}' is offline")

            # Message history request (1:1)
            # The SQL query selects all messages in either direction between the two users (username and target), ordered chronologically. 
            # fetchall()` returns a list of tuples.

            elif method == MSG_HISTORY:
                target = headers.get("To", "").strip()
                con = sqlite3.connect(DB_PATH)
                rows = con.execute(
                    """SELECT sender, text, timestamp FROM messages
                       WHERE (sender=? AND recipient=?) OR (sender=? AND recipient=?)
                       ORDER BY timestamp""",
                    (username, target, target, username)).fetchall()
                con.close()
                history = [{"sender": r[0], "text": r[1], "timestamp": r[2]} for r in rows]
                send_to(connection, MSG_HISTORY, {"Status": "200"}, json.dumps(history))

            # Group message history request
            # filter by group_name and order by timestamp. The server returns the full history of messages sent to that group, which clients can use to populate the chat when they join.
            elif method == GROUP_MSG_HISTORY:
                group_name = headers.get("Group", "").strip()
                con = sqlite3.connect(DB_PATH)
                rows = con.execute("""SELECT sender, text, timestamp FROM messages WHERE group_name=? ORDER BY timestamp""",(group_name,)).fetchall()
                con.close()
                history = [{"sender": r[0], "text": r[1], "timestamp": r[2]} for r in rows]
                send_to(connection, GROUP_MSG_HISTORY, {"Status": "200"}, json.dumps(history))

            else:
                send_error(connection, "405", f"Unknown method: {method}")

    except (ConnectionResetError, BrokenPipeError, OSError):
        print(f"[DISCONNECT] {username or address} (connection error)")

#Disconnect cleanup
#finally runs regardless of how the try block exits, whether by break (logout), return, or an exception (crash, disconnect). 
#Setting connection = None marks the useras offline in clients without deleting the entry (the account persists).
#connection.close() releases the OS socket handle.
    finally:
        if username:
            with clients_lock:
                if username in clients:
                    clients[username]["connection"] = None
                    clients[username]["status"] = "Offline"
        connection.close() #releases the OS socket handle.
        print(f"[CLEANUP] Connection closed for {username or address}")


###UDP HEARTBEAT LISTENER (CLIENT-SERVER & UDP)

def udp_listener():
    """
    Listen for UDP datagrams from clients on port 5073.

    Two message types handled:

    HEARTBEAT: periodic presence signal sent by every client every 10 s.
        Reads the From header, updates clients[sender]["last_heartbeat"] to the
        current time, and sets status to "Available".
        Message format:
            HEARTBEAT|CSC3002F Networks Assignment/1.0\\r\\n
            From:\\t<username>\\r\\n
            Content-Length:\\t0\\r\\n
            \\r\\n

    PING: stateless connectivity check.
        Immediately sends a PONG datagram back to the same address.
        No client lookup or state update required.
    """
    
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) #SOCK_DGRAM creates a UDP socket. No listen() or accept() is needed because UDP does not require these.
    udp_sock.bind(UDP_ADDRESS) #.bind() claims port 5073.
    print(f"[UDP] Heartbeat listener on {SERVER_IP}:{UDP_PORT}")

    #blocks until a UDP datagram arrives
    #Returns the datagram bytes and the sender's ip, port tuple.
    #4096 bytes is the maximum datagram size this sockket will accept.
    #UDP datagrams are complete messages so no length prefix is needed; decode_message works identically on UDP and TCP 'payloads'.
    while True:
        try:
            data, addr = udp_sock.recvfrom(4096)
            msg = decode_message(data)

#Heartbeat Handler
#updates timestamp and sets status to "Available" so the user appears in the discovery list for other clients.
#the heartbeat reaper (Defined in the "STALE CLIENT REAPER" section) checks this timestamp to detect disconnected clients that never sent the LOGOUT Command Message.
            if msg["method"] == HEARTBEAT:
                sender = msg["headers"].get("From", "").strip()
                with clients_lock:
                    if sender in clients:
                        clients[sender]["last_heartbeat"] = time.time()
                        clients[sender]["status"] = "Available"
#Ping Handler
#No state is read or written, just send a PONG back to the same address.
            elif msg["method"] == PING:
                sender = msg["headers"].get("From", "")
                pong = encode_message(PONG, {"To": sender})
                udp_sock.sendto(pong, addr) #Sends PONG Control Message to specified address.

        except Exception as e:
            print(f"[UDP ERROR] {e}")


###STALE CLIENT REAPER

HEARTBEAT_TIMEOUT = 30  # constant holding the seconds before marking a client Idle.
                        # for a client to be marked idle, 3 consecutive datagrams must be missed.

def heartbeat_reaper():
    """
    Background thread marking clients Idle when HEARTBEAT signals stop arriving.

    Wakes every 10 seconds. If a client has an active connection but its last_heartbeat timestamp is more than HEARTBEAT_TIMEOUT (30) seconds old,
    its status is set to "Idle". This detects crashed or silently-disconnected clients that never sent a formal LOGOUT Command Message.

    A crashed client is marked Idle within 10-40 seconds of the crash.
    """
    while True:
        time.sleep(10)      #suspends the thread for 10 seconds between checks.
        now = time.time()
        with clients_lock:
            for uname, data in clients.items():
                if data["connection"] is None:
                    continue
                if now - data["last_heartbeat"] > HEARTBEAT_TIMEOUT:
                    data["status"] = "Idle"


###TCP ACCEPT LOOP

def start_tcp_server():
    tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    #SO_REUSEADDR allows the server to rebind the same port immediately after restart 
    #without having to wait for the OS's TIME_WAIT period.
    # This allows the server to restart quickly without failing with "Address already in use" error message. 
    tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  
    tcp_sock.bind(TCP_ADDRESS)
    tcp_sock.listen()   # puts the socket in a state where it will accept incoming connections.
    print(f"[TCP] Server listening on {SERVER_IP}:{TCP_PORT}")

    #Blocks until a client connects, then returns the new socket and address.
    #Creates new thread immeaditely for each connection so .accept() is free to handle next client
    while True:
        connection, address = tcp_sock.accept()
        thread = threading.Thread(
            target=handle_client,
            args=(connection, address),
            daemon=True #Daemon threads are "killed" immediately when the main thread completes (main process exits).
        )
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")



### ENTRY POINT

def main():
    print("[STARTING] CSC3002F Networks Assignment Server")
    init_db()
    load_users_from_db()
    load_groups_from_db()
    print(f"[DB] Loaded {len(clients)} user(s) and {len(groups)} group(s) from {DB_PATH}")
    threading.Thread(target=udp_listener, daemon=True).start()
    threading.Thread(target=heartbeat_reaper, daemon=True).start()
    start_tcp_server()


if __name__ == "__main__":
    main()