"""
client_final.py - The client used for all Client-Server operations.

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

Usage:
    python client_final.py                  #connects to localhost
   
    python client_final.py <ip_address>     #connects to a specified server IP
            e.g     python client_final.py 192.166.1.10

Note: if "python" doesn't work, try "python3"
"""

import socket      # For TCP and UDP communication via sockets
import threading   # Background threads, Lock
import struct      # 4-byte length prefix encoding for packing and unpacking message lengths in TCP framing
import json        # Body serialisation
import mimetypes   # Detect MIME type from file extension (for P2P transfers)
import os          # File path operations (os.path.isfile, os.path.basename, etc.)
import sys         # sys.argv (command-line args), sys.stdin, sys.stdout
import time        # Timestamps, sleep
import queue       # Thread-safe message passing (history_queue, ack_queue)
import math        # math.ceil for pagination calculations
import select      # Non-blocking I/O (select.select on stdin in raw mode)

#Enable raw terminal mode used in the chat interface
import termios     # Save/restore terminal settings before/after raw mode
import tty         # tty.setraw() enable character-at-a-time terminal input

###CONFIGURATION

FORMAT = "utf-8"    #text encoding string
CRLF = "\r\n"       #line terminator used in message formatting, consistent with HTTP standards.
PROTOCOL_VERSION = "CSC3002F Networks Assignment/1.0"   #protocol versions string embedded in every message's request line.
TCP_PORT = 5072 #Port number on which the server listens for persistent client TCP connections. 
                #Clients connect to this port for all command and message exchanges.
                #5072 chosen as it is not commonly used by other applications and is in the registered port range (1024-49151), minimizing potential conflicts. (Ports 5000 and up recommended by lecturer)
UDP_PORT = 5073 #Port number on which the server listens for UDP datagrams, specifically for presence heartbeats from clients and pings.
P2P_TCP_PORT= 6000  #This client's P2P listener port (file/media receive). This is an initial placeholder. The actual port is overwritten with whatever the OS assigns when binding to port 0.
SERVER_IP = sys.argv[1] if len(sys.argv) > 1 else socket.gethostbyname(socket.gethostname()) #Server IP address to connect to, defaulting to localhost if not provided as a command-line argument.
HEARTBEAT_INTERVAL = 10 #seconds betweend UDP presence signals

###PROTOCOL CONSTANTS (MIRRORS server_final.py)

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

#P2P Methods
MEDIA_META = "MEDIA_META"    # Sender announces file: name, size, type
MEDIA_DATA = "MEDIA_DATA"    # Raw file bytes follow in body

P2P_VERSION= "CSC3002F Networks Assignment-P2P/1.0"

###CLIENT STATE

username = None                 # Set after successful login/register
tcp_socket = None               # Persistent TCP connection to server
udp_socket = None               # UDP socket for heartbeats
running = True                  # False when client is shutting down
print_lock = threading.Lock()   # Prevent interleaved terminal output. Any output to the terminal acquires this lock
current_chat = None             # Username of the open chat, or None. If none, user is at Command Line Interface
history_queue = queue.Queue()   # Delivers MSG_HISTORY responses to the CLI thread
ack_queue = queue.Queue()       # Captures ACKs while in chat mode for active display. Thread-safe FIFO queue.
chat_messages = []              # Full message list for the open chat
chat_page = 0                   # Current page index (0 = oldest)
CHAT_PAGE_SIZE = 10
partner_active = False          # True when chat partner has our chat open (READ Data Message received)
current_chat_is_group = False   # True when current_chat is a group name, not a username
chat_notice = ""                # One-line status shown at bottom of chat (errors, etc.)
_seq_counter = 0
_seq_lock = threading.Lock()    #Makes increments atomic across threads.


def _next_seq():
    """
    Return a unique, ever-increasing sequence number string for MSG/GROUP_MSG.
    Thread-safe increment-and-return sequence.
    """
    global _seq_counter
    with _seq_lock:
        _seq_counter += 1
        return str(_seq_counter)

###TCP FRAMING (4-BYTE BIG-ENDIAN LENGTH PREFIX)
#Almost identical to implementation in server_final.py ("socket" replaces "connection")
def tcp_send(socket, raw_bytes):
    """
    Prefix message with 4-byte big-endian length and send over TCP.
    """
    length_prefix = struct.pack(">I", len(raw_bytes))   #converts an integer n to exactly 4 bytes in big-endian byte order (> = big-endian, I = unsigned 32-bit int). 
    socket.sendall(length_prefix + raw_bytes)       #sends all bytes, retrying internally if the OS only accepts part of the data in one system call 
                                                        #sendall guarantees the entire payload is submitted to the kernel before returning.

def tcp_recv(socket):
    """
    Receive exactly one framed TCP message.
    Returns None if the connection is closed cleanly.
    """
    raw_len = _recv_exact(socket, 4)
    if raw_len is None:
        return None
    n = struct.unpack(">I", raw_len)[0]
    return _recv_exact(socket, n)


def _recv_exact(socket, n):
    """
    Read exactly n bytes from a socket, handling partial reads.
    The loop keeps reading until exactly n bytes have been accumulated. n - len(buf) calculates the remaining bytes needed on each iteration.
    """
    buf = b""
    while len(buf) < n:
        chunk = socket.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf

###MESSAGE ENCODING AND DECODING
def encode_message(method, headers=None, body=b"", version=PROTOCOL_VERSION):
    """
    Encode a CSC3002F Networks Assignment/1.0 (or CSC3002F Networks Assignment-P2P/1.0) message.
    Allows P2P messages to use P2P_VERSION without a separate function. The server's encode_message always uses PROTOCOL_VERSION (hardcoded), since the server only speaks the client-server protocol.

    Message format:
        METHOD|VERSION\r\n
        Key:\tValue\r\n
        Content-Length:\t<n>\r\n
        \r\n
        <body bytes>
    """
    if headers is None:
        headers = {}
    body_bytes = body.encode(FORMAT) if isinstance(body, str) else body
    headers["Content-Length"] = str(len(body_bytes))
    request_line = f"{method}|{version}{CRLF}"
    header_lines = "".join(f"{k}:\t{v}{CRLF}" for k, v in headers.items())
    header_block = (request_line + header_lines + CRLF).encode(FORMAT)
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

###HIGH LEVEL SEND HELPERS (CLIENT TO SERVER OVER TCP)

def send_to_server(method, headers=None, body=b""):
    """
    Encode and send a CSC3002F Networks Assignment/1.0 message to the server over TCP.
    This is the client-side equivalent of "send_to" in "server_final.py".
    Uses module-level tcp_socket.
    """
    raw = encode_message(method, headers or {}, body)
    tcp_send(tcp_socket, raw)


def send_request_recv_response(method, headers=None, body=b""):
    """
    Used only during authetication (before "server_listener" starts).
    Sends a request to the server and blocks the same thread waiting for the server's response until a response arrives.
    Returns the decoded response message dict, or None on error.
    Note: simple request-reply only; inbound MSG/GROUP_MSG are handled by the background listener thread.
    """
    send_to_server(method, headers, body)
    raw = tcp_recv(tcp_socket)
    if raw is None:
        return None
    return decode_message(raw)

###THREAD SAFE PRETTY PRINT

def print_msg(text):
    with print_lock:
        if current_chat:
            return  #Chat view is managed exclusively by render_chat when terminal is in raw mode.
                    #Background messages are silently dropped while in chat view (live messages arrive via chat_messages rather) to avoid corrupting the display.
                    #Outside a chat, the function prints the message on a new line (\n to avoid corrupting the current input), then re-prints the CLI prompt.
                    #Re-printing the prompt is necessary because the print() of the message scrolls the terminal, leaving the prompt behind.

        print(f"\n{text}")
        print(f"[{username}]> ", end="", flush=True)

#==============================================================
###PARADIGM 1: CLIENT-SERVER & TCP
#==============================================================

def do_register(uname, passwd):
    """
    Send REGISTER Command Message and return True on success.
    """
    body = json.dumps({"username": uname, "password": passwd})
    resp = send_request_recv_response(REGISTER, {}, body)
    if resp is None:
        print("No response from server.")
        return False
    if resp["method"] == ACK:
        print(f"[OK] {resp['headers'].get('Status-Text', 'Registered')}")
        return True
    else:
        print(f"[ERROR] {resp['headers'].get('Error-Text', 'Registration failed')}")
        return False


def do_login(uname, passwd):
    """
    Send LOGIN Command Message and return True on success.
    """
    body = json.dumps({"username": uname, "password": passwd})
    resp = send_request_recv_response(LOGIN, {}, body)
    if resp is None:
        print("No response from server.")
        return False
    if resp["method"] == ACK:
        print(f"[OK] {resp['headers'].get('Status-Text', 'Logged in')}")
        return True
    else:
        print(f"[ERROR] {resp['headers'].get('Error-Text', 'Login failed')}")
        return False

def cmd_list_users():
    """
    Request and display the list of online users.
    """
    send_to_server(LIST_USERS, {"From": username})
    # Response arrives on the listener thread and is printed there


def cmd_list_groups():
    """
    Request and display the list of available groups.
    """
    send_to_server(LIST_GROUPS, {"From": username})


def render_chat(to_user, input_buf=""):
    """
    Clear the screen and redraw the entire chat view using \r\n for raw mode.
    """
    total_pages = max(1, math.ceil(len(chat_messages) / CHAT_PAGE_SIZE)) #ensures there is always at least one page even when chat_messages is empty (avoids zero-division in display).
    status = "  • active" if partner_active else ""
    sep = "─" * 40
    lines = [
        sep,
        f"  {to_user}{status}",
        sep,
    ]
    start = chat_page * CHAT_PAGE_SIZE                          
    page_msgs = chat_messages[start:start + CHAT_PAGE_SIZE] #Slices the message list to get only the current page's messages.
    if not chat_messages:
        lines.append("  (no previous messages)")
    for m in page_msgs:
        t = time.strftime("%H:%M:%S", time.localtime(m["timestamp"]))   #Converts a Unix timestamp to local time and formats it as HH:MM:SS.
        who = "you" if m["sender"] == username else m["sender"]         #Your own messages show as "you" for readability.
        lines.append(f"  [{t}] {who}: {m['text']}")
    lines += [
        sep,
        f"  Page {chat_page + 1} of {total_pages}  |  /prev  /next  /back to exit",
        sep,
    ]
    if chat_notice:
        lines.append(f"  {chat_notice}")
    # Build as one write with \r\n so raw mode doesn't misalign lines
    out = "\033[2J\033[H" + "\r\n".join(lines) + f"\r\n[{username} -> {to_user}]> {input_buf}"      #\033[2J is an ANSI escape code that erases the entire screen. \033[H is an ANSI escape code which moves the cursor to row 1, column 1 (top-left).
    sys.stdout.write(out)       #Writes entire output that has been built as one string.
    sys.stdout.flush()          #Forces the Python output buffer to the Os immediately


def update_input_line(to_user, input_buf):
    """
    Overwrite only the input line at the bottom - avoids full redraw on each keypress.
    """
    sys.stdout.write(f"\r\033[K[{username} -> {to_user}]> {input_buf}") #\033[K is ANSI for: erase from cursor to end of line.
    sys.stdout.flush()


def cmd_send_msg(to_user, text):
    """
    Send a one-to-one text message via server relay.
    """
    send_to_server(MSG, {"From": username, "To": to_user, "Sequence_Num": _next_seq()}, text)


def cmd_open_chat(to_user):
    """
    Enter a persistent chat with raw terminal input and 5-second auto-refresh.
    """
    global current_chat, chat_messages, chat_page, partner_active, chat_notice

    #The history request is sent, then the function blocks on history_queue.get(timeout=5).
    #The background server_listener thread will put the response into history_queue when it arrives. 
    #If nothing arrives within 5 seconds, the function aborts.
    send_to_server(MSG_HISTORY, {"From": username, "To": to_user})
    try:
        history = history_queue.get(timeout=5)
    except queue.Empty:
        print("[ERROR] Could not fetch message history.")
        return

    chat_messages = list(history)       #makes a copy (history is the list decoded from JSON)
    chat_page = max(0, math.ceil(len(chat_messages) / CHAT_PAGE_SIZE) - 1)  #Starts at the last page so the user sees the most recent messages first
    partner_active = False
    chat_notice = ""

    #Setting current_chat before sending READ is important: once current_chat is set, the server_listener thread will append incoming messages to chat_messages instead of printing them. 
    #The READ signal notifies the partner that this chat is now open.
    current_chat = to_user

    send_to_server(READ, {"From": username, "To": to_user})

    input_buf = ""
    last_render = 0.0
    REFRESH = 5.0
    
    #Raw terminal mode
    fd = sys.stdin.fileno()
    old_tty = termios.tcgetattr(fd) #saves the current terminal settings so they can be restored later.

    try:
        tty.setraw(fd)  #switches the terminal to raw mode:
        
        #select.select([sys.stdin], [], [], 0.1) waits up to 0.1 seconds for stdin to have data available. 
        #Returns three lists: (readable, writable, exceptional). 
        #If sys.stdin is in the readable list, a character is available. 
        #The 0.1-second timeout allows the loop to check running regularly and trigger the periodic re-render check.
        #sys.stdin.read(1) reads exactly one character.
        while running:
            now = time.time()
            if now - last_render >= REFRESH:
                render_chat(to_user, input_buf)
                last_render = now

            ready, _, _ = select.select([sys.stdin], [], [], 0.1)
            if not ready:
                continue

            ch = sys.stdin.read(1)

            #Enter Key (removes leading/trailing whitespace from typed text)
            if ch in ('\r', '\n'):
                line        = input_buf.strip()
                input_buf   = ""
                chat_notice = ""
                total_pages = max(1, math.ceil(len(chat_messages) / CHAT_PAGE_SIZE))

            #Send message via "cmd_send_message". Block on ack_queue.get(timeout=3) while waiting for server's ACK.
            #If ACK arrives within 3 seconds, add the message to chat_messages locally (server also saves this to database)
            #Update page to show latest message.
            #If ACK does not arrive in 3 seconds, set chat_notice to show an error.
                if line == "/back":
                    break
                elif line == "/prev":
                    if chat_page > 0:
                        chat_page -= 1
                elif line == "/next":
                    if chat_page < total_pages - 1:
                        chat_page += 1
                elif line:
                    cmd_send_msg(to_user, line)
                    try:
                        ack_queue.get(timeout=3)
                        ts = time.time()
                        chat_messages.append({"sender": username, "text": line, "timestamp": ts})
                        chat_page = max(0, math.ceil(len(chat_messages) / CHAT_PAGE_SIZE) - 1)
                    except queue.Empty:
                        chat_notice = "[ERROR] Message not delivered"

                render_chat(to_user, input_buf)
                last_render = time.time()

            elif ch in ('\x7f', '\x08'):        # Backspace
                input_buf = input_buf[:-1]
                update_input_line(to_user, input_buf)

            elif ch == '\x03':                  # Ctrl+C
                break

            elif ch == '\x1b':                  # Escape / arrow keys - consume and ignore
                select.select([sys.stdin], [], [], 0.05)
                if select.select([sys.stdin], [], [], 0)[0]:
                    sys.stdin.read(2)

            elif ch.isprintable():
                input_buf += ch
                update_input_line(to_user, input_buf)

    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_tty) #Cleanup

#After the loop: notify the partner we have left, clear state, then clear the screen.
#\033[2J clears the screen. 
#\033[999;1H moves the cursor to row 999, column 1; the terminal clamps this to the last visible row, so the prompt from run_cli will appear at the bottom of the terminal rather than the top.
#This prevents the prompt from being pushed into the scrollback buffer by subsequent output from background threads.
    send_to_server(LEAVE_CHAT, {"From": username, "To": to_user})
    current_chat   = None
    partner_active = False
    chat_notice    = ""
    sys.stdout.write('\033[2J\033[999;1H')
    sys.stdout.flush()


def cmd_group_msg(group_name, text):
    """
    Send a group message via server relay.
    """
    send_to_server(GROUP_MSG, {"From": username, "Group": group_name, "Sequence_Num": _next_seq()}, text)


def cmd_open_group_chat(group_name):
    """
    Enter a persistent group chat with the same raw terminal interface as 1:1.
    Identical structure to cmd_open_chat with 4 differences:
        Sends GROUP_MSG_HISTORY with a Group header instead of MSG_HISTORY with To.
        Sets current_chat_is_group = True so server_listener routes incoming GROUP_MSG to chat_messages.
	    Uses cmd_group_msg instead of cmd_send_msg when Enter is pressed.
        Does not send READ or LEAVE_CHAT (those are 1:1 concepts - there is no partner_active in group mode).
    display = f"[group] {group_name}" is passed to render_chat and update_input_line so the chat header and prompt clearly indicate this is a group context.
    """
    global current_chat, current_chat_is_group, chat_messages, chat_page, chat_notice

    send_to_server(GROUP_MSG_HISTORY, {"From": username, "Group": group_name})
    try:
        history = history_queue.get(timeout=5)
    except queue.Empty:
        print("[ERROR] Could not fetch group message history.")
        return

    chat_messages = list(history)
    chat_page = max(0, math.ceil(len(chat_messages) / CHAT_PAGE_SIZE) - 1)
    chat_notice = ""
    current_chat = group_name
    current_chat_is_group = True
    display = f"[group] {group_name}"

    input_buf = ""
    last_render = 0.0
    REFRESH = 5.0
    fd = sys.stdin.fileno()
    old_tty = termios.tcgetattr(fd)

    try:
        tty.setraw(fd)
        while running:
            now = time.time()
            if now - last_render >= REFRESH:
                render_chat(display, input_buf)
                last_render = now

            ready, _, _ = select.select([sys.stdin], [], [], 0.1)
            if not ready:
                continue

            ch = sys.stdin.read(1)

            if ch in ('\r', '\n'):
                line        = input_buf.strip()
                input_buf   = ""
                chat_notice = ""
                total_pages = max(1, math.ceil(len(chat_messages) / CHAT_PAGE_SIZE))

                if line == "/back":
                    break
                elif line == "/prev":
                    if chat_page > 0:
                        chat_page -= 1
                elif line == "/next":
                    if chat_page < total_pages - 1:
                        chat_page += 1
                elif line:
                    cmd_group_msg(group_name, line)
                    try:
                        ack_queue.get(timeout=3)
                        ts = time.time()
                        chat_messages.append({"sender": username, "text": line, "timestamp": ts})
                        chat_page = max(0, math.ceil(len(chat_messages) / CHAT_PAGE_SIZE) - 1)
                    except queue.Empty:
                        chat_notice = "[ERROR] Message not delivered"

                render_chat(display, input_buf)
                last_render = time.time()

            elif ch in ('\x7f', '\x08'):        # Backspace
                input_buf = input_buf[:-1]
                update_input_line(display, input_buf)

            elif ch == '\x03':                  # Ctrl+C
                break

            elif ch == '\x1b':                  # Escape / arrow keys — consume and ignore
                select.select([sys.stdin], [], [], 0.05)
                if select.select([sys.stdin], [], [], 0)[0]:
                    sys.stdin.read(2)

            elif ch.isprintable():
                input_buf += ch
                update_input_line(display, input_buf)

    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_tty)

    current_chat          = None
    current_chat_is_group = False
    chat_notice           = ""
    sys.stdout.write('\033[2J\033[999;1H')
    sys.stdout.flush()


def cmd_create_group(group_name):
    """
    Create a new group chat.
    Fire-and-forget: the ACK/ERROR response arrives asynchronously on server_listener and is printed there. 
    Same pattern for cmd_join_group and cmd_leave_group.
    """
    body = json.dumps({"group_name": group_name})
    send_to_server(CREATE_GROUP, {"From": username}, body)


def cmd_join_group(group_name):
    """
    Join an existing group chat.
    """
    body = json.dumps({"group_name": group_name})
    send_to_server(JOIN_GROUP, {"From": username}, body)


def cmd_leave_group(group_name):
    """
    Leave a group chat.
    """
    body = json.dumps({"group_name": group_name})
    send_to_server(LEAVE_GROUP, {"From": username}, body)


def cmd_logout():
    """
    Send LOGOUT and shut down gracefully.
    Sets running = False so all while running: loops exit on their next iteration.
    The server will receive LOGOUT, send ACK, and mark the user offline.
    """
    global running
    send_to_server(LOGOUT, {"From": username})
    running = False


#==============================================================
###PARADIGM 2: CLIENT-SERVER & UDP (HEARTBEAT)
#==============================================================    
def heartbeat_sender():
    """
    Send a UDP HEARTBEAT datagram to the server every HEARTBEAT_INTERVAL seconds.

    The server updates last_heartbeat for this user on receipt.
    No response is expected. Occasional packet loss is acceptable; the next datagram arrives at most HEARTBEAT_INTERVAL seconds later.
    UDP preserves datagram boundaries natively; no length-prefix framing needed.

    Message format:
        HEARTBEAT|CSC3002F Networks Assignment/1.0\\r\\n
        From:\\t<username>\\r\\n
        Content-Length:\\t0\\r\\n
        \\r\\n
    """
    while running:
        try:
            if username:        #Prevents sending before authentication is complete.
                datagram = encode_message(HEARTBEAT, {"From": username})
                udp_socket.sendto(datagram, (SERVER_IP, UDP_PORT))  #sends one UDp datagram to the server's UDP port. No connection is required for UDP
        except OSError:
            pass
        time.sleep(HEARTBEAT_INTERVAL)


#==============================================================
###PARADIGM 3: P2P & TCP (FILE/MEDIA TRANSFER)
#==============================================================

def cmd_send_file(to_user, filepath):
    """
    Send a file directly to another user over P2P TCP.

    Steps:
        1. Ask server for peer's IP + P2P TCP port (PEER_INFO)
        2. Open a direct TCP connection to the peer
        3. Send MEDIA_META (filename, size)
        4. Send MEDIA_DATA (raw file bytes)
    """
    if not os.path.isfile(filepath):
        print_msg(f"[ERROR] File not found: {filepath}")
        return

    # Step 1: Ask server for peer's address
    send_to_server(PEER_INFO, {"From": username, "To": to_user})
    # The response will arrive on the listener thread, which calls
    # _p2p_send_file() once the peer info is received.
    # Store the pending transfer so the listener can complete it.
    pending_transfers[to_user] = filepath
    print_msg(f"[P2P] Requesting peer info for {to_user}...")


def _p2p_send_file(peer_ip, peer_p2p_port, filepath, to_user):
    """
    Open a direct TCP connection to the peer and transfer the file.

    Falls back to server-relayed MEDIA if the P2P connection fails.
    Runs in a dedicated thread so it does not block the UI or server listener.
    """
    filename = os.path.basename(filepath)   #Extracts just the filename from a full path
    total_size = os.path.getsize(filepath)    #Returns file size in bytes without reading the file
    transfer_id = f"{username}-{to_user}-{int(time.time())}"   #Unique identifier for this transger session and helps receiver validate the META_DATA it receives belongs to the MEDIA_META it just ACKed.
    content_type = mimetypes.guess_type(filepath)[0] or "application/octet-stream"  #Infers the MIME type from the file extension (e.g .jpg becomes image/jpeg) and falls back to generic binary (application/octet-stream) if unknown.

    try:
        p2p_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        p2p_sock.connect((peer_ip, peer_p2p_port))

        # MEDIA_META: announce the transfer
        meta = json.dumps({"transfer_id": transfer_id,"filename": filename, "total_size": total_size,"content_type": content_type,})
        raw = encode_message(MEDIA_META, {"From": username}, meta, version=P2P_VERSION)
        tcp_send(p2p_sock, raw)

        # Wait for receiver's ACK before sending data
        ack_raw = tcp_recv(p2p_sock)
        if ack_raw is None:
            raise OSError(f"No ACK from peer for '{filename}'")
        ack = decode_message(ack_raw)
        if ack["method"] != ACK:
            raise OSError(f"Peer rejected: {ack['headers'].get('Error-Text', '')}")

        # MEDIA_DATA: send the raw file bytes
        with open(filepath, "rb") as f:
            file_bytes = f.read()

        raw = encode_message(MEDIA_DATA,{"From": username, "Filename": filename, "Transfer_ID": transfer_id},file_bytes,version=P2P_VERSION,)
        tcp_send(p2p_sock, raw)

        # Wait for final ACK
        done_raw = tcp_recv(p2p_sock)
        if done_raw:
            done = decode_message(done_raw)
            if done["method"] == ACK:
                print_msg(f"[P2P] '{filename}' sent successfully to {peer_ip}:{peer_p2p_port}")
            else:
                print_msg(f"[P2P] Transfer error: {done['headers'].get('Error-Text', '')}")

        p2p_sock.close()

    except (ConnectionRefusedError, OSError) as e:
        print_msg(f"[P2P] Direct transfer failed ({e}). Falling back to server relay...")
        try:
            with open(filepath, "rb") as f:
                file_bytes = f.read()           #Reads entire file into memory.
            send_to_server(MEDIA, {"From": username, "To": to_user, "Filename": filename, "Content-Type": content_type},file_bytes,) #Fallback routes file through server using MEDIA method
            print_msg(f"[MEDIA] '{filename}' sent to {to_user} via server relay.")
        except OSError as relay_err:
            print_msg(f"[MEDIA] Server relay also failed: {relay_err}")


# pending_transfers: maps to_user -> filepath while waiting for PEER_INFO response
pending_transfers = {}

def p2p_receive_listener(listen_sock):
    """
    P2P TCP listener accepts incoming file transfers from peers.

    Runs in a daemon thread. Receives an already-bound socket so that
    the assigned port is known before register_p2p_port() is called.

    Receive protocol (CSC3002F Networks Assignment-P2P/1.0):
        1. Receive MEDIA_META then send ACK
        2. Receive MEDIA_DATA then save file then send ACK
    """
    listen_sock.listen()

    while running:
        try:
            listen_sock.settimeout(1.0) #Makes accept time out after 1 second instead of blocking forever; allows the "while running:" check to run regularly.
            conn, addr = listen_sock.accept()
            t = threading.Thread(target=_handle_p2p_transfer, args=(conn, addr), daemon=True) #Each accepted connection gets its own thread so multiple transfers can occur simultaneously
            t.start()
        except socket.timeout:
            continue
        except OSError:
            break

    listen_sock.close()


def _handle_p2p_transfer(conn, addr):
    """
    Handle one incoming P2P file transfer.
    """
    try:
        # Step 1: receive MEDIA_META
        raw = tcp_recv(conn)
        if raw is None:
            conn.close()
            return
        meta_msg = decode_message(raw)
        if meta_msg["method"] != MEDIA_META:
            conn.close()
            return

        meta = json.loads(meta_msg["body"].decode(FORMAT))
        filename = meta["filename"]
        file_size = meta.get("total_size", meta.get("file_size", 0))
        transfer_id = meta.get("transfer_id", "")
        sender= meta_msg["headers"].get("From", "unknown")

        print_msg(f"[P2P] Incoming file '{filename}' ({file_size} bytes) from {sender}")

        # ACK the meta and signals sender to proceed with data
        raw_ack = encode_message(ACK, {}, version=P2P_VERSION)
        tcp_send(conn, raw_ack)

        # Step 2: receive MEDIA_DATA
        raw = tcp_recv(conn)
        if raw is None:
            conn.close()
            return
        data_msg   = decode_message(raw)
        
        #Validates that the Transger_ID in MEDIA_DATA matches the one announced in MEDIA_META
        recv_tid   = data_msg["headers"].get("Transfer_ID", "")
       
        if transfer_id and recv_tid and recv_tid != transfer_id:
            print_msg(f"[P2P] Transfer_ID mismatch (expected {transfer_id}, got {recv_tid})")
        file_bytes = data_msg["body"]

        # Save to current directory (prefix with sender name to avoid collisions)
        save_path = f"received_{sender}_{filename}"
        with open(save_path, "wb") as f:
            f.write(file_bytes)

        print_msg(f"[P2P] File saved as '{save_path}'")

        # Final ACK
        raw_ack = encode_message(ACK, {}, version=P2P_VERSION)
        tcp_send(conn, raw_ack)

    except Exception as e:
        print_msg(f"[P2P] Transfer error from {addr}: {e}")
    finally:
        conn.close()


###BACKGROUND SERVER-CLIENT MESSAGE LISTENER OVER TCP
    # Runs in a daemon thread. Handles all unsolicited inbound messages
    # (MSG, GROUP_MSG, PEER_INFO responses, ACK, ERROR for async commands).

def server_listener():
    """
    Background daemon thread receives all messages pushed by the server.

    Loops calling tcp_recv() (blocking) and dispatches on method:

        MSG          Print "[HH:MM:SS] sender: text"  (or append to open chat)
        GROUP_MSG    Print "[HH:MM:SS] [group] sender: text"
        ACK          Print delivery confirmation or member count
        ERROR        Print error code and message
        LIST_USERS   Parse JSON body and print formatted user table
        LIST_GROUPS  Parse JSON body and print formatted group table
        PEER_INFO    Parse peer address and trigger pending file transfer
        MEDIA        Save server-relayed binary file to disk

    If tcp_recv() returns None the server has closed the connection.
    Sets running = False so the CLI loop also exits.

    Why a separate thread: the main thread blocks on input(). Without this
    thread incoming messages would only appear after the user presses Enter.
    """
    global running, partner_active
    while running:
        try:
            raw = tcp_recv(tcp_socket)
            if raw is None:
                print_msg("[SERVER] Connection closed by server.")
                running = False
                break

            msg = decode_message(raw)
            method  = msg["method"]
            headers = msg["headers"]
            body = msg["body"]

            #Inbound one-to-one message
            #If the incoming message is from the person whose chat is currently open, append it to chat_messages and render chat will display it on the next 5-second refresh or on Enter.
            #Otherwise, print a notification to the CLI that a message is incoming.
            if method == MSG:
                sender = headers.get("From", "?")
                timestamp = headers.get("Timestamp", "")
                text = body.decode(FORMAT)
                ts = float(timestamp) if timestamp else time.time()
                if current_chat == sender:
                    chat_messages.append({"sender": sender, "text": text, "timestamp": ts})
                else:
                    print_msg(f"Incoming message from {sender} | /msg {sender} to view")

            # Inbound group message
            elif method == GROUP_MSG:
                sender = headers.get("From", "?")
                group_name = headers.get("Group", "?")
                timestamp = headers.get("Timestamp", "")
                text = body.decode(FORMAT)
                ts = float(timestamp) if timestamp else time.time()
                if current_chat_is_group and current_chat == group_name:
                    chat_messages.append({"sender": sender, "text": text, "timestamp": ts})
                else:
                    t_str = time.strftime("%H:%M:%S", time.localtime(ts))
                    print_msg(f"[{t_str}] [{group_name}] {sender}: {text}")

            # ACK (response to async command)
            elif method == ACK:
                if current_chat:
                    ack_queue.put("ok")
                else:
                    info = headers.get("Status-Text", "")
                    delivered = headers.get("Delivered", "")
                    if info:
                        print_msg(f"[OK] {info}")
                    elif delivered:
                        print_msg(f"[OK] Delivered to {delivered} member(s)")
                    else:
                        print_msg("[OK]")

            # Error from server 
            elif method == ERROR:
                code = headers.get("Status", "")
                text = headers.get("Error-Text", "Unknown error")
                print_msg(f"[ERROR {code}] {text}")

            # User list response
            elif method == LIST_USERS:
                try:
                    users = json.loads(body.decode(FORMAT))
                    if not users:
                        print_msg("[USERS] No users currently online.")
                    else:
                        lines = ["[USERS ONLINE]"]
                        for u in users:
                            lines.append(f"  {u['username']} | {u['ip']} | P2P port: {u['p2p_tcp_port']} | {u['status']}")
                        print_msg("\n".join(lines))
                except json.JSONDecodeError:
                    print_msg(f"[USERS] {body.decode(FORMAT)}")

            # Group list response 
            elif method == LIST_GROUPS:
                try:
                    grps = json.loads(body.decode(FORMAT))
                    if not grps:
                        print_msg("[GROUPS] No groups exist yet.")
                    else:
                        lines = ["[GROUPS]"]
                        for g in grps:
                            lines.append(f"  {g['group_name']} | creator: {g['creator']} | {g['member_count']} member(s)")
                        print_msg("\n".join(lines))
                except json.JSONDecodeError:
                    print_msg(f"[GROUPS] {body.decode(FORMAT)}")

            # PEER_INFO response: broker has given us the peer's address
            # Complete any pending file transfer now that we have the peer's details.
            elif method == PEER_INFO:
                status = headers.get("Status", "")
                if status == "200":
                    peer = json.loads(body.decode(FORMAT))
                    peer_user = peer["username"]
                    peer_ip = peer["ip"]
                    peer_port = int(peer["p2p_tcp_port"])

                    if peer_user in pending_transfers:
                        filepath = pending_transfers.pop(peer_user)     #atomically removes and returns the filepath
                        t = threading.Thread(target=_p2p_send_file, args=(peer_ip, peer_port, filepath, peer_user),daemon=True) #p2p_send_file is launched in a new daemon thread so it runs concurrently without blocking server_listener from processing other incoming messages.
                        t.start()
                    else:
                        print_msg(f"[PEER INFO] {peer_user} is at {peer_ip}:{peer_port}")
                else:
                    error_text = headers.get("Error-Text", "Peer not found")
                    print_msg(f"[ERROR] {error_text}")

            elif method == READ:
                sender = headers.get("From", "?")
                if current_chat == sender:
                    partner_active = True

            elif method == LEAVE_CHAT:
                sender = headers.get("From", "?")
                if current_chat == sender:
                    partner_active = False

            #Puts the decoded history list into history_queue.
            elif method in (MSG_HISTORY, GROUP_MSG_HISTORY):
                try:
                    history_queue.put(json.loads(body.decode(FORMAT)))
                except json.JSONDecodeError:
                    history_queue.put([])

            elif method == MEDIA:
                sender   = headers.get("From", "?")
                fname    = headers.get("Filename", "received_file")
                save_path = f"received_{sender}_{fname}"
                with open(save_path, "wb") as f:
                    f.write(body)
                print_msg(f"[MEDIA] File '{fname}' from {sender} saved as '{save_path}'")

            else:
                print_msg(f"[UNHANDLED] method={method}")

        except (ConnectionResetError, BrokenPipeError, OSError):
            if running:
                print_msg("[SERVER] Connection lost.")
            running = False
            break


###CONNECTION SETUP

def connect_to_server():
    """
    Creates the TCP connection and the UDP socket.
    """
    global tcp_socket, udp_socket

    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_socket.connect((SERVER_IP, TCP_PORT))

    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    print(f"[CONNECTED] Server at {SERVER_IP}:{TCP_PORT}")


def register_p2p_port():
    """
    Tell the server which TCP port this client listens on for P2P transfers.

    Called once after login. Other clients can then look up this port via
    PEER_INFO and open a direct TCP connection for file/media transfer.
    """
    body = json.dumps({"p2p_tcp_port": P2P_TCP_PORT})
    send_to_server("REGISTER_PORTS", {"From": username}, body)


###INTERACTIVE COMMAND LINE INTERFACE (CLI)

HELP_TEXT = """
Commands:
  /msg    <user>             Open 1:1 chat with a user (shows history)
  /gchat  <group>            Open group chat (shows history, live messages)
  /gmsg   <group> <text>     Send a one-shot group message (no chat UI)
  /send   <user> <filepath>  Send a file via P2P TCP
  /users                     List online users
  /groups                    List all groups
  /create <group>            Create a group
  /join   <group>            Join a group
  /leave  <group>            Leave a group
  /logout                    Disconnect and exit
  /help                      Show this help
"""


def run_cli():
    """
    Main input loop; reads commands from stdin and dispatches them.
    """
    global running
    print(HELP_TEXT)

    while running:
        try:
            line = input(f"[{username}]> ").strip()
        except (EOFError, KeyboardInterrupt):
            cmd_logout()
            break

        if not line:
            continue

        parts = line.split(None, 2)   # max 3 tokens: command, arg1, rest
        cmd = parts[0].lower()

        if cmd == "/msg":
            if len(parts) < 2:
                print("Usage: /msg <user>")
            else:
                cmd_open_chat(parts[1])

        elif cmd == "/gchat":
            if len(parts) < 2:
                print("Usage: /gchat <group>")
            else:
                cmd_open_group_chat(parts[1])

        elif cmd == "/gmsg":
            if len(parts) < 3:
                print("Usage: /gmsg <group> <text>")
            else:
                cmd_group_msg(parts[1], parts[2])

        elif cmd == "/send":
            if len(parts) < 3:
                print("Usage: /send <user> <filepath>")
            else:
                cmd_send_file(parts[1], parts[2])

        elif cmd == "/users":
            cmd_list_users()

        elif cmd == "/groups":
            cmd_list_groups()

        elif cmd == "/create":
            if len(parts) < 2:
                print("Usage: /create <group>")
            else:
                cmd_create_group(parts[1])

        elif cmd == "/join":
            if len(parts) < 2:
                print("Usage: /join <group>")
            else:
                cmd_join_group(parts[1])

        elif cmd == "/leave":
            if len(parts) < 2:
                print("Usage: /leave <group>")
            else:
                cmd_leave_group(parts[1])

        elif cmd == "/logout":
            cmd_logout()

        elif cmd == "/help":
            print(HELP_TEXT)

        else:
            print(f"Unknown command: {cmd}. Type /help for a list of commands.")

###ENTRY POINT

def main():
    global username, running

    # Step 1: Connect TCP + open UDP socket
    try:
        connect_to_server()
    except ConnectionRefusedError:
        print(f"[ERROR] Could not connect to server at {SERVER_IP}:{TCP_PORT}")
        return

    #  Step 2: Authenticate 
    while True:
        print("\n  1) Register new account")
        print("  2) Login to existing account")
        choice = input("Choice: ").strip()

        uname  = input("Username: ").strip()
        passwd = input("Password: ").strip()

        if choice == "1":
            if do_register(uname, passwd):
                username = uname
                break
        elif choice == "2":
            if do_login(uname, passwd):
                username = uname
                break
        else:
            print("Please enter 1 or 2.")

    # Step 3: Bind P2P socket (port 0 = OS picks a free port)
    global P2P_TCP_PORT
    p2p_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    p2p_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    p2p_sock.bind(("0.0.0.0", 0))
    P2P_TCP_PORT = p2p_sock.getsockname()[1]

    # Step 4: Start P2P TCP listener (Paradigm 3)
    # Must start before registering the port — the listener must be ready
    # to accept connections as soon as other clients learn about this port.
    threading.Thread(target=p2p_receive_listener, args=(p2p_sock,), daemon=True).start()

    # Step 5: Register P2P port with server (TCP)
    # Tells the server "I listen for P2P on port N" so PEER_INFO requests
    # from other clients return the correct port.
    register_p2p_port()

    # Step 6: Start UDP HEARTBEAT sender (Paradigm 2)
    # Client now appears online in LIST_USERS responses.
    threading.Thread(target=heartbeat_sender, daemon=True).start()

    # Step 7: Start TCP server listener (Paradigm 1 inbound)
    # Must start after auth, shares tcp_socket with main thread.
    threading.Thread(target=server_listener, daemon=True).start()

    # Step 8: Run CLI on main thread (blocks until logout)
    run_cli()

    #  Cleanup 
    running = False
    try:
        tcp_socket.close()
        udp_socket.close()
    except OSError:
        pass
    print("\n[DISCONNECTED]")


if __name__ == "__main__":
    main()