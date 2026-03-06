"""
client.py - CSC3002F Networks Assignment Client
================================================
CSC3002F Networks Assignment - Stage 2

3 communication paradigms implemented here:

  1. Client-Server / TCP    login, messaging, group chat, discovery
                            (persistent TCP connection to server on port 5072)

  2. Client-Server / UDP    presence heartbeats sent every 10 seconds
                            (UDP datagrams to server on port 5073)

  3. P2P / TCP              direct file/media transfer between peers
                            (client opens a listener; connects directly after
                             obtaining peer address via PEER_INFO from server)


Protocol:   CSC3002F Networks Assignment/1.0  (HTTP-inspired text headers + binary body)
Framing:    4-byte big-endian length prefix on all TCP messages
            UDP preserves message boundaries natively — no framing needed

Usage:
    python client.py                        # connects to localhost
    python client.py 192.168.1.10           # connects to specified server IP
"""

import socket
import threading
import struct
import json
import os
import sys
import time

# ============================================================================
# Configuration
# ============================================================================

SERVER_IP       = sys.argv[1] if len(sys.argv) > 1 else socket.gethostbyname(socket.gethostname())
TCP_PORT        = 5072
UDP_PORT        = 5073
P2P_TCP_PORT    = 6000          # This client's P2P listener port (file/media receive)
FORMAT          = "utf-8"
CRLF            = "\r\n"
VERSION         = "CSC3002F Networks Assignment/1.0"
HEARTBEAT_INTERVAL = 10         # seconds between UDP presence signals
MEDIA_CHUNK_SIZE   = 65536      # 64 KB chunks for P2P file transfer over TCP

# ============================================================================
# Protocol Constants  (must mirror server.py)
# ============================================================================

REGISTER      = "REGISTER"
LOGIN         = "LOGIN"
LOGOUT        = "LOGOUT"
CREATE_GROUP  = "CREATE_GROUP"
JOIN_GROUP    = "JOIN_GROUP"
LEAVE_GROUP   = "LEAVE_GROUP"
LIST_USERS    = "LIST_USERS"
LIST_GROUPS   = "LIST_GROUPS"
ACK           = "ACK"
ERROR         = "ERROR"
PEER_INFO     = "PEER_INFO"
PING          = "PING"
PONG          = "PONG"
HEARTBEAT     = "HEARTBEAT"
MSG           = "MSG"
GROUP_MSG     = "GROUP_MSG"

# P2P-specific methods (CSC3002F Networks Assignment-P2P/1.0)
MEDIA_META    = "MEDIA_META"    # Sender announces file: name, size, type
MEDIA_DATA    = "MEDIA_DATA"    # Raw file bytes follow in body

P2P_VERSION   = "CSC3002F Networks Assignment-P2P/1.0"

# ============================================================================
# Client State
# ============================================================================

username       = None           # Set after successful login/register
tcp_socket     = None           # Persistent TCP connection to server
udp_socket     = None           # UDP socket for heartbeats
running        = True           # False when client is shutting down
print_lock     = threading.Lock()   # Prevent interleaved terminal output

# ============================================================================
# TCP Framing (4-byte big-endian length prefix)
# ============================================================================

def tcp_send(sock, raw_bytes):
    """Send a length-prefixed message over TCP."""
    prefix = struct.pack(">I", len(raw_bytes))
    sock.sendall(prefix + raw_bytes)


def tcp_recv(sock):
    """Receive exactly one framed TCP message.
    Returns None if the connection was closed cleanly.
    """
    raw_len = _recv_exact(sock, 4)
    if raw_len is None:
        return None
    n = struct.unpack(">I", raw_len)[0]
    return _recv_exact(sock, n)


def _recv_exact(sock, n):
    """Read exactly n bytes, handling TCP partial reads."""
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf

# ============================================================================
# CSC3002F Networks Assignment/1.0 Message Encoding / Decoding
# ============================================================================

def encode_message(method, headers=None, body=b"", version=VERSION):
    """Encode a CSC3002F Networks Assignment/1.0 (or CSC3002F Networks Assignment-P2P/1.0) message.

    Wire format:
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
    """Decode a CSC3002F Networks Assignment/1.0 message from raw bytes.

    Returns: {"method": str, "version": str, "headers": dict, "body": bytes}
    """
    separator = (CRLF + CRLF).encode(FORMAT)
    sep_idx = raw.find(separator)
    if sep_idx == -1:
        raise ValueError("Malformed message: missing header/body separator")

    header_block = raw[:sep_idx].decode(FORMAT)
    body         = raw[sep_idx + len(separator):]
    lines        = header_block.split(CRLF)

    request_line = lines[0]
    if "|" not in request_line:
        raise ValueError(f"Malformed request line: {request_line!r}")
    method, version = request_line.split("|", 1)

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
# High-level send helpers (Client → Server over TCP)
# ============================================================================

def send_to_server(method, headers=None, body=b""):
    """Encode and send a CSC3002F Networks Assignment/1.0 message to the server over TCP."""
    raw = encode_message(method, headers or {}, body)
    tcp_send(tcp_socket, raw)


def send_request_recv_response(method, headers=None, body=b""):
    """Send a request to the server and block until a response arrives.

    Returns the decoded response message dict, or None on error.
    Note: simple request-reply only; inbound MSG/GROUP_MSG are handled
    by the background listener thread, not here.
    """
    send_to_server(method, headers, body)
    raw = tcp_recv(tcp_socket)
    if raw is None:
        return None
    return decode_message(raw)

# ============================================================================
# Pretty printer (thread-safe)
# ============================================================================

def print_msg(text):
    with print_lock:
        print(f"\n{text}")
        print(f"[{username}]> ", end="", flush=True)

# ============================================================================
# Paradigm 1: Client-Server / TCP (Authentication)
# ============================================================================

def do_register(uname, passwd):
    """Send REGISTER and return True on success."""
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
    """Send LOGIN and return True on success."""
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

# ============================================================================
# Paradigm 1: Client-Server / TCP (Commands)
# ============================================================================

def cmd_list_users():
    """Request and display the list of online users."""
    send_to_server(LIST_USERS, {"From": username})
    # Response arrives on the listener thread and is printed there


def cmd_list_groups():
    """Request and display the list of available groups."""
    send_to_server(LIST_GROUPS, {"From": username})


def cmd_send_msg(to_user, text):
    """Send a one-to-one text message via server relay."""
    send_to_server(MSG, {"From": username, "To": to_user}, text)


def cmd_group_msg(group_name, text):
    """Send a group message via server relay."""
    send_to_server(GROUP_MSG, {"From": username, "Group": group_name}, text)


def cmd_create_group(group_name):
    """Create a new group chat."""
    body = json.dumps({"group_name": group_name})
    send_to_server(CREATE_GROUP, {"From": username}, body)


def cmd_join_group(group_name):
    """Join an existing group chat."""
    body = json.dumps({"group_name": group_name})
    send_to_server(JOIN_GROUP, {"From": username}, body)


def cmd_leave_group(group_name):
    """Leave a group chat."""
    body = json.dumps({"group_name": group_name})
    send_to_server(LEAVE_GROUP, {"From": username}, body)


def cmd_logout():
    """Send LOGOUT and shut down gracefully."""
    global running
    send_to_server(LOGOUT, {"From": username})
    running = False

# ============================================================================
# Paradigm 3: P2P / TCP - File / Media Transfer
#
# Design rationale: large binary files are sent directly between peers to
# avoid routing media through the server. The server only brokers the
# peer's IP and P2P TCP port via PEER_INFO.
#
# Transfer protocol (CSC3002F Networks Assignment-P2P/1.0):
#   Sender connects to receiver's P2P TCP listener, then sends:
#     1. MEDIA_META - filename, file size, MIME type
#     2. MEDIA_DATA - raw file bytes in body
# ============================================================================

def cmd_send_file(to_user, filepath):
    """Send a file directly to another user over P2P TCP.

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


def _p2p_send_file(peer_ip, peer_p2p_port, filepath):
    """Open a direct TCP connection to the peer and transfer the file.

    This runs in a dedicated thread so it does not block the UI or
    the server listener.
    """
    filename  = os.path.basename(filepath)
    file_size = os.path.getsize(filepath)

    try:
        p2p_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        p2p_sock.connect((peer_ip, peer_p2p_port))

        # MEDIA_META: announce the transfer 
        meta = json.dumps({
            "filename":  filename,
            "file_size": file_size,
        })
        raw = encode_message(MEDIA_META, {"From": username}, meta, version=P2P_VERSION)
        tcp_send(p2p_sock, raw)

        # Wait for receiver's ACK before sending data
        ack_raw = tcp_recv(p2p_sock)
        if ack_raw is None:
            print_msg(f"[P2P] No ACK from peer for {filename}.")
            p2p_sock.close()
            return
        ack = decode_message(ack_raw)
        if ack["method"] != ACK:
            print_msg(f"[P2P] Peer rejected transfer: {ack['headers'].get('Error-Text','')}")
            p2p_sock.close()
            return

        # MEDIA_DATA: send the raw file bytes 
        with open(filepath, "rb") as f:
            file_bytes = f.read()

        raw = encode_message(MEDIA_DATA, {"From": username, "Filename": filename}, file_bytes, version=P2P_VERSION)
        tcp_send(p2p_sock, raw)

        # Wait for final ACK
        done_raw = tcp_recv(p2p_sock)
        if done_raw:
            done = decode_message(done_raw)
            if done["method"] == ACK:
                print_msg(f"[P2P] '{filename}' sent successfully to {peer_ip}:{peer_p2p_port}")
            else:
                print_msg(f"[P2P] Transfer error: {done['headers'].get('Error-Text','')}")

        p2p_sock.close()

    except (ConnectionRefusedError, OSError) as e:
        print_msg(f"[P2P] Could not connect to peer: {e}")


# pending_transfers: maps to_user -> filepath while waiting for PEER_INFO response
pending_transfers = {}


def p2p_receive_listener(listen_sock):
    """P2P TCP listener — accepts incoming file transfers from peers.

    Runs in a daemon thread. Receives an already-bound socket so that
    the assigned port is known before register_p2p_port() is called.

    Receive protocol (CSC3002F Networks Assignment-P2P/1.0):
        1. Receive MEDIA_META  → send ACK
        2. Receive MEDIA_DATA  → save file → send ACK
    """
    listen_sock.listen()

    while running:
        try:
            listen_sock.settimeout(1.0)
            conn, addr = listen_sock.accept()
            t = threading.Thread(target=_handle_p2p_transfer, args=(conn, addr), daemon=True)
            t.start()
        except socket.timeout:
            continue
        except OSError:
            break

    listen_sock.close()


def _handle_p2p_transfer(conn, addr):
    """Handle one incoming P2P file transfer."""
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

        meta      = json.loads(meta_msg["body"].decode(FORMAT))
        filename  = meta["filename"]
        file_size = meta["file_size"]
        sender    = meta_msg["headers"].get("From", "unknown")

        print_msg(f"[P2P] Incoming file '{filename}' ({file_size} bytes) from {sender}")

        # ACK the meta — signals sender to proceed with data
        raw_ack = encode_message(ACK, {}, version=P2P_VERSION)
        tcp_send(conn, raw_ack)

        # Step 2: receive MEDIA_DATA
        raw = tcp_recv(conn)
        if raw is None:
            conn.close()
            return
        data_msg  = decode_message(raw)
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

# ============================================================================
# Paradigm 2: Client-Server / UDP - Heartbeat
#
# Design rationale: the server needs to know which clients are still alive.
# Using UDP for this avoids the cost of a full TCP connection for what is
# essentially a periodic one-way signal. Occasional packet loss is acceptable
# as the server marks clients Idle only after 30 seconds of silence.
# ============================================================================

def heartbeat_sender():
    """Send a UDP HEARTBEAT datagram to the server every HEARTBEAT_INTERVAL seconds.

    UDP preserves datagram boundaries natively, so no length-prefix framing
    is applied here; the datagram is sent as-is using encode_message().
    """
    while running:
        try:
            if username:
                datagram = encode_message(HEARTBEAT, {"From": username})
                udp_socket.sendto(datagram, (SERVER_IP, UDP_PORT))
        except OSError:
            pass
        time.sleep(HEARTBEAT_INTERVAL)

# ============================================================================
# Background: Server to Client message listener (TCP)
#
# Runs in a daemon thread. Handles all unsolicited inbound messages
# (MSG, GROUP_MSG, PEER_INFO responses, ACK, ERROR for async commands).
# ============================================================================

def server_listener():
    """Background thread — receives all messages from the server.

    The server pushes MSG and GROUP_MSG directly to this client without
    the client polling. This thread prints them as they arrive.
    """
    global running
    while running:
        try:
            raw = tcp_recv(tcp_socket)
            if raw is None:
                print_msg("[SERVER] Connection closed by server.")
                running = False
                break

            msg     = decode_message(raw)
            method  = msg["method"]
            headers = msg["headers"]
            body    = msg["body"]

            # Inbound one-to-one message
            if method == MSG:
                sender    = headers.get("From", "?")
                timestamp = headers.get("Timestamp", "")
                text      = body.decode(FORMAT)
                t_str     = time.strftime("%H:%M:%S", time.localtime(float(timestamp))) if timestamp else ""
                print_msg(f"[{t_str}] {sender}: {text}")

            # Inbound group message 
            elif method == GROUP_MSG:
                sender     = headers.get("From", "?")
                group_name = headers.get("Group", "?")
                timestamp  = headers.get("Timestamp", "")
                text       = body.decode(FORMAT)
                t_str      = time.strftime("%H:%M:%S", time.localtime(float(timestamp))) if timestamp else ""
                print_msg(f"[{t_str}] [{group_name}] {sender}: {text}")

            # ACK (response to async command) 
            elif method == ACK:
                seq  = headers.get("Seq", "")
                info = headers.get("Status-Text", "")
                if info:
                    print_msg(f"[OK] {info}")
                elif seq:
                    print_msg(f"[OK] Delivered to {seq} member(s)")
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
                    peer      = json.loads(body.decode(FORMAT))
                    peer_user = peer["username"]
                    peer_ip   = peer["ip"]
                    peer_port = int(peer["p2p_tcp_port"])

                    if peer_user in pending_transfers:
                        filepath = pending_transfers.pop(peer_user)
                        t = threading.Thread(
                            target=_p2p_send_file,
                            args=(peer_ip, peer_port, filepath),
                            daemon=True
                        )
                        t.start()
                    else:
                        print_msg(f"[PEER INFO] {peer_user} is at {peer_ip}:{peer_port}")
                else:
                    error_text = headers.get("Error-Text", "Peer not found")
                    print_msg(f"[ERROR] {error_text}")

            else:
                print_msg(f"[UNHANDLED] method={method}")

        except (ConnectionResetError, BrokenPipeError, OSError):
            if running:
                print_msg("[SERVER] Connection lost.")
            running = False
            break

# ============================================================================
# Connection Setup
# ============================================================================

def connect_to_server():
    """Open the TCP connection and the UDP socket."""
    global tcp_socket, udp_socket

    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_socket.connect((SERVER_IP, TCP_PORT))

    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    print(f"[CONNECTED] Server at {SERVER_IP}:{TCP_PORT}")


def register_p2p_port():
    """Tell the server which port this client listens on for P2P TCP transfers."""
    body = json.dumps({"p2p_tcp_port": P2P_TCP_PORT})
    send_to_server("REGISTER_PORTS", {"From": username}, body)

# ============================================================================
# Interactive Command-Line Interface
# ============================================================================

HELP_TEXT = """
Commands:
  /msg   <user> <text>       Send a direct message
  /gmsg  <group> <text>      Send a group message
  /send  <user> <filepath>   Send a file via P2P TCP
  /users                     List online users
  /groups                    List all groups
  /create <group>            Create a group
  /join   <group>            Join a group
  /leave  <group>            Leave a group
  /logout                    Disconnect and exit
  /help                      Show this help
"""


def run_cli():
    """Main input loop; reads commands from stdin and dispatches them."""
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
        cmd   = parts[0].lower()

        if cmd == "/msg":
            if len(parts) < 3:
                print("Usage: /msg <user> <text>")
            else:
                cmd_send_msg(parts[1], parts[2])

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

# ============================================================================
# Entry Point
# ============================================================================

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
    threading.Thread(target=p2p_receive_listener, args=(p2p_sock,), daemon=True).start()

    # Step 5: Register P2P port with server 
    register_p2p_port()

    #  Step 6: Start UDP heartbeat sender (Paradigm 2) 
    threading.Thread(target=heartbeat_sender, daemon=True).start()

    # Step 7: Start TCP server listener (Paradigm 1 inbound) 
    threading.Thread(target=server_listener, daemon=True).start()

    # Step 8: Run CLI on main thread 
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