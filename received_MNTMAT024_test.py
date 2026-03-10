"""
test.py - Automated Test Suite
================================
CSC3002F Networks Assignment - Stage 2

Tests all server-side functionality by opening raw socket connections
and exercising every protocol method. No client.py dependency.

Usage:
    python test.py                  # test against localhost
    python test.py 192.168.1.10     # test against a specific IP
"""

import socket
import struct
import json
import sys
import time
import threading

# ============================================================================
# Configuration
# ============================================================================

SERVER_IP = "127.0.0.1"
TCP_PORT  = 5072
UDP_PORT  = 5073
FORMAT    = "utf-8"
CRLF      = "\r\n"
VERSION   = "CSC3002F Networks Assignment/1.0"

# ============================================================================
# Protocol helpers  (standalone copies — no import from server/client)
# ============================================================================

def _recv_exact(s, n):
    buf = b""
    while len(buf) < n:
        chunk = s.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf

def tcp_send(s, raw):
    s.sendall(struct.pack(">I", len(raw)) + raw)

def tcp_recv(s):
    raw_len = _recv_exact(s, 4)
    if raw_len is None:
        return None
    return _recv_exact(s, struct.unpack(">I", raw_len)[0])

def encode_message(method, headers=None, body=b""):
    if headers is None:
        headers = {}
    if isinstance(body, str):
        body = body.encode(FORMAT)
    headers["Content-Length"] = str(len(body))
    head  = f"{method}|{VERSION}{CRLF}"
    head += "".join(f"{k}:\t{v}{CRLF}" for k, v in headers.items())
    head += CRLF
    return head.encode(FORMAT) + body

def decode_message(raw):
    sep     = (CRLF + CRLF).encode(FORMAT)
    idx     = raw.find(sep)
    hblock  = raw[:idx].decode(FORMAT)
    body    = raw[idx + len(sep):]
    lines   = hblock.split(CRLF)
    method, version = lines[0].split("|", 1)
    headers = {}
    for line in lines[1:]:
        if not line:
            continue
        if ":\t" in line:
            k, v = line.split(":\t", 1)
        elif ":" in line:
            k, v = line.split(":", 1)
            v = v.strip()
        else:
            continue
        headers[k] = v
    return {"method": method, "version": version, "headers": headers, "body": body}

def send_to(s, method, headers=None, body=b""):
    tcp_send(s, encode_message(method, headers or {}, body))

def recv_msg(s, timeout=5):
    s.settimeout(timeout)
    raw = tcp_recv(s)
    if raw is None:
        raise ConnectionError("Server closed connection")
    return decode_message(raw)

def new_conn():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((SERVER_IP, TCP_PORT))
    return s

# ============================================================================
# Test framework
# ============================================================================

_GREEN  = "\033[92m"
_RED    = "\033[91m"
_YELLOW = "\033[93m"
_RESET  = "\033[0m"

_results = []   # list of bools — one per check()

def check(name, condition, detail=""):
    _results.append(condition)
    tag    = f"{_GREEN}PASS{_RESET}" if condition else f"{_RED}FAIL{_RESET}"
    suffix = f"  ({detail})" if detail else ""
    print(f"  [{tag}] {name}{suffix}")
    return condition

def section(title):
    print(f"\n{_YELLOW}-- {title} --{_RESET}")

# ============================================================================
# Helper: register and return an open, authenticated socket
# ============================================================================

_user_counter = 0
_user_lock    = threading.Lock()

def fresh_user(password="testpass"):
    """Register a unique user and return (socket, username)."""
    global _user_counter
    with _user_lock:
        _user_counter += 1
        uname = f"testuser_{_user_counter}_{int(time.time()*1000) % 100000}"
    s = new_conn()
    send_to(s, "REGISTER", body=json.dumps({"username": uname, "password": password}))
    r = recv_msg(s)
    if r["method"] != "ACK":
        raise RuntimeError(f"fresh_user: REGISTER failed for {uname}: {r}")
    return s, uname

# ============================================================================
# Test suites
# ============================================================================

def test_register_login():
    section("Registration & Login")

    # New registration succeeds
    s = new_conn()
    send_to(s, "REGISTER", body=json.dumps({"username": "reg_alice", "password": "pw"}))
    r = recv_msg(s)
    check("REGISTER new user -> ACK 201",
          r["method"] == "ACK" and r["headers"].get("Status") == "201")
    s.close()

    # Duplicate username rejected
    s = new_conn()
    send_to(s, "REGISTER", body=json.dumps({"username": "reg_alice", "password": "pw"}))
    r = recv_msg(s)
    check("REGISTER duplicate -> ERROR 409",
          r["method"] == "ERROR" and r["headers"].get("Status") == "409")
    s.close()

    # Login with wrong password
    s = new_conn()
    send_to(s, "LOGIN", body=json.dumps({"username": "reg_alice", "password": "wrong"}))
    r = recv_msg(s)
    check("LOGIN wrong password -> ERROR 401",
          r["method"] == "ERROR" and r["headers"].get("Status") == "401")
    s.close()

    # Login unknown user
    s = new_conn()
    send_to(s, "LOGIN", body=json.dumps({"username": "nobody_xyz", "password": "x"}))
    r = recv_msg(s)
    check("LOGIN unknown user -> ERROR 404",
          r["method"] == "ERROR" and r["headers"].get("Status") == "404")
    s.close()

    # Valid login
    s = new_conn()
    send_to(s, "LOGIN", body=json.dumps({"username": "reg_alice", "password": "pw"}))
    r = recv_msg(s)
    check("LOGIN valid credentials -> ACK 200",
          r["method"] == "ACK" and r["headers"].get("Status") == "200")

    # Duplicate login (same user already online)
    s2 = new_conn()
    send_to(s2, "LOGIN", body=json.dumps({"username": "reg_alice", "password": "pw"}))
    r2 = recv_msg(s2)
    check("LOGIN while already online -> ERROR 409",
          r2["method"] == "ERROR" and r2["headers"].get("Status") == "409")
    s2.close()
    s.close()

    # Bad auth method before any auth
    s = new_conn()
    send_to(s, "LIST_USERS")
    r = recv_msg(s)
    check("Send command before auth -> ERROR 400",
          r["method"] == "ERROR" and r["headers"].get("Status") == "400")
    s.close()


def test_list_commands():
    section("LIST_USERS / LIST_GROUPS")

    s, _ = fresh_user()

    send_to(s, "LIST_USERS")
    r = recv_msg(s)
    check("LIST_USERS -> LIST_USERS response", r["method"] == "LIST_USERS")
    try:
        users = json.loads(r["body"].decode(FORMAT))
        check("LIST_USERS body is a JSON array", isinstance(users, list))
        if users:
            u = users[0]
            check("LIST_USERS entry has expected fields",
                  all(k in u for k in ("username", "ip", "p2p_tcp_port", "status")))
    except (json.JSONDecodeError, KeyError):
        check("LIST_USERS body is a JSON array", False)

    send_to(s, "LIST_GROUPS")
    r = recv_msg(s)
    check("LIST_GROUPS -> LIST_GROUPS response", r["method"] == "LIST_GROUPS")
    try:
        groups = json.loads(r["body"].decode(FORMAT))
        check("LIST_GROUPS body is a JSON array", isinstance(groups, list))
    except json.JSONDecodeError:
        check("LIST_GROUPS body is a JSON array", False)

    s.close()


def test_register_ports():
    section("REGISTER_PORTS")

    s, _ = fresh_user()

    send_to(s, "REGISTER_PORTS", body=json.dumps({"p2p_tcp_port": 9876}))
    r = recv_msg(s)
    check("REGISTER_PORTS valid -> ACK", r["method"] == "ACK")

    send_to(s, "REGISTER_PORTS", body=b"not-json")
    r = recv_msg(s)
    check("REGISTER_PORTS bad body -> ERROR 400",
          r["method"] == "ERROR" and r["headers"].get("Status") == "400")

    s.close()


def test_peer_info():
    section("PEER_INFO")

    s1, u1 = fresh_user()
    send_to(s1, "REGISTER_PORTS", body=json.dumps({"p2p_tcp_port": 7777}))
    recv_msg(s1)  # ACK

    s2, _ = fresh_user()

    # Valid lookup
    send_to(s2, "PEER_INFO", {"To": u1})
    r = recv_msg(s2)
    check("PEER_INFO online user -> PEER_INFO response", r["method"] == "PEER_INFO")
    try:
        peer = json.loads(r["body"].decode(FORMAT))
        check("PEER_INFO p2p_tcp_port matches registered value",
              peer.get("p2p_tcp_port") == 7777, f"got {peer.get('p2p_tcp_port')}")
        check("PEER_INFO contains ip and username",
              "ip" in peer and peer.get("username") == u1)
    except (json.JSONDecodeError, KeyError):
        check("PEER_INFO body is valid JSON", False)

    # Unknown / offline user
    send_to(s2, "PEER_INFO", {"To": "ghost_user_zzz"})
    r = recv_msg(s2)
    check("PEER_INFO unknown user -> ERROR 404",
          r["method"] == "ERROR" and r["headers"].get("Status") == "404")

    s1.close()
    s2.close()


def test_groups():
    section("Group Management")

    s, uname = fresh_user()
    group = f"grp_{uname}"

    # Create
    send_to(s, "CREATE_GROUP", body=json.dumps({"group_name": group}))
    r = recv_msg(s)
    check("CREATE_GROUP new group -> ACK", r["method"] == "ACK")

    # Duplicate create
    send_to(s, "CREATE_GROUP", body=json.dumps({"group_name": group}))
    r = recv_msg(s)
    check("CREATE_GROUP duplicate -> ERROR 409",
          r["method"] == "ERROR" and r["headers"].get("Status") == "409")

    # Creator is automatically a member — join again (idempotent add)
    send_to(s, "JOIN_GROUP", body=json.dumps({"group_name": group}))
    r = recv_msg(s)
    check("JOIN_GROUP existing group -> ACK", r["method"] == "ACK")

    # Join nonexistent group
    send_to(s, "JOIN_GROUP", body=json.dumps({"group_name": "does_not_exist_xyz"}))
    r = recv_msg(s)
    check("JOIN_GROUP nonexistent -> ERROR 404",
          r["method"] == "ERROR" and r["headers"].get("Status") == "404")

    # Leave group
    send_to(s, "LEAVE_GROUP", body=json.dumps({"group_name": group}))
    r = recv_msg(s)
    check("LEAVE_GROUP -> ACK", r["method"] == "ACK")

    # Leave when not a member
    send_to(s, "LEAVE_GROUP", body=json.dumps({"group_name": group}))
    r = recv_msg(s)
    check("LEAVE_GROUP when not a member -> ERROR 404",
          r["method"] == "ERROR" and r["headers"].get("Status") == "404")

    # Bad body
    send_to(s, "CREATE_GROUP", body=b"bad")
    r = recv_msg(s)
    check("CREATE_GROUP bad body -> ERROR 400",
          r["method"] == "ERROR" and r["headers"].get("Status") == "400")

    s.close()


def test_direct_messaging():
    section("Direct Messaging (MSG)")

    s1, u1 = fresh_user()
    s2, u2 = fresh_user()

    # s1 -> s2
    send_to(s1, "MSG", {"To": u2}, "Hello from s1!")
    ack = recv_msg(s1)
    check("MSG send -> ACK from server", ack["method"] == "ACK")

    # s2 receives
    try:
        incoming = recv_msg(s2, timeout=3)
        check("MSG received by target user",
              incoming["method"] == "MSG"
              and incoming["headers"].get("From") == u1
              and incoming["body"].decode(FORMAT) == "Hello from s1!")
        check("MSG has Timestamp header", "Timestamp" in incoming["headers"])
    except socket.timeout:
        check("MSG received by target user", False, "timeout")

    # MSG to offline / unknown user
    send_to(s1, "MSG", {"To": "nobody_offline_xyz"}, "hi")
    r = recv_msg(s1)
    check("MSG to offline user -> ERROR 404",
          r["method"] == "ERROR" and r["headers"].get("Status") == "404")

    s1.close()
    s2.close()


def test_group_messaging():
    section("Group Messaging (GROUP_MSG)")

    s1, u1 = fresh_user()
    s2, u2 = fresh_user()
    s3, u3 = fresh_user()
    group  = f"chatroom_{u1}"

    # s1 creates; s2 and s3 join
    send_to(s1, "CREATE_GROUP", body=json.dumps({"group_name": group}))
    recv_msg(s1)
    send_to(s2, "JOIN_GROUP", body=json.dumps({"group_name": group}))
    recv_msg(s2)
    send_to(s3, "JOIN_GROUP", body=json.dumps({"group_name": group}))
    recv_msg(s3)

    # s1 sends to the group
    send_to(s1, "GROUP_MSG", {"Group": group}, "Hey everyone!")
    ack = recv_msg(s1)
    check("GROUP_MSG send -> ACK", ack["method"] == "ACK")
    delivered = int(ack["headers"].get("Seq", "0"))
    check("GROUP_MSG delivered count is 2", delivered == 2, f"Seq={delivered}")

    # s2 and s3 both receive
    for label, sock in [("s2", s2), ("s3", s3)]:
        try:
            incoming = recv_msg(sock, timeout=3)
            check(f"GROUP_MSG received by {label}",
                  incoming["method"] == "GROUP_MSG"
                  and incoming["headers"].get("From") == u1
                  and incoming["headers"].get("Group") == group)
        except socket.timeout:
            check(f"GROUP_MSG received by {label}", False, "timeout")

    # GROUP_MSG to nonexistent group
    send_to(s1, "GROUP_MSG", {"Group": "no_such_group"}, "test")
    r = recv_msg(s1)
    check("GROUP_MSG to nonexistent group -> ERROR 404",
          r["method"] == "ERROR" and r["headers"].get("Status") == "404")

    s1.close()
    s2.close()
    s3.close()


def test_logout():
    section("LOGOUT")

    s, _ = fresh_user()
    send_to(s, "LOGOUT")
    r = recv_msg(s)
    check("LOGOUT -> ACK", r["method"] == "ACK")
    s.close()

    # After logout the user slot should be free for login again
    # (connection is None, so a new LOGIN should succeed)


def test_unknown_method():
    section("Unknown Method")

    s, _ = fresh_user()
    send_to(s, "DOESNOTEXIST")
    r = recv_msg(s)
    check("Unknown method -> ERROR 405",
          r["method"] == "ERROR" and r["headers"].get("Status") == "405")
    s.close()


def test_udp_heartbeat():
    section("UDP Heartbeat & PING/PONG")

    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.settimeout(3)

    # HEARTBEAT (fire and forget — no reply expected)
    pkt = encode_message("HEARTBEAT", {"From": "reg_alice"})
    try:
        udp.sendto(pkt, (SERVER_IP, UDP_PORT))
        check("HEARTBEAT datagram sent successfully", True)
    except OSError as e:
        check("HEARTBEAT datagram sent successfully", False, str(e))

    # PING -> PONG
    ping = encode_message("PING", {"From": "reg_alice"})
    try:
        udp.sendto(ping, (SERVER_IP, UDP_PORT))
        data, _ = udp.recvfrom(4096)
        pong = decode_message(data)
        check("PING -> PONG reply received", pong["method"] == "PONG")
    except socket.timeout:
        check("PING -> PONG reply received", False, "timeout")
    finally:
        udp.close()


# ============================================================================
# Entry point
# ============================================================================

def main():
    global SERVER_IP
    if len(sys.argv) > 1:
        SERVER_IP = sys.argv[1]

    print("=" * 55)
    print("  CSC3002F Networks Assignment — Server Test Suite")
    print(f"  Target: {SERVER_IP}:{TCP_PORT}")
    print("=" * 55)

    # Verify server is reachable before running anything
    try:
        socket.create_connection((SERVER_IP, TCP_PORT), timeout=2).close()
    except (ConnectionRefusedError, OSError):
        print(f"\n[ERROR] Cannot reach server at {SERVER_IP}:{TCP_PORT}")
        print("        Start the server first:  python server.py")
        sys.exit(1)

    test_register_login()
    test_list_commands()
    test_register_ports()
    test_peer_info()
    test_groups()
    test_direct_messaging()
    test_group_messaging()
    test_logout()
    test_unknown_method()
    test_udp_heartbeat()

    passed = sum(_results)
    total  = len(_results)
    print(f"\n{'=' * 55}")
    print(f"  Results: {passed}/{total} passed")
    if passed == total:
        print(f"  {_GREEN}All tests passed.{_RESET}")
    else:
        print(f"  {_RED}{total - passed} test(s) failed.{_RESET}")
    print("=" * 55)
    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
