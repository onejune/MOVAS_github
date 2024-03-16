def get_host_ip():
    import socket
    host_name = socket.gethostname()
    host_ip = socket.gethostbyname(host_name)
    return host_ip

def get_available_endpoint():
    import socket
    import random
    host_name = socket.gethostname()
    host_ip = socket.gethostbyname(host_name)
    addr_info = socket.getaddrinfo(host_ip, None)
    ip_family = addr_info[0][0]
    with socket.socket(ip_family, socket.SOCK_STREAM) as sock:
        try:
            sock.bind(('', 0))
            _, port = sock.getsockname()
            return host_ip, port
        except socket.error as e:
            message = "can not find bindable port "
            message += "on host %s(%s)" % (host_name, host_ip)
            raise RuntimeError(message) from e
