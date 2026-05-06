import os
from urllib.parse import urlsplit


PROXY_ENV = {
    "http": ("http_proxy", "HTTP_PROXY"),
    "https": ("https_proxy", "HTTPS_PROXY"),
}
LOCALBRICKS_NON_PROXY_HOSTS = ("localhost", "127.0.0.1", "uc-server")


def proxy_url(name):
    lower_name, upper_name = PROXY_ENV[name]
    return os.getenv(lower_name) or os.getenv(upper_name) or ""


def parse_proxy(value, default_scheme, default_port):
    if not value:
        return None

    parsed = urlsplit(value if "://" in value else f"{default_scheme}://{value}")
    host = parsed.hostname
    if not host:
        return None

    try:
        port = parsed.port or default_port
    except ValueError:
        return None

    return {
        "host": host,
        "port": port,
        "username": parsed.username,
        "password": parsed.password,
    }


def add_proxy_options(scheme, proxy):
    options.extend(
        [
            f"-D{scheme}.proxyHost={proxy['host']}",
            f"-D{scheme}.proxyPort={proxy['port']}",
        ]
    )
    if proxy["username"]:
        options.append(f"-D{scheme}.proxyUser={proxy['username']}")
    if proxy["password"]:
        options.append(f"-D{scheme}.proxyPassword={proxy['password']}")


def java_non_proxy_hosts():
    value = os.getenv("no_proxy") or os.getenv("NO_PROXY") or ""
    hosts = []
    for item in [*value.split(","), *LOCALBRICKS_NON_PROXY_HOSTS]:
        host = item.strip()
        if not host:
            continue
        if host.startswith("."):
            host = f"*{host}"
        if host in hosts:
            continue
        hosts.append(host)
    return "|".join(hosts)


options = []

http_proxy = parse_proxy(proxy_url("http"), "http", 80)
if http_proxy:
    add_proxy_options("http", http_proxy)

https_proxy = parse_proxy(proxy_url("https"), "https", 443)
if https_proxy:
    add_proxy_options("https", https_proxy)

non_proxy_hosts = java_non_proxy_hosts()
if non_proxy_hosts:
    options.extend(
        [
            f"-Dhttp.nonProxyHosts={non_proxy_hosts}",
            f"-Dhttps.nonProxyHosts={non_proxy_hosts}",
        ]
    )

print(" ".join(options))
