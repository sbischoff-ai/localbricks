import os


LOCALBRICKS_NON_PROXY_HOSTS = ("localhost", "127.0.0.1", "uc-server")


def split_hosts(value):
    return [host.strip() for host in value.split(",") if host.strip()]


hosts = []
for value in (
    os.getenv("NO_PROXY", ""),
    os.getenv("no_proxy", ""),
    os.getenv("LOCALBRICKS_NO_PROXY_EXTRA", ""),
    ",".join(LOCALBRICKS_NON_PROXY_HOSTS),
):
    for host in split_hosts(value):
        if host not in hosts:
            hosts.append(host)

print(",".join(hosts))
