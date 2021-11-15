from typhoon.connections import ConnectionParams
from typhoon.contrib.hooks.hook_factory import get_hook
from typhoon.contrib.hooks.hook_interface import HookInterface


class SSHTunnel(HookInterface):
    conn_type = 'ssh_tunnel'

    def __init__(self, conn_params: ConnectionParams):
        print(conn_params.__dict__)
        self.conn_params = conn_params
        self.wrapped_conn_id = self.conn_params.extra['wrapped_conn_id']
        self.remote_ip = self.conn_params.host
        self.remote_port = self.conn_params.port
        self.ssh_username = self.conn_params.login
        self.ssh_pkey = self.conn_params.extra.get('ssh_pkey')
        self.ssh_password = self.conn_params.password
        self.tunnel = None
        self.tunneled_conn = None

    def __enter__(self):
        from sshtunnel import open_tunnel

        wrapped_conn = get_hook(self.wrapped_conn_id)
        self.tunnel = open_tunnel(
            (self.remote_ip, self.remote_port),
            ssh_username=self.ssh_username,
            ssh_pkey=self.ssh_pkey,
            ssh_password=self.ssh_password,
            remote_bind_address=(wrapped_conn.conn_params.host, wrapped_conn.conn_params.port),
        )
        self.tunnel.start()
        self.tunneled_conn = wrapped_conn.__class__.__new__(wrapped_conn.__class__)
        self.tunneled_conn.__init__(ConnectionParams(
            conn_type=wrapped_conn.conn_params.conn_type,
            host=self.tunnel.local_bind_host,
            port=self.tunnel.local_bind_port,
            login=wrapped_conn.conn_params.login,
            password=wrapped_conn.conn_params.password,
            extra=wrapped_conn.conn_params.extra,
        ))
        return self.tunneled_conn.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.tunneled_conn.__exit__(exc_type, exc_val, exc_tb)
        self.tunnel.stop()

    def __getattr__(self, item):
        return self.tunneled_conn.__getattr__(item)
