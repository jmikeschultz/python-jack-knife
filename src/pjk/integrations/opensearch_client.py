from pjk.usage import Usage

 # name, type, default
OS_CONFIG_TUPLES = [
    ("default_index", str, None),
    ("os_auth_use_aws", bool, "true"),
    ("os_scheme", str, "https"),
    ("os_verify_certs", bool, "true"),
    ("os_ca_certs", str, None),
    ("os_region", str, None),
    ("os_service", str, "es"),
    ("os_username", str, None),
    ("os_password", str, None),
    ("os_timeout", float, 30),
    ("os_ssl_assert_hostname", bool, "true"),
    ("os_ssl_show_warn", bool, "false"),
    ("os_host", str, None),
    ("os_port", int, None)
]

class OpenSearchClient:
    @classmethod
    def get_client(cls, u: Usage):
        aws_auth = u.get_config("os_auth_use_aws")
        scheme = u.get_config("os_scheme")
        verify_certs = u.get_config("os_verify_certs")
        ca_certs = u.get_config("os_ca_certs")
        region = u.get_config("os_region")
        service = u.get_config("os_service")
        username = u.get_config("os_username")
        password = u.get_config("os_password")
        timeout = u.get_config("os_timeout")
        ssl_assert_hostname = u.get_config("os_ssl_assert_hostname")
        ssl_show_warn = u.get_config("os_ssl_show_warn")
        host = u.get_config("os_host")
        port = u.get_config("os_port")

        # Reasonable port defaults
        if port is None:
            port = 443 if scheme == "https" else 9200

        if host is None:
            raise ValueError("Config os_host is required (set os_host + os_port/os_scheme, or a connection profile).")

        # Lazy import so this module can still be imported if deps aren't installed.
        try:
            from opensearchpy import OpenSearch, RequestsHttpConnection, Urllib3HttpConnection
        except Exception as e:
            raise RuntimeError("opensearch-py must be installed to use OpenSearchQueryPipe") from e

        http_auth = None
        connection_class = Urllib3HttpConnection  # default
        use_ssl = (scheme == "https")

        if aws_auth:
            # AWS SigV4 (works for OpenSearch Service / legacy ES domains)
            try:
                import boto3
                from requests_aws4auth import AWS4Auth
            except Exception as e:
                raise RuntimeError("boto3 and requests-aws4auth are required for os_auth_method='aws'") from e

            if not region:
                raise ValueError("Config os_region is required for os_auth_method='aws'.")

            session = boto3.Session()
            credentials = session.get_credentials()
            if credentials is None:
                raise RuntimeError("No AWS credentials found (boto3 session.get_credentials() returned None).")

            creds = credentials.get_frozen_credentials()
            http_auth = AWS4Auth(creds.access_key, creds.secret_key, region, service, session_token=creds.token)
            connection_class = RequestsHttpConnection  # SigV4 signing via requests path

        else:
            if not (username and password):
                raise ValueError("os_username and os_password are required for os_auth_method='basic'.")
            http_auth = (username, password)

        # Build client
        client = OpenSearch(
            hosts=[{"host": host, "port": port}],
            http_auth=http_auth,
            use_ssl=use_ssl,
            verify_certs=verify_certs,
            ssl_assert_hostname=ssl_assert_hostname,
            ssl_show_warn=ssl_show_warn,
            ca_certs=ca_certs,
            timeout=timeout,
            connection_class=connection_class,
        )

        return client
