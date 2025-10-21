from pjk.common import Config

class OpenSearchClient:

    @classmethod
    def get_client(cls, config: Config):
        aws_auth = config.lookup("os_auth_use_aws", "true") != "false"
        scheme = config.lookup("os_scheme", "https")
        verify_certs = config.lookup("os_verify_certs", "true") != "false"
        ca_certs = config.lookup("os_ca_certs", None)
        region = config.lookup("os_region", None)
        service = config.lookup("os_service", "es")
        username = config.lookup("os_username", None)
        password = config.lookup("os_password", None)
        timeout = float(config.lookup("os_timeout", 30))
        ssl_assert_hostname = config.lookup("os_ssl_assert_hostname", "true") != "false"
        ssl_show_warn = config.lookup("os_ssl_show_warn", "false") == "true"
        host = config.lookup("os_host", None)
        port = config.lookup("os_port", None)

        # Reasonable port defaults
        if port is None:
            port = 443 if scheme == "https" else 9200
        else:
            port = int(port)

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
