from urllib import parse

def parse_url(url:str)->dict:
    """Parses a database URL."""

    config = {}

    url = parse.urlparse(url)

    # Remove query strings.
    path = url.path[1:]
    path = path.split('?', 2)[0]

    # Update with environment configuration.
    config.update({
        'name': path,
        'user': url.username,
        'passwd': url.password,
        'host': url.hostname,
        'port': url.port,
        'scheme': url.scheme
    })


    return config