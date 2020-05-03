import configparser
config = configparser.ConfigParser()
config.read(".env")
from unimeta.pipeline import MysqlSource

if __name__ == "__main__":
    mysql_url = config['mysql'].get('url')
    source = MysqlSource(dababase_url=mysql_url)
    source.subscribe()