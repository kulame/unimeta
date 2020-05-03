import configparser
config = configparser.ConfigParser()
config.read(".env")
from unimeta.pipeline import MysqlSource

if __name__ == "__main__":
    mysql = config['mysql']
    db = mysql.get('db')
    host = mysql.get('host')
    port = mysql.getint('port')
    user = mysql.get('user')
    passwd = mysql.get('password')
    source = MysqlSource(host=host,port=port,user=user,passwd=passwd)
    source.subscribe()