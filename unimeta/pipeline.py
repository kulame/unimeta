from pymysqlreplication import BinLogStreamReader

class event():
    pass

class Sink():
    
    def publish(self):
        pass

class Source():
    
    def subscribe(self):
        pass


class MysqlSource(Source):
    
    def __init__(self,host,port,user,passwd):
        Source.__init__(self)
        self.settings = {
            "host": host,
            "port": port,
            "user": user,
            "passwd": passwd
        }
        self.stream = BinLogStreamReader(connection_settings = self.settings, 
                                         server_id=100,
                                         blocking=True)

    def subscribe(self):
        for binlogevent in self.stream:
            binlogevent.dump()

    def close(self):
        self.stream.close()


class ClickHouseSink(Sink):
    pass

class Pipeline():
    pass