from extenddbint import *
import bson

class XTDBCursor:
    def __init__(self,cur,db):
        #print 'Initializing cursor object ',cur
        self.cursor = cur
        self.xdb = db
    def next(self):
        st,obj = XTDBCursorNext(self.cursor,None,None)
        #print 'Iterator next ',st,obj
        if st:
            return bson.BSON(obj).decode()
        else:
            #print 'Nothing is there in next'
            raise StopIteration
            return None
    def __iter__(self):
        return self
    def __del__(self):
        if XTDBCursorFree and self.xdb.db:
            XTDBCursorFree(self.cursor)
            self.cursor = None

class XTDB:
    def __init__(self,dbName,dataDir='./'):
        self.db = XTDBInitHandle(dbName,dataDir,None)
    def _checkDB(self):
        if self.db == None:
            raise Exception('DB already closed.')
        
    def insert(self,d):
        self._checkDB();
        if type(d) != type({}):
            raise Exception('Type dictionary is expected as argument.')
        s = bson.BSON().encode(d)
        return XTDBInsert(self.db,s)

    def find(self,d): 
        self._checkDB();
        if type(d) != type({}):
            raise Exception('Type dictionary is expected as argument.')
        s = bson.BSON().encode(d)
        cur = XTDBFind(self.db,s)
        return XTDBCursor(cur,self)

    def remove(self,d):
        self._checkDB();
        if type(d) != type({}):
            raise Exception('Type dictionary is expected as argument.')
        s = bson.BSON().encode(d)
        return XTDBRemove(self.db,s)

    def update(self,query,newValue,upsert=False):
        self._checkDB();
        if type(query) != type({}):
            raise Exception('Type dictionary is expected as argument.')
        s = bson.BSON().encode(query)
        o = bson.BSON().encode(newValue)
        return XTDBUpdate(self.db,s,o,upsert)

    def count(self,query):
        self._checkDB();
        if type(query) != type({}):
            raise Exception('Type dictionary is expected as argument.')
        s = bson.BSON().encode(query)
        return XTDBCount(self.db,s)

    def getlasterror(self):
        self._checkDB();
        obj = XTDBGetLastErrorBson(self.db,None)
        return bson.BSON(obj).decode()

    def sync(self,diskSync=False):
        self._checkDB()
        return XTDBSync(self.db,diskSync);

    def createindex(self,indexName):
        self._checkDB()
        return XTDBCreateIndex(self.db,indexName)

    def close(self):
        self._checkDB();
        XTDBFreeHandle(self.db)
        self.db = None

    def drop(self):
        self._checkDB();
        err = XTDBDrop(self.db)
        self.db = None
        return err

    def dropindex(self,indexName):
        self._checkDB()
        return XTDBDropIndex(self.db,indexName)
