from extenddb import *

def DBDump(x):
    print 'DB dump starting '
    c= x.find({})
    for d in c:
        print d
    print 'DB dump end'

x = XTDB('a')
x.insert({'a':1})
x.insert({'a':2})
c=x.find({})
print 'After insert'
for a in c:
    print a

x.remove({'a':1})
c=x.find({})
print 'After remove'
for a in c:
    print a

print 'Update result ', x.update({'a':2},{'a':100})
print x.getlasterror()
print 'After update'
c=x.find({})
for a in c:
    print a

print x.count({'a':100})
print x.count({'a':1})

x.createindex('a')
x.dropindex('a')
x.sync()
x.drop()

x = XTDB('a')
x.insert({'a':[],'b':1})
DBDump(x)

x.update({'b':1},{'$arrSet':{'a':{'query':{".s":1} , 'value':{"$set":{"":2}},'upsert':True } }})
print x.getlasterror()
DBDump(x)
x.update({'b':1},{'$pop':{'a':0}})
DBDump(x)
print x.getlasterror()
x.update({'b':1},{'$push':{'a':1}})
print x.getlasterror()
DBDump(x)
