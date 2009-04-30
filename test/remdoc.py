#! /usr/bin/env python
import sys
import couchdb
db = couchdb.Database(sys.argv[1])
del db[sys.argv[2]]
