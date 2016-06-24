#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# (Copyleft) 2016 - AWI : Aquitaine Webmédia Indépendant
#
# Copy records from primecoind RPC calls to CouchDB
# 
from __future__ import print_function
version = "0.1.2"
conf_file = "~/.primecoin/primecoin.conf"

debug = False

import sys
import os
from datetime import datetime
from optparse import OptionParser
#from optparse import TitledHelpFormatter
from optparse import IndentedHelpFormatter
try:
    import bitcoin.rpc
    jrpc = False
except:
    from jsonrpc import ServiceProxy
    jrpc = True
from couchdb import Server
import json
import requests

def cdb_connect(couchdb, name):
    try:
        cdb = couchdb[name]
    except:
        print("Creating new database : %s" % name)
        cdb = couchdb.create(name)
        # Use .json files to create _design/* entries
        for ind in ["xpm", "block", "trans"]:
            json_filename = "../couchdb/design.%s.json" % ind
            print("Loading JSON file : %s" % json_filename)
            json_file = open(json_filename,"r")
            if json_file:
                xpm_map = json.load(json_file)
                if xpm_map:
                    # Need to remove revision field to import content
                    try:
                        del xpm_map["_rev"]
                    except:
                        pass
                json_file.close()
                cdb.save(xpm_map)
    return(cdb)

def main():
    global debug, version, conf_file

    server = "localhost"
    verbose = False
    replace = False
    parser = OptionParser(usage="%prog [-cdr] first [last]\n\nFor help try : %prog --help", formatter=IndentedHelpFormatter(), epilog="(Copyleft) 2016 - AWI : http://pool-prime.net/", description="Arguments:\t\t\t\t\t\t\t\t\t\t\tfirst: start to insert blocks from this block (default: 0)\t\t\t\tlast: stop to insert at this block (default is -1, means last received from primecoind)", version="%prog " + "%s" % version)
    parser.add_option("-c", "--conf", action="store", type="string", dest="conf", default=conf_file, help="configuration file (default: %default)")
    parser.add_option("-r", "--replace", action="store_true", dest="replace", default=replace, help="replace existing records (default: %default)")
    parser.add_option("-d", "--debug", action="store_true", dest="debug", default=debug, help="verbose logging (default: %default)")
    (options, args) = parser.parse_args()
    debug = options.debug
    replace = options.replace
    conf_file = options.conf
    conf_file = os.path.expanduser(conf_file)
    if debug:
        print("options=%s, args=%s" % (options, args ))
    if len(args):
        if len(args) == 1:
            first = int(args[0])
            last = -1
        else:
            if len(args) == 2:
                first = int(args[0])
                last = int(args[1])
            else:
                parser.error("no more than 2 arguments...")
    else:
        last = -1
        first = -1
    content = "Can not read %s (perhaps need to be in bitcoin group ?)" % conf_file
    try:
        conf = open(conf_file, 'r')
        content = conf.read()
        conf.close()
    except:
        pass
    user = password = ""
    port = 9912
    for line in content.split():
        if "rpcuser" in line:
            (tmp, user) = line.split('=',1)
        if "rpcpassword" in line:
            (tmp, password) = line.split('=',1)
        if "rpcport" in line:
            (tmp, ports) = line.split('=',1)
            port = int(ports)
    if debug:
        print("port=%d, user=%s, password=%s" % (port, user, password))
    if jrpc:
        primecoind = ServiceProxy("http://%s:%s@127.0.0.1:%d" % (user, password,port))
    else:
        primecoind = bitcoin.rpc.RawProxy("http://%s:%s@127.0.0.1:%d" % (user, password,port))

    # Yes, it means that you need to use same user:password for primecoind and couchdb admin...
    # if not, fix value here
    # user = "couchdbadmin"
    # password = "couchdbadmin_password"
    couchdb = Server(url='http://%s:%s@localhost:5984/' % (user, password))
    cdb = cdb_connect(couchdb, "xpm")

    if first < 0:
        #headers = {"Content-type": "application/json"}
        #r = requests.get("http://localhost:5984/xpm/_design/block/_view/last?limit=1&descending=true", headers=headers)
        #rec = r.json()
        #first = int(rec["rows"][0]["key"]) + 1
        row = cdb.view("_design/block/_view/by_height", limit=1, descending=True)
        if row:
            first = int(list(row)[0].key) + 1
        else:
            first = 0
    if last < 0:
        last = int(primecoind.getblockcount()) + 1
    if debug:
        print("Let's go for blocks from %d to %d :" % (first, last))
    if primecoind and cdb:
        for current in range(first, last):
            blk_hash = primecoind.getblockhash(current)
            block = primecoind.getblock(blk_hash)
            dte = datetime.utcfromtimestamp(block['time'])
            when = [dte.year, dte.month, dte.day, dte.hour, dte.minute, dte.second]
            block["_id"] = blk_hash
            block["date"] = when
            block["type"] = "block"
            if debug:
                print("xpm:primecoind:%s" % (block))
            for txid in block['tx']:
                # Only block 0 should have no trans
                if current:
                    rawtrans = primecoind.getrawtransaction(txid)
                    trans = primecoind.decoderawtransaction(rawtrans)
                    trans["raw"] = rawtrans
                    if debug:
                        print("%s:%s" % (current,txid))
                    trans["_id"] = txid
                    trans["height"] = current
                    trans["date"] = when
                    trans["type"] = "trans"
                    if replace:
                        record = cdb.get(txid)
                        if record:
                            if debug:
                                print("%s:%s trans already exist..." % (record.id, record.rev))
                            cdb.delete(record)
                    cdb.save(trans)
            if replace:
                record = cdb.get(blk_hash)
                if record:
                    if debug:
                        print("%s:%s block already exist..." % (record.id, record.rev))
                    cdb.delete(record)
            cdb.save(block)
            print("%d:%d-%d-%d:%d:%d:%s(%d)" % (current, dte.year,dte.month,dte.day, dte.hour, dte.minute,blk_hash,len(block['tx'])))
        return(0)
    else:
        return(-1)

if (__name__ == '__main__'):
    sys.exit(main())

#vim: ts=4 filetype=python
