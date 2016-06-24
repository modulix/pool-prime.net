#!/bin/bash
#
# (Copyleft) - 2016 AWI : Aqitaine Webmédia Indépendant
#
source /etc/profile

while read LIG
    do
	if [ $(echo $LIG | grep -c "SetBestChain") != "0" ]
		then
		./bin/primecoind2couchdb.py
	fi
done < <(tail -f ~/.primecoin/debug.log)

