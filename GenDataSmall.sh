#!/usr/bin/env bash
rm -rf 'Data/Live/'
mkdir -p 'Data/Live/'
for f in Data/Small/x*; do
	echo $f
	cp $f Data/Live/
	sleep 3s
done
