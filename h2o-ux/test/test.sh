#!/bin/sh

echo "!! Assuming you have casper installed"

casperjs test ./casper --testhost=localhost:8888 --screenfile=./casper-screen.png --pre=setUp.coffee --direct --log-level=warning