#!/bin/bash

cd ~
rm -rf gcp_map_reduce
git clone https://github.com/aniruddhavpatil/gcp_map_reduce.git
cd gcp_map_reduce/simple_key_value_store
nohup python3 Server.py > store.log &