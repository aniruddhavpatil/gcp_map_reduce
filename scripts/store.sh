#!/bin/bash

cd ~
rm -rf gcp_map_reduce
git clone https://github.com/aniruddhavpatil/gcp_map_reduce.git
cd gcp_map_reduce
nohup python3 simple_key_value_store/Server.py &