#!/bin/bash

cd ~
rm -rf gcp_map_reduce
git clone https://github.com/aniruddhavpatil/gcp_map_reduce.git
cd gcp_map_reduce
sleep 60
python3 Worker.py > cloud_log.txt