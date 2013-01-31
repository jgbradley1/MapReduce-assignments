#!/bin/bash
# By Josh Bradley

echo "Starting MapReduce job...\n"

# execute the MapReduce job
src/Cloud9/etc/hadoop-cluster.sh edu.umd.cloud9.example.simple.DemoWordCount -input bible+shakes.nopunc.gz -output jgbradley1 -numReducers 5

echo "Done"
