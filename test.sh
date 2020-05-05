#!/bin/bash


echo $1

case $1 in
    "pub")
    ./test-nsqd-pub 192.168.0.102 sample_topic
    ;;
    "sub")
    ./test-nsqd-sub 192.168.0.102 sample_topic ch
    ;;
    *)
    ./test-nsqd-pub 192.168.0.102 sample_topic
    ./test-nsqd-sub 192.168.0.102 sample_topic ch
    ;;
esac

#./test-lookupd-sub 192.168.0.100 sample_topic ch
