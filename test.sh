#!/bin/bash

case $1 in
    "pub")
    ./test-nsqd-pub 192.168.0.100 sample_topic
    ;;
    "sub")
    ./test-nsqd-sub 192.168.0.100 sample_topic ch
    ;;
    "lookupd-sub")
    ./test-lookupd-sub 192.168.0.100 sample_topic sample_channel
    ;;
    *)
    ./test-nsqd-pub 192.168.0.100 sample_topic
    ./test-nsqd-sub 192.168.0.100 sample_topic sample_channel
    ;;
esac
