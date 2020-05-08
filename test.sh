#!/bin/bash

case $2 in
    "pub")
    ./test-nsqd-pub $1 sample_topic
    ;;
    "sub")
    ./test-nsqd-sub $1 sample_topic ch
    ;;
    "lookupd-pub")
    ./test-lookupd-pub $1 sample_topic
    ;;
    "lookupd-sub")
    ./test-lookupd-sub $1 sample_topic ch
    ;;
    *)
    ./test-nsqd-pub $1 sample_topic
    ./test-nsqd-sub $1 sample_topic sample_channel
    ;;
esac
