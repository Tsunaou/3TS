#!/bin/bash
rm ./test-mongo
c++ --std=c++11 src/mongodb_test.cc \
  -I/usr/local/include/mongocxx/v_noabi \
  -I/usr/local/include/libmongoc-1.0 \
  -I/usr/local/include/bsoncxx/v_noabi \
  -I/usr/local/include/libbson-1.0 \
  -L/usr/local/lib -lmongocxx -lbsoncxx \
  --output test-mongo

export LD_LIBRARY_PATH=/usr/local/lib64:$LD_LIBRARY_PATH
./test-mongo
