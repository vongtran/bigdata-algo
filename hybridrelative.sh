#!/usr/bin/env bash

hadoop fs -rm -R /user/cloudera/amz
hadoop fs -mkdir /user/cloudera /user/cloudera/amz /user/cloudera/amz/input

hadoop fs -put file* /user/cloudera/amz/input

hadoop jar cs1-0.0.1-SNAPSHOT.jar cs1.cb.PairMapStripeReduceRelative /user/cloudera/amz/input /user/cloudera/amz/outputpr1
