#!/bin/bash

( find -name '[^\.]*.py';
  echo "pack.sh";
  echo "pandoc.sh";
  echo "README.md"; ) \
  \
  | zip themuserb -@

