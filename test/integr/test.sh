#!usr/bin/env bash

# Set up the binaries
mkdir -p "build"
cd "build"
cmake .. > /dev/null
make > /dev/null
cd ..

path="./build/MaRe-Nostrum"

output=$($path "file10000.txt")
if [[ $? != 0 ]]; then  # Check the exit code
  echo "FAILED"
else
  # Check the output
  # It must contain 10 '-' and 10 words "about:". sum of numbers following "about:" must be 196
  if [[ $(echo $output | grep -o "-" | wc -l) != 10 ]]; then
    echo "FAILED"
  elif [[ $(echo $output | grep -o "about:" | wc -l) != 10 ]]; then
    echo "FAILED"
  elif [[ $(echo $output | grep -o "about: [0-9]*" | grep -o "[0-9]*" | awk '{s+=$1} END {print s}') != 196 ]]; then
    echo "FAILED"
  else
    echo "OK"
  fi
fi
