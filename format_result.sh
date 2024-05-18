#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Usage: ./format_result.sh <log_file>"
    exit 1
fi

log_file=$1

echo "Processing log file: $log_file"

awk '
/Received/ {
    match($0, /\[x\] Received (.*)/, arr)
    print arr[1]
}
' $log_file > temp_file

mv temp_file $log_file

echo "Formatted log saved to $log_file"
