#!/bin/bash

run_query() {
    compose_file=$1
    log_file=$2

    docker-compose -f $compose_file down --remove-orphans

    docker-compose -f $compose_file up --build

    docker-compose -f $compose_file logs -f client > $log_file &
}

case "$1" in
    "q1")
        run_query "docker-compose1.yaml" "output1.txt"
        ;;
    "q2")
        run_query "docker-compose2.yaml" "output2.txt"
        ;;
    "q3")
        run_query "docker-compose3.yaml" "output3.txt"
        ;;
    "q4")
        run_query "docker-compose4.yaml" "output4.txt"
        ;;
    "q5")
        run_query "docker-compose5.yaml" "output5.txt"
        ;;
    *)
        echo "Usage: ./run.sh [q1|q2|q3|q4|q5]"
        exit 1
        ;;
esac

exit 0
