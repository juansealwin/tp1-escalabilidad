#!/bin/bash
case "$1" in
    "q1")
        docker-compose -f docker-compose1.yaml up --build
        ;;
    "q2")
        docker-compose -f docker-compose2.yaml up --build
        ;;
    "q3")
        docker-compose -f docker-compose3.yaml up --build
        ;;
    "q4")
        docker-compose -f docker-compose4.yaml up --build
        ;;
    "q5")
        docker-compose -f docker-compose5.yaml up --build
        ;;
    *)
        echo "Usage: ./run.sh [q1|q2|q3|q4|q5]"
        exit 1
        ;;
esac

exit 0