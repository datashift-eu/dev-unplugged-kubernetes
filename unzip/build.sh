#!/bin/bash
cd unzip
docker build -q -t localhost:5001/mission-control/unzip .
docker push -q localhost:5001/mission-control/unzip
cd ..