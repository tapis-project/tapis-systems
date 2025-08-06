#!/bin/bash
set -xv
kubectl apply -f systems-config.yml
kubectl create -f credinfoinitjob-apply.yml
