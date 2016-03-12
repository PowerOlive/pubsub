#!/bin/bash
ulimit -n 1024768 && rm -f nohup.out && nohup ./perfclient -numclients 10000 -rampup 5m -stderrthreshold WARNING &

