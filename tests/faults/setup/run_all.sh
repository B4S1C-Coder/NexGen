#!/usr/bin/env bash
# Unattended NexGen fault-injection evaluation; the instance stops itself at the end.
sudo shutdown -h +420 "NexGen eval safety net (7 h)"
cd ~/NexGen/master
rm -rf ../tests/faults/runs
~/.local/bin/uv run python ../tests/faults/run_faults.py --demo ~/otel-demo --reps 3 --warm 180 --cool 120 > ~/logs/faults.log 2>&1
~/.local/bin/uv run python ../tests/faults/analyze.py > ~/logs/analyze.log 2>&1
echo "finished $(date -u)" > ~/EVAL_DONE
sudo shutdown -c
sudo shutdown -h +10 "NexGen eval finished; stopping in 10 minutes"
