#!/usr/bin/env bash
# usage: stallwatch.sh <marker> <outdir> <worktree>
marker=$1; out=$2; tree=$3; mkdir -p "$out"; n=0; last=""
while true; do
  if [ -f "$marker" ]; then
    mtime=$(stat -c %Y "$marker" 2>/dev/null) || { sleep 1; continue; }
    age=$(( $(date +%s) - mtime ))
    label=$(cat "$marker" 2>/dev/null)
    if [ "$age" -ge 8 ] && [ "$label" != "$last" ]; then
      for p in $(pgrep -x beam.smp); do
        [ "$(readlink /proc/$p/cwd)" = "$tree" ] || continue
        n=$((n+1)); f="$out/stall-$n.txt"
        { echo "age=${age}s label=$label"; date -u +%T;
          for t in /proc/$p/task/*; do echo "$(basename $t) $(cat $t/comm) $(awk '{print $3}' $t/stat) $(cat $t/wchan)"; done
        } > "$f"
        timeout 120 ${GDB:-gdb} -p "$p" -batch -ex "set pagination off" -ex "thread apply all bt 30" >> "$f" 2>&1
        echo "captured $f"
      done
      last=$label
    fi
  fi
  sleep 1
done
