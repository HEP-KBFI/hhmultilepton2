#!/usr/bin/env bash
# =====================================================================
# Smoke test BEFORE the full submission: ONE slurm job, then stops.
#
#   CANARY=1 bash run_multilepton.sh      (hands over to this script)
#   bash tests/canary_slurm.sh            (same thing)
#   -> "CANARY OK ... you can now switch to full submission"  or  "CANARY FAILED"
#
# Runs cf.CalibrateEvents branch 0 (the stage where jobs used to hang) for a random dataset of
# requested_datasets, with the same version and parameters as the full run (run_multilepton.sh),
# so the full submission just skips that branch. Waits for the job, reports, submits nothing else.
#
# optional:  CANARY_DATASET=dy_ee_m10to50_powheg   CANARY_TIMEOUT=20m
#            extra arguments are passed on to law
# =====================================================================

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo" || exit 1

# config, version, datasets, ... come from the run script, which stops after its settings
RUN_MULTILEPTON_SETTINGS_ONLY=1 source "$repo/run_multilepton.sh"

canary_dataset="${CANARY_DATASET:-}"      # pin a dataset instead of drawing a random one
canary_timeout="${CANARY_TIMEOUT:-30m}"   # give up (and scancel the job) after this long

canary_job_info() {     # $1 = dataset, $2 = stamp file -> "status:code:job_id" per job law recorded since the stamp
    # law's own verdict: once a job is done it sets status=finished/failed and replaces the real
    # slurm job id by the string "dummy_job_id", so the id is only usable while the job is alive
    local f
    while IFS= read -r f; do
        python3 - "$f" <<'PY'
import json, sys
for job in json.load(open(sys.argv[1])).get("jobs", {}).values():
    print("{}:{}:{}".format(job.get("status"), job.get("code"), job.get("job_id")))
PY
    done < <(find "$CF_STORE_LOCAL"/*/cf.CalibrateEvents/"$config"/"$1" -path "*/$version/slurm_jobs_*.json" -newer "$2" 2>/dev/null)
}

if [[ "$workflow" != slurm ]]; then
    echo "CANARY is a slurm smoke test but workflow=$workflow, nothing to do"
    exit 1
fi
pick="${canary_dataset:-${requested_datasets[RANDOM % ${#requested_datasets[@]}]}}"
stamp=$(mktemp)
if [[ -t 1 ]]; then c_ok=$'\033[1;32m'; c_bad=$'\033[1;31m'; c_off=$'\033[0m'; else c_ok=; c_bad=; c_off=; fi

canary_cancel() {   # scancel what the canary submitted (the driver may be gone before the job ends)
    local ids
    ids=$(canary_job_info "$pick" "$stamp" | awk -F: '$3 ~ /^[0-9]+$/ {print $3}' | tr '\n' ' ')
    [[ -n "$ids" ]] && { echo "cancelling canary slurm job(s): $ids"; scancel $ids; }
}

if [[ -n "$canary_dataset" ]]; then
    how="pinned via CANARY_DATASET"
else
    how="random draw out of ${#requested_datasets[@]} requested datasets"
fi
echo
echo "CANARY: 1 slurm job, cf.CalibrateEvents branch 0 of ${pick}"
echo "        (${how}), version ${version}, timeout ${canary_timeout}"
echo
start=$SECONDS
set -x
timeout -k 30 "$canary_timeout" law run cf.CalibrateEvents \
    --config "$config" --dataset "$pick" --calibrator "${calibrators%%,*}" \
    --version "$version" --shift "$_shift" \
    --workflow "$workflow" --branches 0 \
    --tasks-per-job "$tasks_per_job" --limit-dataset-files "$limit_dataset_files" \
    --poll-interval 1 --retries 0 --workers 1 "$@" &
canary_pid=$!
set +x
trap 'echo; echo "canary interrupted"; kill "$canary_pid" 2>/dev/null; canary_cancel; exit 130' INT TERM
wait "$canary_pid"; rc=$?
trap - INT TERM
took=$(( SECONDS - start ))

timed_out=0
if [[ $rc -eq 124 || $rc -eq 137 ]]; then
    timed_out=1
    canary_cancel
fi
info=$(canary_job_info "$pick" "$stamp")      # law's verdict per job: status:code:job_id
log=$(find "$CF_STORE_LOCAL"/*/cf.CalibrateEvents/"$config"/"$pick" -path "*/$version/stdall_*.txt" -newer "$stamp" 2>/dev/null | head -1)
rm -f "$stamp"

reason=
if [[ $timed_out == 1 ]]; then
    reason="no result after ${canary_timeout}: the job hangs again"
elif [[ -z "$info" ]]; then
    reason="no slurm job was recorded, law stopped before submitting (exit code $rc)"
elif grep -qv '^finished:0:' <<<"$info"; then
    reason="law does not report the job as finished with code 0"
elif [[ -n "$log" ]] && ! tail -5 "$log" | grep -q 'job exit code : 0'; then
    reason="the job log does not end with 'job exit code : 0'"
fi

echo
echo "================================================================================"
if [[ -z "$reason" ]]; then
    echo "${c_ok} CANARY OK${c_off}: cf.CalibrateEvents branch 0 of ${pick} completed in $((took / 60))m$((took % 60))s (law: finished, job exit code 0)"
    [[ $(wc -l <<<"$info") -gt 1 ]] && echo " WARNING: law recorded several jobs, expected exactly one"
    echo
    echo " You can now switch to full submission (that branch is done and will be skipped):"
    echo "     nohup bash run_multilepton.sh > ~/cf_dev.log 2>&1 & disown"
else
    echo "${c_bad} CANARY FAILED${c_off}: ${reason}"
    [[ -n "$info" ]] && echo " law:   ${info//$'\n'/ ; }   (status:code:job_id)"
    [[ -n "$log" ]] && echo " log:   ${log}"
    echo
    echo " Do NOT launch the full submission yet."
fi
echo "================================================================================"
[[ -z "$reason" ]]
exit $?
