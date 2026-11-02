#!/usr/bin/env bash
# =====================================================================
# Launch (survives logout / closing your laptop):
#   nohup bash run_multilepton.sh > ~/cf_dev.log 2>&1 & disown
#
# Watch (clean status only, drop the luigi spam):
#   tail -f ~/cf_dev.log | grep --line-buffered -E "all: [0-9]|submitted [0-9]+/|failed|error|ERROR"
#   squeue -u kjaffel -h -o "%T %r" | sort | uniq -c
#
# Snapshot without running anything:  add  --print-status 2  to a law command.
# =====================================================================

# ---- per-job slurm walltime ----
export CF_SLURM_RUNTIME=6h

# ---------------- what to run ----------------
task=cf.SelectEvents        # cf.CalibrateEvents | cf.SelectEvents | cf.ReduceEvents | cf.ProduceColumns
                            #   | cf.CreateYieldTable | cf.PlotVariables1D
config=24_v15_central
version=matheus_new_prod_ext2   # <-- YOUR dev version; bump it for a fresh run

selector=default
calibrators=default
producers=default
limit_dataset_files=-1      # -1 = all files

# ---------------- speed / scheduling ----------------
tasks_per_job=10            # bundle this many files into one slurm job
parallel_jobs=200           # max concurrent jobs
poll_interval=120           # seconds between status prints (less log spam)

# ---------------- plotting / yield only ----------------
categories=c4mu,c3mu1tau,c2mu2tau
variables=nmu
_shift=nominal

requested_datasets=(
dy_tautau_m10to50_powheg
dy_tautau_m50toinf_0j_amcatnlo
dy_tautau_m50toinf_1j_amcatnlo
dy_tautau_m50toinf_2j_amcatnlo
dy_ee_m10to50_powheg
dy_ee_m50toinf_0j_amcatnlo
dy_ee_m50toinf_1j_amcatnlo
dy_ee_m50toinf_2j_amcatnlo
dy_mumu_m10to50_powheg
dy_mumu_m50toinf_0j_amcatnlo
dy_mumu_m50toinf_1j_amcatnlo
dy_mumu_m50toinf_2j_amcatnlo
)

datasets_csv=$(IFS=,; echo "${requested_datasets[*]}")

# ===================================================================
# Aggregation tasks (yield table / plots) combine all datasets in one call
# ===================================================================
if [[ "$task" == cf.CreateYieldTable || "$task" == cf.PlotVariables1D ]]; then
    extra=( --producers "$producers" --categories "$categories" )
    for cft in cf.GetDatasetLFNs cf.CalibrateEvents cf.SelectEvents cf.ReduceEvents cf.ProduceColumns; do
        extra+=( --${cft}-limit-dataset-files "$limit_dataset_files" )
    done
    [[ "$task" == cf.PlotVariables1D ]] && extra+=( --variables "$variables" --shift "$_shift" )
    set -x
    law run "$task" \
        --config "$config" --datasets "$datasets_csv" \
        --version "$version" --selector "$selector" --calibrators "$calibrators" \
        --workers 4 --retries 1 "${extra[@]}" "$@"
    set +x
    exit $?
fi

# ===================================================================
# Per-dataset processing: ONE long-lived driver, datasets one after another.
# This is the robust choice: if the driver ever dies, just re-run this script
# -- columnflow skips every finished branch and only resubmits what's missing.
# Each dataset is now fast because of --tasks-per-job.
# ===================================================================
for dataset in "${requested_datasets[@]}"; do
    echo "=================  $dataset  ================="
    set -x
    law run "$task" \
        --config "$config" \
        --dataset "$dataset" \
        --version "$version" \
        --workflow slurm \
        --selector "$selector" \
        --calibrators "$calibrators" \
        --limit-dataset-files "$limit_dataset_files" \
        --shift "$_shift" \
        --tasks-per-job "$tasks_per_job" \
        --cf.CalibrateEvents-tasks-per-job "$tasks_per_job" \
        --parallel-jobs "$parallel_jobs" \
        --poll-interval "$poll_interval" \
        --retries 1 \
        --workers 1 \
        "$@"
    set +x
done

# -------------------------------------------------------------------
# OPTIONAL — true "all datasets at once" under a SINGLE scheduler.
# Comment out the for-loop above and use this instead if you prefer it
# (one driver submits every dataset together, one global --parallel-jobs cap):
#
#   law run cf.SelectEventsWrapper \
#       --configs "$config" --datasets "$datasets_csv" --shifts "$_shift" \
#       --version "$version" --selector "$selector" --calibrators "$calibrators" \
#       --workflow slurm --limit-dataset-files "$limit_dataset_files" \
#       --cf.SelectEvents-tasks-per-job "$tasks_per_job" \
#       --cf.SelectEvents-parallel-jobs "$parallel_jobs" \
#       --cf.CalibrateEvents-tasks-per-job "$tasks_per_job" \
#       --cf.CalibrateEvents-parallel-jobs "$parallel_jobs" \
#       --poll-interval "$poll_interval" --workers 4 --retries 1
# -------------------------------------------------------------------
