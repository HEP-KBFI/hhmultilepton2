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

# ---------------- what to run ----------------
task=cf.SelectEvents        # cf.CalibrateEvents | cf.SelectEvents | cf.ReduceEvents | cf.ProduceColumns
                            #   | cf.CreateYieldTable | cf.PlotVariables1D
config=24_v15_central
version=matheus_new_prod_ext2
workflow=slurm              # slurm | local | htcondor
selector=default
calibrators=default
producers=default
limit_dataset_files=-1      # -1 = all files

# ---- per-job slurm walltime ----
export CF_SLURM_RUNTIME=6h

# ---------------- scheduling mode ----------------
# wrapper    = ALL datasets together under one driver -> SLURM fills the queue
#              across every dataset at once (DEFAULT). NOTE: --parallel-jobs is
#              PER DATASET, so total concurrent ~= parallel_jobs x #datasets.
# sequential = one dataset at a time (robust, but later datasets wait).
mode="${MODE:-wrapper}"

# ---------------- speed / scheduling ----------------
tasks_per_job=1             # 1 ROOT file per job: fast + never times out (10/job TIMED OUT on low-mass DY)
parallel_jobs=300            # max concurrent jobs PER DATASET (NOT global, see note) -- low to coexist with bamboo
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
# PARALLEL mode (default): ONE driver submits ALL datasets together under a
# single scheduler. SLURM then fills the queue across every dataset at once --
# no dataset waits for another. NOTE: --parallel-jobs is PER DATASET (not global),
# so total concurrent ~= parallel_jobs x #datasets -- keep it modest vs bamboo.
# Resumable: if the driver dies, just re-run -- finished branches are skipped.
# ===================================================================
if [[ "$mode" == wrapper ]]; then
    wrapper="${task}Wrapper"          # cf.SelectEvents -> cf.SelectEventsWrapper
    workflow_tasks=(cf.CalibrateEvents cf.SelectEvents cf.ReduceEvents cf.ProduceColumns)
    analysis_tasks=(cf.SelectEvents cf.ReduceEvents cf.ProduceColumns)

    wrap_extra=()
    for cft in "${workflow_tasks[@]}"; do
        wrap_extra+=(
            --${cft}-workflow "$workflow"
            --${cft}-tasks-per-job "$tasks_per_job"
            --${cft}-parallel-jobs "$parallel_jobs"
            --${cft}-poll-interval "$poll_interval"
            --${cft}-retries 1
            --${cft}-limit-dataset-files "$limit_dataset_files"
        )
    done
    for cft in "${analysis_tasks[@]}"; do
        wrap_extra+=( --${cft}-selector "$selector" --${cft}-calibrators "$calibrators" )
    done

    set -x
    law run "$wrapper" \
        --configs "$config" \
        --datasets "$datasets_csv" \
        --shifts "$_shift" \
        --version "$version" \
        --workers 4 \
        "${wrap_extra[@]}" "$@"
    set +x
    exit $?
fi

# ===================================================================
# SEQUENTIAL fallback: ONE long-lived driver, datasets one after another.
# Robust, but every dataset waits for the previous one to finish.
# Use with:  MODE=sequential bash run_multilepton_dev.sh
# ===================================================================
for dataset in "${requested_datasets[@]}"; do
    echo "=================  $dataset  ================="
    set -x
    law run "$task" \
        --config "$config" \
        --dataset "$dataset" \
        --version "$version" \
        --workflow "$workflow" \
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
