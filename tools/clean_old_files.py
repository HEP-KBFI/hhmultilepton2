#!/usr/bin/env python3
# coding: utf-8

"""
Interactively clean old versions (tests, failed attempts, stale job dirs) from the columnflow store,
either the LOCAL store (default) or the REMOTE xrootd/webdav store (--remote).

A "version" is the last directory of a columnflow output path, e.g.

    cf.CalibrateEvents/24_v15_central/<dataset>/nominal/calib__default/<version>/
        stdall_0To1.txt  slurm_jobs_0.json  job_aBc123/  calib_0.parquet ...

Versions are found by content (a dir holding files or job_*/cf_wlcg_cache dirs), not by depth, as
the depth differs between tasks. By default the same version name is grouped over all tasks and
datasets, so you answer once per version name (use --by path to decide dir by dir).

Every cf.* task dir below the root is covered (calibration, selection, reduction, columns, ML,
histograms, plots, yield tables, ...). Tasks without a version level (bundles, LFN lists, sandboxes)
are listed per task, never auto-selected and can be left out with --skip-unversioned. A version
dir that has subdirs (e.g. per-fold ML outputs) is one unit. Empty directories (stale version dirs)
are rows like any other, with the size "empty", and go through the same keep/delete questions.

Nothing is deleted while you answer: your choices are collected, shown as a summary, and removed
only after a final confirmation (skip it with --yes, never delete with --dry-run).

Local store: /local/.../cf_store/analysis_multilepton (logs, job dirs, locally written outputs).
Remote store (--remote): davs://xrootd.hep.kbfi.ee:1094/store/user/kjaffel/cf_store, where the
tasks of --workflow slurm write their real outputs. It needs the python gfal2 bindings (run it from
your multilepton_venv shell) and a valid grid proxy. Remote deletion is irreversible (no trash) and
goes file by file, then directories bottom-up, never relying on a server-side recursive rmdir.

Safety: dirs referenced by a live slurm job (squeue, matched through the slurm_jobs_*.json in the
local store) are never offered, dirs modified in the last --recent-min minutes are protected too
(override with --force), and every path is checked to be strictly inside the root before removal.

Examples:

    clean_old_files.py --list                     # just show what is there (local store)
    clean_old_files.py --remote --list            # same for the remote store
    clean_old_files.py --remote --tests-only      # only test-like versions, remote store
    clean_old_files.py --remote --empty-only      # only empty dirs, remote store (answer d/k per row)
    clean_old_files.py --match 'test_*' --older-than 2 --dry-run
    clean_old_files.py --task cf.CalibrateEvents --by path
"""

from __future__ import annotations

import argparse
import errno
import fnmatch
import getpass
import json
import logging
import os
import re
import shutil
import stat
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime


DEFAULT_ROOT = "/local/kjaffel/HHMultilepton_Run3/cf_store/analysis_multilepton"
DEFAULT_LOCAL_STORE = "/local/kjaffel/HHMultilepton_Run3/cf_store"
DEFAULT_REMOTE_ROOT = "davs://xrootd.hep.kbfi.ee:1094/store/user/kjaffel/cf_store"

# tasks without a real <version> level: grouped per task, never auto-selected, recreated on demand
UNVERSIONED_TASKS = {
    "cf.BundleExternalFiles", "cf.BundleRepo", "cf.BundleSoftware", "cf.BundleBashSandbox",
    "cf.BundleCMSSWSandbox", "cf.BuildBashSandbox", "cf.GetDatasetLFNs",
}

# version names that look like throw-away tests, only used as a hint and for --tests-only / "t"
TEST_RE = re.compile(r"(^|[_\-.])(my|old|bak)?(test|tmp|temp|debug|dev|try|trial|regtest|nocache|dry|scratch)", re.I)

# empty dirs named like this are not versions but what is left of the parents of one: law level
# dirs such as calib__default / sel__default / prod__default, and the shift dir (nominal, *_up, *_down)
PARENT_RE = re.compile(r"__|^nominal$|_(up|down)$")
LEFTOVER_PARENTS = "(empty parent dirs)"

# "namespace.TaskFamily", as opposed to analysis dirs such as "analysis_multilepton"
TASK_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z]")


# ---------------------------------------------------------------------------------------------
# colors and logging
# ---------------------------------------------------------------------------------------------

class Style(object):

    enabled = True
    codes = dict(
        reset="0", bold="1", dim="2", red="31", green="32", yellow="33", blue="34", magenta="35",
        cyan="36", grey="90",
    )

    @classmethod
    def paint(cls, text, *names):
        if not cls.enabled or not names:
            return str(text)
        return "\033[{}m{}\033[0m".format(";".join(cls.codes[n] for n in names), text)


paint = Style.paint


class ColorFormatter(logging.Formatter):

    level_styles = {
        logging.DEBUG: ("grey",),
        logging.INFO: ("green",),
        logging.WARNING: ("yellow",),
        logging.ERROR: ("red",),
        logging.CRITICAL: ("bold", "red"),
    }

    def format(self, record):
        stamp = paint(time.strftime("%H:%M:%S", time.localtime(record.created)), "grey")
        level = paint("{:<7}".format(record.levelname), *self.level_styles.get(record.levelno, ()))
        return "{} {} {}".format(stamp, level, record.getMessage())


log = logging.getLogger("clean_old_files")


def setup_logging(verbose=False):
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(ColorFormatter())
    log.addHandler(handler)
    log.setLevel(logging.DEBUG if verbose else logging.INFO)
    log.propagate = False


def human_size(n):
    n = float(n)
    for unit in ("B", "K", "M", "G"):
        if n < 1024:
            return "{:.0f}{}".format(n, unit) if unit == "B" else "{:.1f}{}".format(n, unit)
        n /= 1024.0
    return "{:.1f}T".format(n)


def size_style(n, empty=False):
    return "grey" if empty else "red" if n >= 1 << 30 else "yellow" if n >= 100 << 20 else "green"


def size_text(n, empty=False):
    return "empty" if empty else human_size(n)


def human_age(seconds):
    seconds = max(seconds, 0)
    if seconds < 3600:
        return "{:.0f} min".format(seconds / 60.0)
    if seconds < 48 * 3600:
        return "{:.0f} h".format(seconds / 3600.0)
    return "{:.0f} d".format(seconds / 86400.0)


def fmt_time(ts):
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


# ---------------------------------------------------------------------------------------------
# storage backends: local filesystem or remote (gfal2, davs://)
# ---------------------------------------------------------------------------------------------

@dataclass
class Dirent(object):
    name: str
    path: str       # absolute path or full url
    is_dir: bool
    is_link: bool
    size: int       # bytes (on disk for the local store), 0 for remote dirs
    mtime: float

    @property
    def real_dir(self):
        return self.is_dir and not self.is_link


class LocalBackend(object):

    remote = False

    def __init__(self, root):
        self.root = os.path.realpath(root)

    def listdir(self, path):
        out = []
        with os.scandir(path) as it:
            for k in it:
                try:
                    st = k.stat(follow_symlinks=False)
                except OSError:
                    continue
                size = getattr(st, "st_blocks", 0) * 512 or st.st_size
                out.append(Dirent(k.name, k.path, k.is_dir(follow_symlinks=False), k.is_symlink(), size, st.st_mtime))
        return out

    def map(self, fn, items):
        return [fn(i) for i in items]

    def exists(self, path):
        return os.path.lexists(path)

    def is_dir(self, path):
        return os.path.isdir(path) and not os.path.islink(path)

    def inside(self, path):
        return os.path.realpath(path).startswith(self.root + os.sep)

    def relpath(self, path):
        return os.path.relpath(path, self.root).replace(os.sep, "/")

    def dirname(self, path):
        return os.path.dirname(path)

    def rmdir(self, path):
        os.rmdir(path)

    def delete_tree(self, path):
        errors = []
        shutil.rmtree(path, onerror=lambda func, p, exc: errors.append((p, exc[1])))
        if errors:
            raise OSError(str(errors[0][1]))


class RemoteBackend(object):

    remote = True

    def __init__(self, root, threads=8):
        import gfal2  # ImportError is handled by the caller
        self.gfal2 = gfal2
        self.root = root.rstrip("/")
        self._tls = threading.local()
        self._pool = ThreadPoolExecutor(max_workers=max(threads, 1))

    def ctx(self):
        ctx = getattr(self._tls, "ctx", None)
        if ctx is None:
            ctx = self._tls.ctx = self.gfal2.creat_context()
        return ctx

    @staticmethod
    def _enoent(exc):
        return getattr(exc, "code", None) == 2

    def _retry(self, fn, what, tries=3):
        for i in range(tries):
            try:
                return fn()
            except Exception as exc:  # gfal2.GError
                if self._enoent(exc):
                    raise OSError(errno.ENOENT, "{}: {}".format(what, exc))
                if i == tries - 1:
                    raise OSError("{}: {}".format(what, exc))
                time.sleep(2 * (i + 1))

    def listdir(self, url):
        def go():
            d = self.ctx().opendir(url)
            out = []
            while True:
                res = d.readpp()
                if not res or res[0] is None:
                    break
                dirent, st = res
                name = dirent.d_name
                if name in (".", ".."):
                    continue
                is_dir = stat.S_ISDIR(st.st_mode)
                out.append(Dirent(name, url + "/" + name, is_dir, stat.S_ISLNK(st.st_mode),
                                  0 if is_dir else int(st.st_size), float(st.st_mtime)))
            return out
        return self._retry(go, "listing " + url)

    def map(self, fn, items):
        return list(self._pool.map(fn, items))

    def exists(self, url):
        try:
            self.ctx().stat(url)
            return True
        except Exception as exc:
            if self._enoent(exc):
                return False
            raise OSError("stat {}: {}".format(url, exc))

    def is_dir(self, url):
        try:
            return stat.S_ISDIR(self.ctx().stat(url).st_mode)
        except Exception:
            return False

    def inside(self, url):
        url = url.rstrip("/")
        return url.startswith(self.root + "/") and ".." not in url.split("/")

    def relpath(self, url):
        return url[len(self.root) + 1:]

    def dirname(self, url):
        return url.rsplit("/", 1)[0]

    def rmdir(self, url):
        self._retry(lambda: self.ctx().rmdir(url), "rmdir " + url)

    def _unlink(self, url):
        try:
            self._retry(lambda: self.ctx().unlink(url), "unlink " + url)
        except OSError as exc:
            if exc.errno != errno.ENOENT:    # already gone is fine
                raise

    def delete_tree(self, url):
        """Files first (in parallel), then subdirs, then the empty dir itself, bottom-up."""
        kids = self.listdir(url)
        list(self._pool.map(self._unlink, [k.path for k in kids if not k.real_dir]))
        for k in kids:
            if k.real_dir:
                self.delete_tree(k.path)
        self.rmdir(url)


# ---------------------------------------------------------------------------------------------
# scanning
# ---------------------------------------------------------------------------------------------

@dataclass
class Entry(object):
    """One version directory."""

    path: str
    rel: str
    task: str
    version: str
    where: str
    size: int = 0
    nfiles: int = 0
    njobdirs: int = 0
    mtime: float = 0.0
    live_jobs: list = field(default_factory=list)
    recent: bool = False
    mixed: bool = False     # version dir that also has subdirs (e.g. per-fold ML outputs)
    empty: bool = False     # nothing in it: stale version dir (or what is left of its parents)

    @property
    def dataset(self):
        parts = self.where.split("/")
        return parts[1] if len(parts) >= 2 and "__" not in parts[1] else ""

    @property
    def blocked(self):
        if self.live_jobs:
            return "live slurm job(s) " + ",".join(self.live_jobs)
        if self.recent:
            return "modified in the last few minutes"
        return ""


@dataclass
class Group(object):
    """Entries that are decided on together."""

    name: str
    entries: list

    @property
    def size(self):
        return sum(e.size for e in self.entries)

    @property
    def nfiles(self):
        return sum(e.nfiles for e in self.entries)

    @property
    def newest(self):
        return max(e.mtime for e in self.entries)

    @property
    def oldest(self):
        return min(e.mtime for e in self.entries)

    @property
    def unversioned(self):
        return all(e.task in UNVERSIONED_TASKS for e in self.entries)

    @property
    def test_like(self):
        return not self.unversioned and bool(TEST_RE.search(self.name))

    @property
    def deletable(self):
        return [e for e in self.entries if not e.blocked]

    @property
    def empty(self):
        return all(e.empty for e in self.entries)

    @property
    def nempty(self):
        return sum(e.empty for e in self.entries)


def is_payload_dir(name):
    return name.startswith("job_") or name == "cf_wlcg_cache"


def safe_listdir(be, path):
    try:
        return be.listdir(path)
    except OSError as exc:
        log.warning("cannot read %s: %s", path, exc)
        return None


def find_version_dirs(be, start):
    """
    Walks below the Dirent *start* and returns ([(version_dirent, has_subdirs)], [empty_dirents]).
    A version dir holds files and/or job dirs, optionally also subdirectories (e.g. per-fold ML
    outputs) which then belong to it. Empty dirs are ambiguous (stale version dir or just a
    leftover parent) and are returned separately.
    """
    versions, empties = [], []
    frontier = [start]
    while frontier:
        listings = be.map(lambda d: safe_listdir(be, d.path), frontier)
        nxt = []
        for d, kids in zip(frontier, listings):
            if kids is None:
                continue
            subdirs = [k for k in kids if k.real_dir]
            others = [k for k in kids if not k.real_dir]
            payload = [k for k in subdirs if is_payload_dir(k.name)]
            structural = [k for k in subdirs if not is_payload_dir(k.name)]
            if d.path != start.path:
                if not kids:
                    empties.append(d)
                    continue
                if others or payload:
                    versions.append((d, bool(structural)))
                    if structural:
                        log.debug("version dir with subdirs, kept as one unit: %s", d.path)
                    continue
            nxt.extend(structural)
        frontier = nxt
    return versions, empties


def disk_usage(be, top):
    """Returns (size, number of files, newest mtime) below the Dirent *top*, not following links."""
    total, nfiles, newest = 0, 0, top.mtime
    stack = [top.path]
    while stack:
        d = stack.pop()
        kids = safe_listdir(be, d)
        for k in kids or []:
            total += k.size
            newest = max(newest, k.mtime)
            if k.real_dir:
                stack.append(k.path)
            else:
                nfiles += 1
    return total, nfiles, newest


def read_job_ids(path):
    """Slurm job ids law recorded for this version dir (slurm_jobs_*.json, always a local file)."""
    ids = []
    try:
        names = [n for n in os.listdir(path) if n.startswith("slurm_jobs_") and n.endswith(".json")]
    except OSError:
        return ids
    for name in names:
        try:
            with open(os.path.join(path, name)) as f:
                data = json.load(f)
        except (OSError, ValueError):
            continue
        for job in (data.get("jobs") or {}).values():
            job_id = job.get("job_id") if isinstance(job, dict) else None
            if job_id not in (None, "", -1):
                ids.append(str(job_id))
    return ids


def live_slurm_jobs(user):
    """Set of job ids still in the queue for *user*, or None if squeue is unavailable."""
    try:
        res = subprocess.run(
            ["squeue", "-u", user, "-h", "-o", "%i"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        log.warning("squeue not usable (%s), cannot protect dirs of running slurm jobs", exc)
        return None
    if res.returncode != 0:
        log.warning("squeue failed (%s), cannot protect dirs of running slurm jobs", res.stderr.strip())
        return None
    return {line.strip().split("_")[0] for line in res.stdout.splitlines() if line.strip()}


def make_entry(be, dirent, mixed, live_jobs, recent_min, local_store, empty=False):
    rel = be.relpath(dirent.path)
    parts = rel.split("/")
    ti = next((i for i, p in enumerate(parts) if TASK_RE.match(p)), 0)
    size, nfiles, mtime = (0, 0, dirent.mtime) if empty else disk_usage(be, dirent)
    njobdirs = 0
    if not be.remote:
        try:
            njobdirs = sum(1 for n in os.listdir(dirent.path) if n.startswith("job_"))
        except OSError:
            pass
    entry = Entry(
        path=dirent.path,
        rel=rel,
        task=parts[ti],
        version=parts[-1],
        where="/".join(parts[ti + 1:-1]),
        size=size,
        nfiles=nfiles,
        njobdirs=njobdirs,
        mtime=mtime,
        recent=(time.time() - mtime) < recent_min * 60,
        mixed=mixed,
        empty=empty,
    )
    if live_jobs:
        # the recorded job ids always live in the local store, also for remote outputs
        local_dir = os.path.join(local_store, rel) if be.remote else dirent.path
        entry.live_jobs = sorted(set(read_job_ids(local_dir)) & live_jobs)
    return entry


def find_task_dirs(be):
    """Task dirs below the root, also one level down if the root holds analysis dirs."""
    top = be.listdir(be.root)
    tasks = []
    for k in sorted(top, key=lambda k: k.name):
        if not k.real_dir:
            continue
        if TASK_RE.match(k.name):
            tasks.append(k)
        else:
            sub = safe_listdir(be, k.path) or []
            tasks.extend(s for s in sorted(sub, key=lambda s: s.name) if s.real_dir and TASK_RE.match(s.name))
    return tasks


def scan(be, tasks, skip_unversioned, live_jobs, recent_min, local_store):
    entries = []
    task_dirs = find_task_dirs(be)
    for t in task_dirs:
        if tasks and not any(fnmatch.fnmatch(t.name, p) for p in tasks):
            continue
        if skip_unversioned and t.name in UNVERSIONED_TASKS:
            log.debug("skipping task without versions: %s", t.name)
            continue
        if be.remote:
            log.info("scanning %s ...", t.name)
        versions, empty = find_version_dirs(be, t)
        found = [(v, mixed, False) for v, mixed in versions] + [(d, False, True) for d in empty]
        entries.extend(be.map(lambda f: make_entry(be, f[0], f[1], live_jobs, recent_min, local_store, f[2]), found))
    return entries, {t.path for t in task_dirs}


# ---------------------------------------------------------------------------------------------
# presentation and questions
# ---------------------------------------------------------------------------------------------

def build_groups(entries, by, sort):
    groups = {}
    for e in entries:
        if e.task in UNVERSIONED_TASKS:
            key = e.task + " (no version)" if by == "version" else e.rel
        elif e.empty and by == "version" and PARENT_RE.search(e.version):
            key = LEFTOVER_PARENTS
        else:
            key = e.version if by == "version" else e.rel
        groups.setdefault(key, []).append(e)
    groups = [Group(name, es) for name, es in groups.items()]
    if sort == "size":
        groups.sort(key=lambda g: -g.size)
    elif sort == "name":
        groups.sort(key=lambda g: g.name)
    else:
        groups.sort(key=lambda g: g.newest)
    return groups


def print_task_table(entries):
    """One line per columnflow task found in the store, so it is visible what is covered."""
    by_task = {}
    for e in entries:
        by_task.setdefault(e.task, []).append(e)
    width = max([len(t) for t in by_task] + [4])
    print(paint("{:<{w}}  {:>8}  {:>5}  {:>7}".format("TASK", "VERSIONS", "DIRS", "SIZE", w=width), "bold"))
    for task, es in sorted(by_task.items()):
        size = sum(e.size for e in es)
        empty = all(e.empty for e in es)
        nver = "-" if task in UNVERSIONED_TASKS else str(len({e.version for e in es if not (e.empty and PARENT_RE.search(e.version))}))
        print("{}  {:>8}  {:>5}  {}".format(
            paint("{:<{w}}".format(task, w=width), "cyan"), nver, len(es),
            paint("{:>7}".format(size_text(size, empty)), size_style(size, empty))))
    print()


def print_table(groups):
    width = max([len(g.name) for g in groups] + [7])
    head = "{:>3}  {:<{w}}  {:>4}  {:>7}  {:<16}  {}".format("#", "VERSION", "DIRS", "SIZE", "LAST MODIFIED", "FLAGS", w=width)
    print(paint(head, "bold"))
    for i, g in enumerate(groups, 1):
        flags = []
        if g.test_like:
            flags.append(paint("test-like", "magenta"))
        if g.unversioned:
            flags.append(paint("no-version", "dim"))
        if any(e.mixed for e in g.entries):
            flags.append(paint("has-subdirs", "dim"))
        if g.nempty and not g.empty:
            flags.append(paint("incl.{}-empty".format(g.nempty), "dim"))
        blocked = [e for e in g.entries if e.blocked]
        if blocked:
            flags.append(paint("protected:{}".format(len(blocked)), "red"))
        print("{:>3}  {}  {:>4}  {}  {:<16}  {}".format(
            i,
            paint("{:<{w}}".format(g.name, w=width), "cyan"),
            len(g.entries),
            paint("{:>7}".format(size_text(g.size, g.empty)), size_style(g.size, g.empty)),
            fmt_time(g.newest),
            " ".join(flags),
        ))
    print(paint("{} version(s), {} total".format(len(groups), human_size(sum(g.size for g in groups))), "dim"))


def show_group(i, n, g):
    now = time.time()
    if g.test_like:
        hint = paint("TEST-LIKE", "magenta", "bold")
    elif g.name == LEFTOVER_PARENTS:
        hint = paint("folders left over after their version was removed", "dim")
    elif g.empty:
        hint = paint("nothing in it: stale version dir", "dim")
    elif g.unversioned:
        hint = paint("task without versions, recreated on demand", "dim")
    else:
        hint = paint("no test pattern in the name", "dim")
    tasks = sorted({e.task for e in g.entries})
    datasets = sorted({e.dataset for e in g.entries if e.dataset})
    print()
    print("{} version {}  {}".format(paint("[{}/{}]".format(i, n), "bold"), paint(g.name, "bold", "cyan"), hint))
    print("      size {}  |  {} dir(s)  |  {} file(s)  |  {}".format(
        paint(size_text(g.size, g.empty), size_style(g.size, g.empty)), len(g.entries), g.nfiles, ", ".join(tasks)))
    print("      last modified {} ({} ago), oldest {}".format(
        fmt_time(g.newest), human_age(now - g.newest), fmt_time(g.oldest)))
    if datasets:
        shown = ", ".join(datasets[:3]) + (" (+{} more)".format(len(datasets) - 3) if len(datasets) > 3 else "")
        print("      datasets: " + shown)
    blocked = [e for e in g.entries if e.blocked]
    if blocked:
        print(paint("      protected: {} of {} dir(s): {}".format(len(blocked), len(g.entries), blocked[0].blocked), "red"))


def show_dirs(g, limit=40):
    now = time.time()
    for e in g.entries[:limit]:
        print("        {}  {}  {}{}{}".format(
            paint("{:>7}".format(size_text(e.size, e.empty)), size_style(e.size, e.empty)),
            paint("{:>6}".format(human_age(now - e.mtime)), "grey"),
            e.rel,
            paint("  [has subdirs]", "dim") if e.mixed else "",
            paint("  [{}]".format(e.blocked), "red") if e.blocked else "",
        ))
    if len(g.entries) > limit:
        print(paint("        ... {} more".format(len(g.entries) - limit), "dim"))


def ask(prompt):
    try:
        return input(prompt).strip().lower()
    except EOFError:
        return "q"


def confirm(prompt):
    """y/yes/d/delete -> True, Enter/n/no/q/k -> False, anything else is asked again."""
    while True:
        ans = ask(prompt)
        if ans in ("y", "yes", "d", "delete"):
            return True
        if ans in ("", "n", "no", "q", "k", "keep"):
            return False
        print(paint("      please answer y or n", "yellow"))


def interactive_select(groups):
    """Returns the groups marked for deletion."""
    marked = []
    auto_tests = False
    for i, g in enumerate(groups, 1):
        show_group(i, len(groups), g)
        if not g.deletable:
            log.warning("every dir of this version is protected -> kept")
            continue
        if auto_tests and g.test_like:
            log.info("auto-marked for deletion (test-like, you chose 't')")
            marked.append(g)
            continue
        while True:
            print("      " + paint("[k]", "green") + "eep  " + paint("[d]", "red") + "elete  " +
                  paint("[l]", "cyan") + "ist dirs  " + paint("[t]", "magenta") + " delete this+all remaining test-like  " +
                  paint("[s]", "yellow") + "kip rest  " + paint("[q]", "yellow") + "uit")
            ans = ask("      (Enter = keep) > ")
            if ans in ("", "k"):
                log.info("keep %s", g.name)
                break
            if ans == "d":
                marked.append(g)
                log.info("marked for deletion: %s", g.name)
                break
            if ans == "l":
                show_dirs(g)
                continue
            if ans == "t":
                auto_tests = True
                if g.test_like:
                    marked.append(g)
                    log.info("marked for deletion: %s (and all remaining test-like versions)", g.name)
                    break
                log.info("will auto-mark remaining test-like versions, this one is not test-like")
                continue
            if ans == "s":
                log.info("keeping everything that is left")
                return marked
            if ans == "q":
                log.info("stopped asking, reviewing what was marked so far")
                return marked
            print(paint("      please answer k, d, l, t, s or q", "yellow"))
    return marked


def show_empty_paths(entries, limit=20):
    """Full path of each empty dir (a url for the remote store), oldest first."""
    empties = sorted((e for e in entries if e.empty), key=lambda e: e.mtime)
    if not empties:
        return
    print()
    log.info("%d empty director%s (stale version dirs, offered like any other version):", len(empties), "y" if len(empties) == 1 else "ies")
    for e in empties[:limit]:
        print("        {}  {}".format(fmt_time(e.mtime), e.path))
    if len(empties) > limit:
        print(paint("        ... {} more".format(len(empties) - limit), "dim"))


# ---------------------------------------------------------------------------------------------
# deletion
# ---------------------------------------------------------------------------------------------

def prune_empty_parents(be, path, keep):
    """Removes parents that became empty, never a task dir (in *keep*) or anything above it."""
    parent = be.dirname(path)
    while be.inside(parent) and parent not in keep:
        try:
            if be.listdir(parent):
                break
            be.rmdir(parent)
        except OSError:
            break
        log.debug("removed empty parent %s", parent)
        parent = be.dirname(parent)


def delete_dirs(be, items, dry_run, keep):
    """*items* are (path, size, only_if_empty). Returns (freed bytes, number of failures)."""
    freed, failed = 0, 0
    for path, size, only_if_empty in items:
        if not be.exists(path):
            log.debug("already gone: %s", path)
            continue
        if not be.is_dir(path) or not be.inside(path):
            log.error("refusing to delete %s (not a plain dir inside %s)", path, be.root)
            failed += 1
            continue
        rel = be.relpath(path)
        if dry_run:
            log.info("%s would delete %s (%s)", paint("[dry-run]", "yellow"), rel, size_text(size, only_if_empty))
            continue
        try:
            if only_if_empty:
                if be.listdir(path):
                    log.warning("not empty any more, kept: %s", rel)
                    continue
                be.rmdir(path)
            else:
                be.delete_tree(path)
            if be.exists(path):
                raise OSError("still exists")
        except OSError as exc:
            failed += 1
            log.error("could not fully delete %s: %s", rel, exc)
            continue
        freed += size
        log.info("deleted %s (%s)", rel, size_text(size, only_if_empty))
        prune_empty_parents(be, path, keep)
    return freed, failed


# ---------------------------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------------------------

def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Interactively clean old versions from the columnflow store (local or remote).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("Examples:")[1] if "Examples:" in __doc__ else None,
    )
    p.add_argument("--remote", action="store_true", help="scan the remote webdav store instead of the local one")
    p.add_argument("--root", help="store root, default: {} (local) or {} (--remote); a davs:// url implies --remote".format(DEFAULT_ROOT, DEFAULT_REMOTE_ROOT))
    p.add_argument("--local-store", default=DEFAULT_LOCAL_STORE, help="local store, used to find the slurm job ids of remote versions, default: %(default)s")
    p.add_argument("--threads", type=int, default=8, help="parallel requests on the remote store, default: %(default)s")
    p.add_argument("--list", action="store_true", help="only print what is there and exit")
    p.add_argument("--dry-run", action="store_true", help="ask as usual but never delete anything")
    p.add_argument("--yes", action="store_true", help="skip the final confirmation")
    p.add_argument("--by", choices=("version", "path"), default="version", help="decide per version name (default) or per directory")
    p.add_argument("--sort", choices=("age", "size", "name"), default="age", help="oldest first (default), biggest first, or by name")
    p.add_argument("--match", action="append", metavar="GLOB", help="only versions matching this glob, repeatable")
    p.add_argument("--task", action="append", metavar="GLOB", help="only these tasks, e.g. cf.CalibrateEvents, repeatable")
    p.add_argument("--older-than", type=float, metavar="DAYS", help="only versions whose newest file is older than this")
    p.add_argument("--tests-only", action="store_true", help="only versions whose name looks like a test")
    p.add_argument("--empty-only", action="store_true", help="only empty directories (stale versions and leftover parents)")
    p.add_argument("--skip-unversioned", action="store_true", help="leave out tasks without a version level (bundles, LFN lists, sandboxes)")
    p.add_argument("--all-tasks", action="store_true", help=argparse.SUPPRESS)  # old name, now the default
    p.add_argument("--recent-min", type=float, default=30.0, help="protect dirs modified in the last N minutes, default: %(default)s")
    p.add_argument("--force", action="store_true", help="also offer recently modified dirs (never dirs of live slurm jobs)")
    p.add_argument("--color", choices=("auto", "always", "never"), default="auto")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def make_backend(args):
    remote = args.remote or "://" in (args.root or "")
    if not remote:
        root = os.path.realpath(args.root or DEFAULT_ROOT)
        if not os.path.isdir(root) or root in ("/", os.path.expanduser("~")):
            log.error("%s is not a usable store root", root)
            return None
        return LocalBackend(root)
    root = (args.root or DEFAULT_REMOTE_ROOT).rstrip("/")
    if not re.search(r"/store/user/[^/]+/.+", root):
        log.error("refusing remote root %s: it must be a directory below /store/user/<user>/", root)
        return None
    if root.startswith("root://"):
        log.warning("root:// goes through XrdCl (flaky door, fork deadlocks), davs:// is recommended")
    try:
        return RemoteBackend(root, args.threads)
    except ImportError as exc:
        log.error("python module gfal2 is not importable (%s): run this from your multilepton_venv shell", exc)
        return None


def main(argv=None):
    args = parse_args(argv)
    Style.enabled = args.color == "always" or (
        args.color == "auto" and sys.stdout.isatty() and "NO_COLOR" not in os.environ
    )
    setup_logging(args.verbose)

    be = make_backend(args)
    if be is None:
        return 1

    user = getpass.getuser()
    log.info("scanning the %s store %s", paint("REMOTE" if be.remote else "local", "bold"), paint(be.root, "bold"))
    live = live_slurm_jobs(user)
    if live is not None:
        log.info("%d slurm job(s) of %s currently in the queue", len(live), user)
    try:
        entries, keep = scan(be, args.task, args.skip_unversioned, live, args.recent_min, args.local_store)
    except OSError as exc:
        log.error("cannot read %s: %s", be.root, exc)
        if be.remote:
            log.error("is your grid proxy valid? (voms-proxy-info -all)")
        return 1
    if args.force:
        for e in entries:
            e.recent = False

    now = time.time()
    if args.older_than is not None:
        entries = [e for e in entries if (now - e.mtime) > args.older_than * 86400]
    if args.match:
        entries = [e for e in entries if any(fnmatch.fnmatch(e.version, m) for m in args.match)]
    if args.tests_only:
        entries = [e for e in entries if TEST_RE.search(e.version)]
    if args.empty_only:
        entries = [e for e in entries if e.empty]

    groups = build_groups(entries, args.by, args.sort)
    nempty = sum(e.empty for e in entries)
    log.info("found %d version dir(s)%s in %d group(s), %s", len(entries), " ({} empty)".format(nempty) if nempty else "",
             len(groups), human_size(sum(e.size for e in entries)))
    if groups:
        print_task_table(entries)
        print_table(groups)
    if args.list:
        show_empty_paths(entries)
        return 0

    marked = interactive_select(groups) if groups else []
    items = [(e.path, e.size, e.empty) for g in marked for e in g.deletable]
    if not items:
        log.info("nothing to delete")
        return 0

    print()
    log.info(paint("summary", "bold"))
    for g in marked:
        print("      {}  {}  {} dir(s)".format(
            paint("{:>7}".format(size_text(sum(e.size for e in g.deletable), g.empty)), "red"), paint(g.name, "cyan"), len(g.deletable)))
    total = sum(size for _, size, _ in items)
    if be.remote:
        log.warning("REMOTE store: there is no trash, deletion is irreversible")
    log.warning("%d dir(s) will be removed for good, %s", len(items), paint(human_size(total), "bold"))

    if args.dry_run:
        log.info("dry run, nothing will be deleted")
    elif not args.yes and not confirm("      " + paint("delete now? [y/N] > ", "red")):
        log.info("aborted, nothing was deleted")
        return 0

    freed, failed = delete_dirs(be, items, args.dry_run, keep)
    if not args.dry_run:
        log.info("freed %s%s", paint(human_size(freed), "bold", "green"),
                 paint(", {} failure(s)".format(failed), "red") if failed else "")
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print()
        log.warning("interrupted")
        sys.exit(130)
