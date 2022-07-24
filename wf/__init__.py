"""
Assemble and sort some COVID reads...
"""

import subprocess
from pathlib import Path
from typing import List, Optional

from latch import small_task, workflow
from latch.types import LatchDir, LatchFile
from latch.types.utils import _is_valid_url

from .docs import UNHOST_DOCS


def file_glob(
    pattern: str, remote_directory: str, target_dir: Optional[Path] = None
) -> List[LatchFile]:
    """Constructs a list of LatchFiles from a glob pattern.
    Convenient utility for passing collections of files between tasks. See
    [nextflow's channels](https://www.nextflow.io/docs/latest/channel.html) or
    [snakemake's wildcards](https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html#wildcards).
    for similar functionality in other orchestration tools.
    The remote location of each constructed LatchFile will be consructed by
    appending the file name returned by the pattern to the directory
    represented by the `remote_directory`.
    Args:
        pattern: A glob pattern to match a set of files, eg. '\*.py'. Will
            resolve paths with respect to the working directory of the caller.
        remote_directory: A valid latch URL pointing to a directory, eg.
            latch:///foo. This _must_ be a directory and not a file.
        target_dir: An optional Path object to define an alternate working
            directory for path resolution
    Returns:
        A list of instantiated LatchFile objects.
    Intended Use: ::
        @small_task
        def task():
            ...
            return file_glob("*.fastq.gz", "latch:///fastqc_outputs")
    """

    if not _is_valid_url(remote_directory):
        return []

    if target_dir is None:
        wd = Path.cwd()
    else:
        wd = target_dir
    matched = sorted(wd.glob(pattern))

    return [LatchFile(str(file), remote_directory + file.name) for file in matched]


@small_task
def fastp(
    read1: LatchFile,
    read2: LatchFile,
    sample_name: str,
) -> List[LatchFile]:
    """Adapter removal and read trimming with fastp"""

    _fastp_cmd = [
        "/root/fastp",
        "--in1",
        read1.local_path,
        "--in2",
        read2.local_path,
        "--out1",
        f"{sample_name}_1.trim.fastq.gz",
        "--out2",
        f"{sample_name}_2.trim.fastq.gz",
        "--json",
        f"{sample_name}.fastp.json",
        "--html",
        f"{sample_name}.fastp.html",
        "--thread",
        "4",
        "--detect_adapter_for_pe",
    ]

    subprocess.run(_fastp_cmd)

    return file_glob(f"{sample_name}*", "latch:///fastp_outputs/")


@workflow(UNHOST_DOCS)
def unhost(
    read1: LatchFile, read2: LatchFile, sample_name: str = "unhost_sample"
) -> List[LatchFile]:
    """A Workflow for fastq preprocessing and host read removal

    UnHost
    ---

    A fastq preprocessing and host read removal workflow
    for short-read metagenomics data.
    """
    return fastp(read1=read1, read2=read2, sample_name=sample_name)
