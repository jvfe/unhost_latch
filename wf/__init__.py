"""
Assemble and sort some COVID reads...
"""

import subprocess
from pathlib import Path
from typing import List

from latch import small_task, workflow
from latch.types import LatchDir, LatchFile, file_glob

from .docs import UNHOST_DOCS


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
