"""
Assemble and sort some COVID reads...
"""

import subprocess
from pathlib import Path

from latch import small_task, workflow
from latch.types import LatchDir, LatchFile

from .docs import UNHOST_DOCS


@small_task
def fastp(
    read1: LatchFile,
    read2: LatchFile,
    sample_name: str,
) -> LatchDir:
    """Adapter removal and read trimming with fastp"""

    output_dir = Path("fastp_results").resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    output_prefix = f"{str(output_dir)}/{sample_name}"

    _fastp_cmd = [
        "/root/fastp",
        "--in1",
        read1.local_path,
        "--in2",
        read2.local_path,
        "--out1",
        f"{output_prefix}_1.trim.fastq.gz",
        "--out2",
        f"{output_prefix}_2.trim.fastq.gz",
        "--json",
        f"{output_prefix}.fastp.json",
        "--html",
        f"{output_prefix}.fastp.html",
        "--thread",
        "4",
        "--detect_adapter_for_pe",
    ]

    subprocess.run(_fastp_cmd)

    return LatchDir(str(output_dir), f"latch:///unhost_{sample_name}")


@workflow(UNHOST_DOCS)
def unhost(
    read1: LatchFile, read2: LatchFile, sample_name: str = "unhost_sample"
) -> LatchDir:
    """A Workflow for fastq preprocessing and host read removal

    UnHost
    ---

    A fastq preprocessing and host read removal workflow
    for short-read metagenomics data.
    """
    return fastp(read1=read1, read2=read2, sample_name=sample_name)
