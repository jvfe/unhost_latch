import subprocess
from pathlib import Path
from typing import List, Union

from latch import cached_large_task, small_task, workflow
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


@cached_large_task("0.1.0")
def build_bowtie_index(
    host_genome: LatchFile, sample_name: str, host_name: str
) -> LatchDir:

    output_dir_name = f"{sample_name}_btidx"
    output_dir = Path(output_dir_name).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    _bt_idx_cmd = [
        "bowtie2/bowtie2-build",
        host_genome.local_path,
        f"{str(output_dir)}/{host_name}",
        "--threads",
        "31",
    ]

    subprocess.run(_bt_idx_cmd)

    return LatchDir(str(output_dir), f"latch:///unhost_{sample_name}_host_idx")


@workflow(UNHOST_DOCS)
def unhost(
    read1: LatchFile,
    read2: LatchFile,
    host_genome: LatchFile,
    host_name: str = "host",
    sample_name: str = "unhost_sample",
) -> List[Union[LatchFile, LatchDir]]:
    """A Workflow for fastq preprocessing and host read removal

    UnHost
    ---

    A fastq preprocessing and host read removal workflow
    for short-read metagenomics data.
    """
    trimmed_data = fastp(read1=read1, read2=read2, sample_name=sample_name)
    host_idx = build_bowtie_index(
        host_genome=host_genome, sample_name=sample_name, host_name=host_name
    )

    return [trimmed_data, host_idx]
