import subprocess
from pathlib import Path
from typing import List, Optional, Union

from latch import large_task, small_task, workflow, workflow_reference
from latch.resources.launch_plan import LaunchPlan
from latch.resources.tasks import cached_large_task
from latch.types import LatchDir, LatchFile

from .docs import UNHOST_DOCS

CACHE_VERSION = "0.1.0"


@small_task
def fastp(
    read_dir: LatchDir,
    sample_name: str,
) -> LatchDir:
    """Adapter removal and read trimming with fastp"""

    output_dir = Path("fastp_results").resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    output_prefix = f"{str(output_dir)}/{sample_name}"

    read1 = f"{read_dir.local_path}/{sample_name}_1_filtered.fastq"
    read2 = f"{read_dir.local_path}/{sample_name}_2_filtered.fastq"

    _fastp_cmd = [
        "/root/fastp",
        "--in1",
        read1,
        "--in2",
        read2,
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

    return LatchDir(str(output_dir), f"latch:///unhost_{sample_name}_trimmed")


@cached_large_task(CACHE_VERSION)
def build_bowtie_index(
    host_genome: LatchFile, sample_name: str, host_name: str
) -> LatchDir:

    output_dir_name = f"{sample_name}_btidx"
    output_dir = Path(output_dir_name).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    host_name_clean = host_name.replace(" ", "_").lower()

    _bt_idx_cmd = [
        "bowtie2/bowtie2-build",
        host_genome.local_path,
        f"{str(output_dir)}/{host_name_clean}",
        "--threads",
        "31",
    ]

    subprocess.run(_bt_idx_cmd)

    return LatchDir(str(output_dir), f"latch:///unhost_{sample_name}_host_idx")


# @cached_large_task(CACHE_VERSION)
@large_task
def map_to_host(
    host_idx: LatchDir,
    read_dir: LatchDir,
    sample_name: str,
    host_name: str,
) -> LatchDir:

    output_dir_name = f"{sample_name}_bt_unaligned"
    output_dir = Path(output_dir_name).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    host_name_clean = host_name.replace(" ", "_").lower()

    _bt_cmd = [
        "bowtie2/bowtie2",
        "-x",
        f"{host_idx.local_path}/{host_name_clean}",
        "-1",
        f"{read_dir.local_path}/{sample_name}_1.trim.fastq.gz",
        "-2",
        f"{read_dir.local_path}/{sample_name}_2.trim.fastq.gz",
        "--un-conc-gz",
        f"{output_dir}/{sample_name}_unaligned.fastq.gz",
        "--threads",
        "31",
    ]

    subprocess.run(_bt_cmd)

    return LatchDir(str(output_dir), f"latch:///unhost_{sample_name}_unaligned")


@workflow_reference(
    name="HighComplexity",
    version="0.4.0-3b621f",
)
def highcomplexity(
    read1: LatchFile,
    read2: LatchFile,
    sample_name: str,
    contaminants: Optional[LatchFile],
) -> LatchDir:
    ...


@workflow(UNHOST_DOCS)
def unhost(
    read1: LatchFile,
    read2: LatchFile,
    host_genome: LatchFile,
    host_name: str = "host",
    sample_name: str = "unhost_sample",
    contaminants: Optional[LatchFile] = None,
) -> LatchDir:
    """A Workflow for fastq preprocessing and host read removal

    UnHost
    ---

    A FastQ preprocessing and host read removal workflow
    for short-read metagenomics data. It's comprised of:

    - [fastp](https://github.com/OpenGene/fastp) for read trimming, adapter removal
    and other preprocessing.

    - [bowtie2](https://github.com/BenLangmead/bowtie2) for creating an index from the host's
    reference genome and extracting unaligned reads
    from an alignment to said genome.

    ---
    ## References

    Shifu Chen, Yanqing Zhou, Yaru Chen, Jia Gu;
    fastp: an ultra-fast all-in-one FASTQ preprocessor,
    Bioinformatics, Volume 34, Issue 17, 1 September 2018,
    Pages i884–i890, https://doi.org/10.1093/bioinformatics/bty560

    Langmead B, Wilks C., Antonescu V., Charles R. Scaling read
    aligners to hundreds of threads on general-purpose processors.
    Bioinformatics. bty648.
    """

    complexity_filtered = highcomplexity(
        read1=read1, read2=read2, sample_name=sample_name, contaminants=contaminants
    )
    trimmed_data = fastp(read_dir=complexity_filtered, sample_name=sample_name)
    host_idx = build_bowtie_index(
        host_genome=host_genome, sample_name=sample_name, host_name=host_name
    )

    unaligned = map_to_host(
        host_idx=host_idx,
        read_dir=trimmed_data,
        sample_name=sample_name,
        host_name=host_name,
    )

    return unaligned


LaunchPlan(
    unhost,
    "Test Microbiome",
    {
        "read1": LatchFile("s3://latch-public/test-data/4318/SRR579292_1.fastq"),
        "read2": LatchFile("s3://latch-public/test-data/4318/SRR579292_2.fastq"),
        "host_genome": LatchFile(
            "s3://latch-public/test-data/4318/Homo_sapiens.GRCh38.dna_rm.toplevel.fa.gz"
        ),
        "host_name": "homo_sapiens",
        "sample_name": "example_microbiome",
    },
)
