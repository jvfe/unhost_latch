A Workflow for fastq preprocessing and host read removal

## UnHost

A FastQ preprocessing and host read removal workflow
for short-read metagenomics data. It's comprised of:

- [fastp](https://github.com/OpenGene/fastp) for read trimming, adapter removal and other preprocessing.

- [bowtie2](https://github.com/BenLangmead/bowtie2) for creating an index from the
  host's reference genome and extracting unaligned reads
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
