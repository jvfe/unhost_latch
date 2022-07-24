from latch.types import LatchAuthor, LatchMetadata, LatchParameter, LatchRule

UNHOST_DOCS = LatchMetadata(
    display_name="UnHost",
    documentation="https://github.com/jvfe/unhost_latch/blob/main/README.md",
    author=LatchAuthor(
        name="jvfe",
        github="https://github.com/jvfe",
    ),
    repository="https://github.com/jvfe/unhost_latch",
    license="MIT",
)

UNHOST_DOCS.parameters = {
    "read1": LatchParameter(
        display_name="Read 1",
        description="Paired-end read 1 file.",
        section_title="Data",
    ),
    "read2": LatchParameter(
        display_name="Read 2",
        description="Paired-end read 2 file.",
    ),
    "sample_name": LatchParameter(
        display_name="Sample name",
        description="Sample name (will define output file names)",
    ),
}
