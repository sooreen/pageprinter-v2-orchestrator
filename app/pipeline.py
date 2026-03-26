"""Pipeline definition: agent sequence with gates."""

# Each step: agent name, whether it has a human gate, gate name
PIPELINE = [
    # data-input (3.1.2) is built into the orchestrator — start_pipeline() runs the parser
    {"agent": "idea-search", "gate": "idea_approval"},
    {"agent": "competitor-search", "gate": "competitor_approval"},
    {"agent": "competitor-parser", "gate": None},
    {"agent": "competitor-analyzer", "gate": "structure_approval"},
    {"agent": "data-aggregator", "gate": None},
    {"agent": "seed-generator", "gate": "seed_approval"},
    {"agent": "wordstat-parser", "gate": None},
    {"agent": "semantic-cleanup", "gate": None},
    {"agent": "semantic-clustering", "gate": "cluster_approval"},
    {"agent": "url-tree-builder", "gate": "url_approval"},
    {"agent": "page-structure", "gate": None},
    # Design branch (starts after data-aggregator)
    {"agent": "design-refs", "gate": "design_approval"},
    # Pass 2: Design system
    {"agent": "components-registry", "gate": None},
    {"agent": "style-guide", "gate": None},
    # Pass 3: Per-page (parallelizable, but sequential in this MVP)
    {"agent": "text-brief", "gate": None},
    {"agent": "text-writer", "gate": None},
    {"agent": "text-validator", "gate": None},
    {"agent": "module-creator", "gate": None},
    {"agent": "page-assembler", "gate": None},
    {"agent": "internal-linking", "gate": None},
    {"agent": "final-checker", "gate": None},
    {"agent": "publisher", "gate": None},
]


def get_pipeline() -> list[dict]:
    """Return the pipeline definition."""
    return PIPELINE
