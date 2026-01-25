#!/usr/bin/env python3
"""
Setup Kuzu databases for artificial trees and LDBC data.
Kuzu is an embedded database - no Docker/server needed.
"""

import kuzu
import os
from pathlib import Path

# Configuration - use relative paths for Docker compatibility
SCRIPT_DIR = Path(__file__).parent.resolve()
KUZU_DB_BASE = SCRIPT_DIR / "kuzu_databases"
ARTIFICIAL_TREES_PATH = SCRIPT_DIR / "data" / "artificial_trees"


def create_ir_database(db_path: Path, nodes_csv: Path, edges_csv: Path):
    """Create a Kuzu database with integer-range indexed data."""
    db_path.mkdir(parents=True, exist_ok=True)

    db = kuzu.Database(str(db_path))
    conn = kuzu.Connection(db)

    # Create schema
    conn.execute("""
        CREATE NODE TABLE IF NOT EXISTS TreeNode(
            id INT64,
            type STRING,
            integer_id INT64,
            upper_bound INT64,
            height INT64,
            depth INT64,
            PRIMARY KEY(id)
        )
    """)

    conn.execute("""
        CREATE REL TABLE IF NOT EXISTS HAS_CHILD(
            FROM TreeNode TO TreeNode
        )
    """)

    # Load data
    conn.execute(f"COPY TreeNode FROM '{nodes_csv}' (HEADER=true)")
    conn.execute(f"COPY HAS_CHILD FROM '{edges_csv}' (HEADER=true)")

    # Verify
    result = conn.execute("MATCH (n:TreeNode) RETURN count(n) AS count")
    count = result.get_next()[0]

    return count


def create_s_database(db_path: Path, nodes_csv: Path, edges_csv: Path):
    """Create a Kuzu database with string indexed data."""
    db_path.mkdir(parents=True, exist_ok=True)

    db = kuzu.Database(str(db_path))
    conn = kuzu.Connection(db)

    # Create schema
    conn.execute("""
        CREATE NODE TABLE IF NOT EXISTS TreeNode(
            id INT64,
            type STRING,
            string_id STRING,
            height INT64,
            depth INT64,
            PRIMARY KEY(id)
        )
    """)

    conn.execute("""
        CREATE REL TABLE IF NOT EXISTS HAS_CHILD(
            FROM TreeNode TO TreeNode
        )
    """)

    # Load data
    conn.execute(f"COPY TreeNode FROM '{nodes_csv}' (HEADER=true)")
    conn.execute(f"COPY HAS_CHILD FROM '{edges_csv}' (HEADER=true)")

    # Verify
    result = conn.execute("MATCH (n:TreeNode) RETURN count(n) AS count")
    count = result.get_next()[0]

    return count


def setup_all_artificial_trees():
    """Set up Kuzu databases for all artificial tree variants."""
    from tqdm import tqdm

    # Find all unique graph names
    graph_names = set()
    for f in ARTIFICIAL_TREES_PATH.glob("*_nodes_*.csv"):
        # Extract base name (e.g., "truebase_100" from "truebase_100_nodes_ir.csv")
        name = f.stem.replace("_nodes_ir", "").replace("_nodes_s", "")
        graph_names.add(name)

    graph_names = sorted(graph_names)

    print(f"Found {len(graph_names)} graph configurations")
    print(f"Database directory: {KUZU_DB_BASE}")

    for graph_name in tqdm(graph_names, desc="Setting up Kuzu databases"):
        # IR version
        ir_nodes = ARTIFICIAL_TREES_PATH / f"{graph_name}_nodes_ir.csv"
        ir_edges = ARTIFICIAL_TREES_PATH / f"{graph_name}_edges_ir.csv"

        if ir_nodes.exists() and ir_edges.exists():
            db_path = KUZU_DB_BASE / f"{graph_name}_ir"
            try:
                count = create_ir_database(db_path, ir_nodes, ir_edges)
                # tqdm.write(f"  ✓ {graph_name}_ir: {count} nodes")
            except Exception as e:
                tqdm.write(f"  ✗ {graph_name}_ir: {e}")

        # String version
        s_nodes = ARTIFICIAL_TREES_PATH / f"{graph_name}_nodes_s.csv"
        s_edges = ARTIFICIAL_TREES_PATH / f"{graph_name}_edges_s.csv"

        if s_nodes.exists() and s_edges.exists():
            db_path = KUZU_DB_BASE / f"{graph_name}_s"
            try:
                count = create_s_database(db_path, s_nodes, s_edges)
                # tqdm.write(f"  ✓ {graph_name}_s: {count} nodes")
            except Exception as e:
                tqdm.write(f"  ✗ {graph_name}_s: {e}")

    print("\nKuzu database setup complete!")
    print(f"Databases stored in: {KUZU_DB_BASE}")


if __name__ == "__main__":
    setup_all_artificial_trees()
