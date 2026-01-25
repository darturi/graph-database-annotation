#!/usr/bin/env python3
"""
Create a small toy Kuzu database for testing the KuzuExecutor.
Kuzu is embedded - no Docker needed, just runs locally.
"""

import kuzu
import shutil
from pathlib import Path

# Configuration - use relative path for Docker compatibility
SCRIPT_DIR = Path(__file__).parent.resolve()
TOY_DB_PATH = SCRIPT_DIR / "kuzu_databases" / "toy_test"

def create_toy_database():
    """Create a small test database with a simple tree structure."""

    # Remove existing database if it exists
    if TOY_DB_PATH.exists():
        if TOY_DB_PATH.is_dir():
            shutil.rmtree(TOY_DB_PATH)
        else:
            TOY_DB_PATH.unlink()

    # Ensure parent directory exists (but not the DB path itself - Kuzu creates it)
    TOY_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Create database and connection
    db = kuzu.Database(str(TOY_DB_PATH))
    conn = kuzu.Connection(db)

    print("Creating schema...")

    # Create node table
    conn.execute("""
        CREATE NODE TABLE TreeNode(
            id INT64,
            name STRING,
            integer_id INT64,
            upper_bound INT64,
            height INT64,
            depth INT64,
            PRIMARY KEY(id)
        )
    """)

    # Create relationship table
    conn.execute("""
        CREATE REL TABLE HAS_CHILD(
            FROM TreeNode TO TreeNode
        )
    """)

    print("Inserting nodes...")

    # Create a simple tree:
    #
    #           1 (root)
    #          /|\
    #         2 3 4
    #        /|   |
    #       5 6   7
    #       |
    #       8
    #
    # Integer IDs assigned in DFS order: 1,2,5,8,6,3,4,7
    # Upper bounds: node's max descendant integer_id

    nodes = [
        # (id, name, integer_id, upper_bound, height, depth)
        (1, "root", 1, 8, 3, 0),
        (2, "child_1", 2, 5, 2, 1),
        (3, "child_2", 6, 6, 0, 1),
        (4, "child_3", 7, 8, 1, 1),
        (5, "grandchild_1", 3, 4, 1, 2),
        (6, "grandchild_2", 5, 5, 0, 2),
        (7, "grandchild_3", 8, 8, 0, 2),
        (8, "great_grandchild", 4, 4, 0, 3),
    ]

    for node in nodes:
        conn.execute(f"""
            CREATE (n:TreeNode {{
                id: {node[0]},
                name: '{node[1]}',
                integer_id: {node[2]},
                upper_bound: {node[3]},
                height: {node[4]},
                depth: {node[5]}
            }})
        """)

    print("Inserting edges...")

    # Edges: child -> parent (HAS_CHILD points to parent)
    edges = [
        (2, 1),  # child_1 -> root
        (3, 1),  # child_2 -> root
        (4, 1),  # child_3 -> root
        (5, 2),  # grandchild_1 -> child_1
        (6, 2),  # grandchild_2 -> child_1
        (7, 4),  # grandchild_3 -> child_3
        (8, 5),  # great_grandchild -> grandchild_1
    ]

    for child_id, parent_id in edges:
        conn.execute(f"""
            MATCH (child:TreeNode {{id: {child_id}}}), (parent:TreeNode {{id: {parent_id}}})
            CREATE (child)-[:HAS_CHILD]->(parent)
        """)

    print("\nVerifying database...")

    # Count nodes
    result = conn.execute("MATCH (n:TreeNode) RETURN count(n) AS count")
    count = result.get_next()[0]
    print(f"  Nodes: {count}")

    # Count edges
    result = conn.execute("MATCH ()-[r:HAS_CHILD]->() RETURN count(r) AS count")
    count = result.get_next()[0]
    print(f"  Edges: {count}")

    # Show tree structure
    print("\nTree structure:")
    result = conn.execute("""
        MATCH (n:TreeNode)
        RETURN n.name, n.integer_id, n.upper_bound, n.depth
        ORDER BY n.integer_id
    """)

    while result.has_next():
        row = result.get_next()
        indent = "  " * row[3]
        print(f"  {indent}{row[0]} (int_id={row[1]}, upper={row[2]})")

    print(f"\n✓ Toy database created at: {TOY_DB_PATH}")
    print("\nTest with KuzuExecutor:")
    print(f"""
from logic.ExecutorDefinitions import KuzuExecutor

ke = KuzuExecutor("{TOY_DB_PATH}")

# Find all nodes
time_ms, results = ke.execute_query("MATCH (n:TreeNode) RETURN n.name, n.integer_id")
print(f"Query took {{time_ms:.2f}}ms")
for r in results:
    print(r)

# Find descendants of root using integer range
time_ms, results = ke.execute_query('''
    MATCH (root:TreeNode {{integer_id: 1}})
    MATCH (n:TreeNode)
    WHERE n.integer_id > 1 AND n.integer_id <= root.upper_bound
    RETURN n.name
''')
print(f"Descendants: {{[r[0] for r in results]}}")

# Find descendants using path traversal
time_ms, results = ke.execute_query('''
    MATCH (n:TreeNode)-[:HAS_CHILD*1..]->(root:TreeNode {{integer_id: 1}})
    RETURN n.name
''')
print(f"Descendants (traversal): {{[r[0] for r in results]}}")
""")


if __name__ == "__main__":
    create_toy_database()
