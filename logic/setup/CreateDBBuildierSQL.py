import os
from pathlib import Path
from dotenv import load_dotenv

def create_sql(
        graph_name : str,
        vertex_dict : dict,
        edge_dict : dict,
        save_dir : Path,
        docker_data_dir=Path("graph_data")
):
    graph_name = graph_name.lower()

    sql_base = f"""
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM ag_catalog.ag_graph WHERE name = '{graph_name}'
    ) THEN
        PERFORM ag_catalog.drop_graph('{graph_name}', true);
    END IF;
END $$;    

-- create graph
SELECT ag_catalog.create_graph('{graph_name}');
    
-- autovacuum on basic tables off
ALTER TABLE {graph_name}._ag_label_vertex
    SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
ALTER TABLE {graph_name}._ag_label_edge
    SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
    
"""

    for vertex_key, vertex_val in vertex_dict.items():
        sql_base += f"""
-- create vertex label
SELECT ag_catalog.create_vlabel('{graph_name}', '{vertex_key}');

-- autovacuum for the specific label off
ALTER TABLE {graph_name}."{vertex_key}"
    SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);

-- load vertex data
SELECT ag_catalog.load_labels_from_file(
    '{graph_name}',
    '{vertex_key}',
    '/data/prepared/{vertex_val}'
);
"""

    for edge_key, edge_val in edge_dict.items():
        sql_base += f"""
-- create edge label
SELECT ag_catalog.create_elabel('{graph_name}', '{edge_key}');

-- autovacuum for the edge label off
ALTER TABLE {graph_name}."{edge_key}"
    SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);

-- load edge data
SELECT ag_catalog.load_edges_from_file(
    '{graph_name}',
    '{edge_key}',
    '/data/prepared/{edge_val}'
);

"""

    for vertex_name in vertex_dict.keys():
        if "_prepost" in graph_name:
            sql_base += f"""
-- Batch convert string properties to integers for {vertex_name}
DO $$
DECLARE
    batch_size INT := 50000;
    batch_result BIGINT := 1;
    total_converted BIGINT := 0;
BEGIN
    WHILE batch_result > 0 LOOP
        SELECT cnt::bigint INTO batch_result
        FROM ag_catalog.cypher('{graph_name}', $cypher$
            MATCH (n:{vertex_name})
            WHERE NOT exists(n._converted)
            WITH n LIMIT 50000
            SET n.integer_id = toInteger(n.integer_id),
                n.upper_bound = toInteger(n.upper_bound),
                n.height = toInteger(n.height),
                n.depth = toInteger(n.depth),
                n._converted = true
            RETURN count(n)
        $cypher$) AS (cnt agtype);

        total_converted := total_converted + batch_result;
        RAISE NOTICE 'Batch converted % (total: %)', batch_result, total_converted;
    END LOOP;

    RAISE NOTICE '{vertex_name} conversion complete: % nodes', total_converted;

    -- Remove temporary marker in batches
    batch_result := 1;
    WHILE batch_result > 0 LOOP
        SELECT cnt::bigint INTO batch_result
        FROM ag_catalog.cypher('{graph_name}', $cypher$
            MATCH (n:{vertex_name})
            WHERE exists(n._converted)
            WITH n LIMIT 50000
            REMOVE n._converted
            RETURN count(n)
        $cypher$) AS (cnt agtype);
        RAISE NOTICE 'Batch removed marker % nodes', batch_result;
    END LOOP;

    RAISE NOTICE '{vertex_name} cleanup complete';
END $$;
"""
        elif "_dewey" in graph_name:
            sql_base += f"""
-- Batch convert height/depth for {vertex_name}
DO $$
DECLARE
    batch_size INT := 50000;
    batch_result BIGINT := 1;
    total_converted BIGINT := 0;
BEGIN
    WHILE batch_result > 0 LOOP
        SELECT cnt::bigint INTO batch_result
        FROM ag_catalog.cypher('{graph_name}', $cypher$
            MATCH (n:{vertex_name})
            WHERE NOT exists(n._converted)
            WITH n LIMIT 50000
            SET n.height = toInteger(n.height),
                n.depth = toInteger(n.depth),
                n._converted = true
            RETURN count(n)
        $cypher$) AS (cnt agtype);

        total_converted := total_converted + batch_result;
        RAISE NOTICE 'Batch converted % (total: %)', batch_result, total_converted;
    END LOOP;

    -- Remove temporary marker in batches
    batch_result := 1;
    WHILE batch_result > 0 LOOP
        SELECT cnt::bigint INTO batch_result
        FROM ag_catalog.cypher('{graph_name}', $cypher$
            MATCH (n:{vertex_name})
            WHERE exists(n._converted)
            WITH n LIMIT 50000
            REMOVE n._converted
            RETURN count(n)
        $cypher$) AS (cnt agtype);
        RAISE NOTICE 'Batch removed marker % nodes', batch_result;
    END LOOP;
END $$;
"""
        # _plain requires no conversion (no annotation columns)

    for vertex_name in vertex_dict.keys():
        sql_base += f"""ANALYZE {graph_name}."{vertex_name}";\n"""

    for edge_name in edge_dict.keys():
        sql_base += f"""ANALYZE {graph_name}."{edge_name}";\n"""


    with open(save_dir / f'{graph_name}_creation.sql', 'w') as f:
        f.write(sql_base)

def create_sql_for_artificial_trees(artificial_base_dir : Path, save_dir : Path):
    for family_name in ["truebase", "ultratall", "ultrawide"]:
        for size in [10, 100, 1000, 10000, 100000]:
            for annotation_type in ["dewey", "prepost", "plain"]:
                edge_dict = {
                    "HAS_CHILD": artificial_base_dir / family_name / str(size) / "edges" / f"TreeEdges_{annotation_type}.csv"
                }
                node_dict = {
                    "TreeNode": artificial_base_dir / family_name / str(size) / "nodes" / f"TreeNodes_{annotation_type}.csv"
                }

                create_sql(f"{family_name.lower()}_{size}_{annotation_type}", node_dict, edge_dict, save_dir)

def create_sql_for_annotated_ldbc(ldbc_dir : Path, save_dir : Path):
    edge_dict = {}
    prepost_node_dict = {}
    dewey_node_dict = {}
    plain_node_dict = {}

    nodes_dir = Path(os.getenv("PROJECT_PATH")) / "data" / "prepared" / ldbc_dir / "nodes"
    edges_dir = Path(os.getenv("PROJECT_PATH")) / "data" / "prepared" / ldbc_dir / "edges"

    # Process node files
    for filename in os.listdir(nodes_dir):
        node_path = ldbc_dir / "nodes" / filename

        if filename.endswith("_prepost.csv"):
            s = filename.split("_")[0]
            prepost_node_dict[s[0].upper() + s[1:] if s else s] = node_path

        elif filename.endswith("_dewey.csv"):
            s = filename.split("_")[0]
            dewey_node_dict[s[0].upper() + s[1:] if s else s] = node_path

        elif filename.endswith("_plain.csv"):
            s = filename.split("_")[0]
            plain_node_dict[s[0].upper() + s[1:] if s else s] = node_path

    # Process edge files (shared across all annotation types)
    for filename in os.listdir(edges_dir):
        edge_path = ldbc_dir / "edges" / filename
        # Remove .csv extension for edge label name
        edge_dict[filename[:-4]] = edge_path

    graph_name_base = ldbc_dir.parts[-1] if isinstance(ldbc_dir, Path) else ldbc_dir.split("/")[-1]

    create_sql(f"{graph_name_base}_prepost", prepost_node_dict, edge_dict, save_dir)
    create_sql(f"{graph_name_base}_dewey", dewey_node_dict, edge_dict, save_dir)
    create_sql(f"{graph_name_base}_plain", plain_node_dict, edge_dict, save_dir)

def create_sql_for_black_forest(artificial_forest_base_dir : Path, save_dir : Path, size : int):
    for annotation_type in ["dewey", "prepost", "plain"]:
        edge_dict = {
            "HAS_CHILD": artificial_forest_base_dir / str(size) / "edges" / f"TreeEdges_{annotation_type}.csv"
        }
        node_dict = {
            "TreeNode": artificial_forest_base_dir / str(size) / "nodes" / f"TreeNodes_{annotation_type}.csv"
        }

        create_sql(f"artificial_forest_{size}_{annotation_type}", node_dict, edge_dict, save_dir)


if __name__ == '__main__':
    load_dotenv()

    save_dir = Path(os.getenv("PROJECT_PATH")) / Path("graph_init_sql")

    ldbc_dir = Path("snb/sf1")
    artificial_tree_dir = Path("artificial_trees")
    artificial_forest_dir = Path("artificial_forests")

    # create_sql_for_artificial_trees(artificial_tree_dir, save_dir)
    # create_sql_for_annotated_ldbc(ldbc_dir, save_dir)

    """create_sql_for_black_forest(
        artificial_forest_base_dir=artificial_forest_dir,
        save_dir=save_dir,
        size=1000
    )"""

    ldbc_dir_2 = Path("snb/sf2")
    create_sql_for_annotated_ldbc(ldbc_dir_2, save_dir)

    ldbc_dir_3 = Path("snb/sf3")
    create_sql_for_annotated_ldbc(ldbc_dir_3, save_dir)

