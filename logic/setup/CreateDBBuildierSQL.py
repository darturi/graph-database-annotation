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
        set_addendum = ""
        if "ir" in graph_name:
            set_addendum = """n.integer_id = toInteger(n.integer_id),                                                                                                                                                                                                          
        n.upper_bound = toInteger(n.upper_bound),
        """

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
    '/age/{docker_data_dir}/{vertex_val}'
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
    '/age/{docker_data_dir}/{edge_val}'
);

"""

    for vertex_name in vertex_dict.keys():
        sql_base += f"""ANALYZE {graph_name}."{vertex_name}";\n"""

    for edge_name in edge_dict.keys():
        sql_base += f"""ANALYZE {graph_name}."{edge_name}";\n"""


    with open(save_dir / f'{graph_name}_creation.sql', 'w') as f:
        f.write(sql_base)

def create_sql_for_artificial_trees(artificial_base_dir : Path, save_dir : Path):
    artificial_tree_leaf = artificial_base_dir.parts[-1]

    for family_name in ["truebase", "ultratall", "ultrawide"]:
        for size in [10, 100, 1000, 10000, 100000]:
            for annotation_type in ["s", "ir"]:
                edge_dict = {
                    "HAS_CHILD" : artificial_tree_leaf / Path(f"{family_name}_{size}_edges_{annotation_type}.csv")
                }
                node_dict = {
                    "TreeNode" : artificial_tree_leaf / Path(f"{family_name}_{size}_nodes_{annotation_type}.csv")
                }

                create_sql(f"{family_name}_{size}_{annotation_type}", node_dict, edge_dict, save_dir)

def create_sql_for_annotated_ldbc(ldbc_dir : Path, save_dir : Path):
    edge_dict = {}
    ir_node_dict = {}
    s_node_dict = {}

    for filename in os.listdir(ldbc_dir):
        full_path = ldbc_dir / filename

        part_path = "/".join(full_path.parts[-2:])

        if filename.endswith("_ir.csv"):
            s = filename.split("_")[0]
            ir_node_dict[s[0].upper() + s[1:] if s else s] = part_path

        elif filename.endswith("_s.csv"):
            s = filename.split("_")[0]
            s_node_dict[s[0].upper() + s[1:] if s else s] = part_path

        else:
            edge_dict[filename[:-8]] = part_path

    graph_name_base = "_".join((ldbc_dir.parts[-1]).split("_")[1:])

    create_sql(f"{graph_name_base}_ir", ir_node_dict, edge_dict, save_dir)
    create_sql(f"{graph_name_base}_s", s_node_dict, edge_dict, save_dir)

def create_sql_for_black_forest(artificial_forest_base_dir : Path, save_dir : Path, size : int):
    for annotation_type in ["s", "ir"]:
        edge_dict = {
            "HAS_CHILD": artificial_forest_base_dir / Path(f"artificialforest_{size}_edges_{annotation_type}.csv")
        }
        node_dict = {
            "TreeNode": artificial_forest_base_dir / Path(f"artificialforest_{size}_nodes_{annotation_type}.csv")
        }

        create_sql(f"artificialforest_{size}_{annotation_type}", node_dict, edge_dict, save_dir)


if __name__ == '__main__':
    load_dotenv()

    save_dir = Path(os.getenv("PROJECT_PATH")) / Path("graph_init_sql")

    lbdc_dir = Path(os.getenv("PROJECT_PATH")) / Path("data/annotated_ldbc_01")
    lbdc2_dir = Path(os.getenv("PROJECT_PATH")) / Path("data/annotated_ldbc_comment_only_01")
    artificial_tree_dir = Path(os.getenv("PROJECT_PATH")) / Path("data/artificialtrees")

    artificial_forest_dir = Path(os.getenv("PROJECT_PATH")) / Path("data/artificial_forest")

    # create_sql_for_artificial_trees(artificial_tree_dir, save_dir)
    # create_sql_for_annotated_ldbc(lbdc_dir, save_dir)
    # create_sql_for_annotated_ldbc(lbdc2_dir, save_dir)

    create_sql_for_black_forest(
        artificial_forest_base_dir=artificial_forest_dir,
        save_dir=save_dir,
        size=40
    )