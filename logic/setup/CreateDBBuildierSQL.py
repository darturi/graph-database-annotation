import os


def create_sql(graph_name, vertex_dir, edge_dir, save_dir, docker_data_dir="graph_data"):
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

    for vertex_key, vertex_val in vertex_dir.items():
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

    for edge_key, edge_val in edge_dir.items():
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

    for vertex_name in vertex_dir.keys():
        sql_base += f"""ANALYZE {graph_name}."{vertex_name}";\n"""

    for edge_name in edge_dir.keys():
        sql_base += f"""ANALYZE {graph_name}."{edge_name}";\n"""


    with open(os.path.join(save_dir, f'{graph_name}_creation.sql'), 'w') as f:
        f.write(sql_base)

"""def create_sql_for_ldbc(save_dir):
    annotated_ldbc_vertex_ir = {
        "Comment": "/data/test/comment_0_0_ir.csv",
        "Post": "/data/test/post_0_0_ir.csv"
    }
    annotated_ldbc_vertex_s = {
        "Comment": "/data/test/comment_0_0_s.csv",
        "Post": "/data/test/post_0_0_s.csv"
    }
    annotated_ldbc_edge = {
        "comment_replyOf_comment": "/data/test/comment_replyOf_comment_0_0.csv",
        "comment_replyOf_post": "/data/test/comment_replyOf_post_0_0.csv",
    }

    annotated_ldbc_vertex_comment_only_ir = {
        "Comment": "/data/test/comment_0_0_ir.csv",
    }
    annotated_ldbc_vertex_comment_only_s = {
        "Comment": "/data/test/comment_0_0_s.csv",
    }
    annotated_ldbc_edge_comment_only = {
        "comment_replyOf_comment": "/data/test/comment_replyOf_comment_0_0.csv",
    }

    create_sql("ldbc_01_ir", annotated_ldbc_vertex_ir,
               annotated_ldbc_edge, save_dir)
    create_sql("ldbc_01_s", annotated_ldbc_vertex_s,
               annotated_ldbc_edge, save_dir)
    create_sql("ldbc_01_comment_ir", annotated_ldbc_vertex_comment_only_ir,
               annotated_ldbc_edge_comment_only, save_dir)
    create_sql("ldbc_01_comment_s", annotated_ldbc_vertex_comment_only_s,
               annotated_ldbc_edge_comment_only, save_dir)"""

def create_sql_for_artificial_trees(artificial_base_dir, save_dir):
    artificial_tree_leaf = artificial_base_dir.split("/")[-1]

    for family_name in ["truebase", "ultratall", "ultrawide"]:
        for size in [10, 100, 1000, 10000, 100000]:
            for annotation_type in ["s", "ir"]:
                edge_dict = {
                    "HAS_CHILD" : os.path.join(artificial_tree_leaf, f"{family_name}_{size}_edges_{annotation_type}.csv")
                }
                node_dict = {
                    "TreeNode" : os.path.join(artificial_tree_leaf, f"{family_name}_{size}_nodes_{annotation_type}.csv")
                }

                create_sql(f"{family_name}_{size}_{annotation_type}", node_dict, edge_dict, save_dir)

def create_sql_for_annotated_ldbc(ldbc_dir, save_dir):
    edge_dict = {}
    ir_node_dict = {}
    s_node_dict = {}

    for filename in os.listdir(ldbc_dir):
        full_path = os.path.join(ldbc_dir, filename)

        part_path = "/".join(full_path.split("/")[-2:])

        if filename.endswith("_ir.csv"):
            s = filename.split("_")[0]
            ir_node_dict[s[0].upper() + s[1:] if s else s] = part_path

        elif filename.endswith("_s.csv"):
            s = filename.split("_")[0]
            s_node_dict[s[0].upper() + s[1:] if s else s] = part_path

        else:
            edge_dict[filename[:-8]] = part_path

    graph_name_base = "_".join((ldbc_dir.split("/")[-1]).split("_")[1:])

    create_sql(f"{graph_name_base}_ir", ir_node_dict, edge_dict, save_dir)
    create_sql(f"{graph_name_base}_s", s_node_dict, edge_dict, save_dir)


if __name__ == '__main__':
    save_dir = "/Users/danielarturi/Desktop/COMP 400/Kuzu1/LDBC_Work/graph_init_sql"

    lbdc_dir = "/Users/danielarturi/Desktop/COMP 400/Kuzu1/LDBC_Work/data/annotated_ldbc_01"
    lbdc2_dir = "/Users/danielarturi/Desktop/COMP 400/Kuzu1/LDBC_Work/data/annotated_ldbc_comment_only_01"
    artificial_tree_dir = "/Users/danielarturi/Desktop/COMP 400/Kuzu1/LDBC_Work/data/artificial_trees"

    create_sql_for_artificial_trees(artificial_tree_dir, save_dir)
    create_sql_for_annotated_ldbc(lbdc_dir, save_dir)
    create_sql_for_annotated_ldbc(lbdc2_dir, save_dir)