import json
import os
import pandas as pd
from Annotator import TreeNode, Annotator#, AnnotatorScribe
import random


def generate_random_tree(num_nodes, min_children=0, max_children=5, node_name="TreeNode"):
    """
    Generate a random tree with the specified number of nodes.

    Args:
        num_nodes: Total number of nodes to generate in the tree
        min_children: Minimum number of children a node can have
        max_children: Maximum number of children a node can have

    Returns:
        TreeNode: The root node of the generated tree

    Raises:
        ValueError: If num_nodes < 1 or if min_children > max_children
    """
    if num_nodes < 1:
        raise ValueError("num_nodes must be at least 1")
    if min_children > max_children:
        raise ValueError("min_children cannot be greater than max_children")

    # Create the root node
    root = TreeNode(node_id=1)
    root.type = node_name
    nodes_created = 1
    node_id_counter = 2

    # Keep track of nodes that can still have children added
    # Start with just the root
    frontier = [root]

    while nodes_created < num_nodes and frontier:
        # Pick a random node from the frontier to add children to
        parent = random.choice(frontier)

        # Determine how many children this node should have
        # Make sure we don't exceed the remaining nodes needed
        max_possible = min(max_children, num_nodes - nodes_created)
        num_children = random.randint(min_children, max(min_children, max_possible))

        # Create the children
        for _ in range(num_children):
            if nodes_created >= num_nodes:
                break

            child = TreeNode(node_id=node_id_counter)
            parent.add_child(child)
            child.type = node_name
            frontier.append(child)

            node_id_counter += 1
            nodes_created += 1

        # Remove this parent from frontier since it has its children
        frontier.remove(parent)

    return root

def write_tree_to_csv(root_node, edge_name, node_name):

    def tree_formatter(tree):
        tree_node_dict = []
        tree_node_rel_dict = []

        tree_node_dict.append(tree.get_row_dict())

        for child in tree.children:
            tree_node_rel_dict.append([child.id, child.type, tree.id, tree.type])

            new_entries, new_edges = tree_formatter(child)

            tree_node_dict.extend(new_entries)
            tree_node_rel_dict.extend(new_edges)

        return tree_node_dict, tree_node_rel_dict

    tree_nodes, tree_edges = tree_formatter(root_node)

    # print(tree_nodes)

    tree_nodes = pd.DataFrame(tree_nodes)
    tree_edges = pd.DataFrame(tree_edges, columns=["start_id","start_vertex_type","end_id","end_vertex_type"])

    tree_nodes.to_csv(node_name, index=False, encoding="utf-8")
    tree_edges.to_csv(edge_name, index=False, encoding="utf-8")

def create_all_trees(tree_path="/Users/danielarturi/Desktop/COMP 400/Kuzu1/LDBC_Work/data/artificial_trees", seed=42):
    random.seed(seed)

    param_dict = {
        "TrueBase": {
            10: {
                "lb": 1,
                "ub": 3,
            },
            100: {
                "lb": 1,
                "ub": 4,
            },
            1000: {
                "lb": 2,
                "ub": 5,
            },
            10000: {
                "lb": 3,
                "ub": 5,
            },
            100000: {
                "lb": 3,
                "ub": 20,
            },
        },

        "UltraTall": {
            10: {
                "lb": 1,
                "ub": 3,
            },
            100: {
                "lb": 1,
                "ub": 3,
            },
            1000: {
                "lb": 1,
                "ub": 3,
            },
            10000: {
                "lb": 1,
                "ub": 3,
            },
            100000: {
                "lb": 1,
                "ub": 3,
            },
        },

        "UltraWide": {
            10: {
                "lb": 2,
                "ub": 4,
            },
            100: {
                "lb": 4,
                "ub": 6,
            },
            1000: {
                "lb": 7,
                "ub": 9,
            },
            10000: {
                "lb": 9,
                "ub": 11,
            },
            100000: {
                "lb": 11,
                "ub": 13,
            },
        },
    }

    ann = Annotator()

    for tree_family in param_dict.keys():
        print(f"Generating {tree_family}")

        family_info = param_dict[tree_family]

        for size in family_info.keys():
            print(f"\t{size}")
            lb, ub = family_info[size]['lb'], family_info[size]['ub']

            tree = generate_random_tree(num_nodes=size+1, min_children=lb, max_children=ub)

            base_name = f"{tree_family}_{size}".lower()

            ir_edge_path = os.path.join(tree_path, f"{base_name}_edges_ir.csv")
            ir_node_path = os.path.join(tree_path, f"{base_name}_nodes_ir.csv")
            s_edge_path = os.path.join(tree_path, f"{base_name}_edges_s.csv")
            s_node_path = os.path.join(tree_path, f"{base_name}_nodes_s.csv")

            ann.current_root_id = 1

            ann.ir_annotate_single_tree(tree)

            write_tree_to_csv(tree, ir_edge_path, ir_node_path)

            # Update graph metadata .jsonl
            metadata_path = os.path.join(os.getenv("GRAPH_METADATA_STORE"), f"{base_name}_ir.json")
            with open(metadata_path, "w", encoding="utf-8") as f:
                ir_record = {
                    "graph_name": f"{base_name}_ir",
                    "roots": [1],
                    "id_list": ann.ids,
                }
                json.dump(ir_record, f, ensure_ascii=False)
                f.write("\n")

            tree.clear_annotations()
            ann.ids = []
            ann.current_root_id = 1  # Reset for string annotation

            ann.s_annotate_single_tree(tree)

            write_tree_to_csv(tree, s_edge_path, s_node_path)

            # Update graph metadata .jsonl
            metadata_path = os.path.join(os.getenv("GRAPH_METADATA_STORE"), f"{base_name}_s.json")
            with open(metadata_path, "w", encoding="utf-8") as f:
                s_record = {
                    "graph_name": f"{base_name}_s",
                    "roots": ["1"],
                    "id_list": ann.ids,
                }
                json.dump(s_record, f, ensure_ascii=False)
                f.write("\n")

            ann.ids = []

if __name__ == '__main__':
    create_all_trees()

