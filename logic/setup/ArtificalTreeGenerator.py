import json
import os
import pandas as pd
from Annotator import TreeNode, Annotator
import random
from pathlib import Path
from dotenv import load_dotenv

def generate_random_tree(num_nodes : int, min_children=0, max_children=5, node_name="TreeNode", start_id=1):
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
    root = TreeNode(node_id=start_id)
    root.type = node_name
    nodes_created = 1
    node_id_counter = start_id + 1

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

    return root, node_id_counter

def generate_random_forest(
        num_nodes : int,
        min_depth : int,
        max_depth : int,
        min_children : int,
        max_children : int,
        node_name="TreeNode",
        start_id=0
):
    if num_nodes < 1:
        raise ValueError("num_nodes must be at least 1")
    if min_children > max_children:
        raise ValueError("min_children cannot be greater than max_children")
    if min_depth > max_depth:
        raise ValueError("min_children cannot be greater than max_children")

    root_list = []
    running_id = start_id

    while running_id <= num_nodes:
        running_id += 1
        root = TreeNode(node_id=running_id)
        root.type = node_name

        frontier = [root]
        root_list.append(root)

        tree_depth = random.randint(min_depth, max_depth)

        for depth_level in range(tree_depth):
            new_frontier = []

            # Pick one node that must have at least 1 child to guarantee min_depth
            guaranteed_node = random.choice(frontier) if depth_level < min_depth else None

            while len(frontier) > 0:
                parent = random.choice(frontier)

                # Ensure at least 1 child for the guaranteed node until min_depth is reached
                effective_min_children = max(1, min_children) if parent is guaranteed_node else min_children

                max_possible = min(max_children, num_nodes - running_id)
                num_children = random.randint(effective_min_children, max(effective_min_children, max_possible))

                # Create the children
                for _ in range(num_children):
                    if running_id >= num_nodes:
                        return root_list

                    running_id += 1

                    child = TreeNode(node_id=running_id)
                    parent.add_child(child)
                    child.type = node_name
                    new_frontier.append(child)

                # Remove this parent from frontier since it has its children
                frontier.remove(parent)

            frontier = new_frontier

    return root_list


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

def write_forest_to_csv(
        root_list : list[TreeNode],
        edge_name : Path,
        node_name : Path
):
    tree_nodes = []
    tree_edges = []

    for tree in root_list:
        node_slice, edge_slice = tree_formatter(tree)

        tree_nodes.extend(node_slice)
        tree_edges.extend(edge_slice)

    tree_nodes = pd.DataFrame(tree_nodes)
    tree_edges = pd.DataFrame(tree_edges, columns=["start_id", "start_vertex_type", "end_id", "end_vertex_type"])

    tree_nodes.to_csv(node_name, index=False, encoding="utf-8")
    tree_edges.to_csv(edge_name, index=False, encoding="utf-8")

    return tree_nodes, tree_edges


def write_tree_to_csv(root_node : TreeNode, edge_name : Path, node_name : Path):
    tree_nodes, tree_edges = tree_formatter(root_node)

    # print(tree_nodes)

    tree_nodes = pd.DataFrame(tree_nodes)
    tree_edges = pd.DataFrame(tree_edges, columns=["start_id","start_vertex_type","end_id","end_vertex_type"])

    tree_nodes.to_csv(node_name, index=False, encoding="utf-8")
    tree_edges.to_csv(edge_name, index=False, encoding="utf-8")

    return tree_nodes, tree_edges

def create_forest(
        forest_path : Path,
        num_nodes : int,
        min_depth : int,
        max_depth : int,
        min_children : int,
        max_children : int,
        node_name="TreeNode",
        start_id=1,
        seed=42,
):
    random.seed(seed)

    root_list = generate_random_forest(
        num_nodes=num_nodes,
        min_depth=min_depth,
        max_depth=max_depth,
        min_children=min_children,
        max_children=max_children,
        node_name=node_name,
        start_id=start_id
    )

    ann = Annotator(csv_dir=Path(""))

    base_name=f"artificial_forest_{num_nodes}"

    ir_edge_path = forest_path / Path(str(num_nodes)) / Path("edges") / Path("TreeEdges_prepost.csv")
    ir_node_path = forest_path / Path(str(num_nodes)) / Path("nodes") / Path("TreeNodes_prepost.csv")
    s_edge_path = forest_path / Path(str(num_nodes)) / Path("edges") / Path("TreeEdges_dewey.csv")
    s_node_path = forest_path / Path(str(num_nodes)) / Path("nodes") / Path("TreeNodes_dewey.csv")
    plain_edge_path = forest_path / Path(str(num_nodes)) / Path("edges") / Path("TreeEdges_plain.csv")
    plain_node_path = forest_path / Path(str(num_nodes)) / Path("nodes") / Path("TreeNodes_plain.csv")

    ann.current_root_id = 1

    for tree in root_list:
        ann.ir_annotate_single_tree(tree)

    write_forest_to_csv(
        root_list=root_list,
        edge_name=ir_edge_path,
        node_name=ir_node_path,
    )

    metadata_path = os.getenv("PROJECT_PATH") / Path("graph_metadata") / f"{base_name}_prepost.json"
    with open(metadata_path, "w", encoding="utf-8") as f:
        ir_record = {
            "graph_name": f"{base_name}_prepost",
            "roots": [node.new_id["integer_id"] for node in root_list],
            "id_list": ann.ids,
        }
        json.dump(ir_record, f, ensure_ascii=False)
        f.write("\n")

    for tree in root_list:
        tree.clear_annotations()
    ann.ids = []
    ann.current_root_id = 1

    for tree in root_list:
        ann.s_annotate_single_tree(tree)

    write_forest_to_csv(
        root_list=root_list,
        edge_name=s_edge_path,
        node_name=s_node_path,
    )

    metadata_path = os.getenv("PROJECT_PATH") / Path("graph_metadata") / f"{base_name}_dewey.json"
    with open(metadata_path, "w", encoding="utf-8") as f:
        ir_record = {
            "graph_name": f"{base_name}_dewey",
            "roots": [node.new_id["string_id"] for node in root_list],
            "id_list": ann.ids,
        }
        json.dump(ir_record, f, ensure_ascii=False)
        f.write("\n")

    # Write plain version (dewey without height, depth, string_id)
    dewey_nodes_df = pd.read_csv(s_node_path)
    plain_nodes_df = dewey_nodes_df.drop(columns=["height", "depth", "string_id"])
    plain_nodes_df.to_csv(plain_node_path, index=False, encoding="utf-8")

    # Copy edge file for plain version
    dewey_edges_df = pd.read_csv(s_edge_path)
    dewey_edges_df.to_csv(plain_edge_path, index=False, encoding="utf-8")

def create_all_trees(tree_path : Path, seed=42):
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

    ann = Annotator(csv_dir=Path(""))

    for tree_family in param_dict.keys():
        print(f"Generating {tree_family}")

        family_info = param_dict[tree_family]

        for size in family_info.keys():
            print(f"\t{size}")
            lb, ub = family_info[size]['lb'], family_info[size]['ub']

            tree, _ = generate_random_tree(num_nodes=size, min_children=lb, max_children=ub)

            base_name = f"{tree_family}_{size}".lower()

            ir_edge_path = tree_path / Path(tree_family) / Path(str(size)) / Path("edges") / Path("TreeEdges_prepost.csv")
            ir_node_path = tree_path / Path(tree_family) / Path(str(size)) / Path("nodes") / Path("TreeNodes_prepost.csv")
            s_edge_path = tree_path / Path(tree_family) / Path(str(size)) / Path("edges") / Path("TreeEdges_dewey.csv")
            s_node_path = tree_path / Path(tree_family) / Path(str(size)) / Path("nodes") / Path("TreeNodes_dewey.csv")
            plain_edge_path = tree_path / Path(tree_family) / Path(str(size)) / Path("edges") / Path("TreeEdges_plain.csv")
            plain_node_path = tree_path / Path(tree_family) / Path(str(size)) / Path("nodes") / Path("TreeNodes_plain.csv")

            ann.current_root_id = 1

            ann.ir_annotate_single_tree(tree)

            write_tree_to_csv(tree, ir_edge_path, ir_node_path)

            # Update graph metadata .jsonl
            metadata_path = os.getenv("PROJECT_PATH") / Path("graph_metadata") / f"{base_name}_prepost.json"
            with open(metadata_path, "w", encoding="utf-8") as f:
                ir_record = {
                    "graph_name": f"{base_name}_prepost",
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
            metadata_path = os.getenv("PROJECT_PATH") / Path("graph_metadata") / f"{base_name}_dewey.json"
            with open(metadata_path, "w", encoding="utf-8") as f:
                s_record = {
                    "graph_name": f"{base_name}_dewey",
                    "roots": ["1"],
                    "id_list": ann.ids,
                }
                json.dump(s_record, f, ensure_ascii=False)
                f.write("\n")

            # Write plain version (dewey without height, depth, string_id)
            dewey_nodes_df = pd.read_csv(s_node_path)
            plain_nodes_df = dewey_nodes_df.drop(columns=["height", "depth", "string_id"])
            plain_nodes_df.to_csv(plain_node_path, index=False, encoding="utf-8")

            # Copy edge file for plain version
            dewey_edges_df = pd.read_csv(s_edge_path)
            dewey_edges_df.to_csv(plain_edge_path, index=False, encoding="utf-8")

            ann.ids = []

if __name__ == '__main__':
    load_dotenv()

    print(os.getenv("PROJECT_PATH"))

    base_save = os.getenv("PROJECT_PATH") / Path("data") / Path("prepared")

    save_path = base_save / Path("artificial_forests")
    """create_forest(
        forest_path=save_path,
        num_nodes=40,
        min_children=0,
        max_children=2,
        min_depth=1,
        max_depth=3
    )"""

    create_forest(
        forest_path=save_path,
        num_nodes=1000,
        min_children=0,
        max_children=3,
        min_depth=4,
        max_depth=9
    )

    # original_data_dir = Path("/Users/danielarturi/Desktop/graph-database-annotation/data/original_data/ldbc_data/social_network-sf0.1-CsvBasic-LongDateFormatter/dynamic")

    # create_all_trees(
    #      tree_path=base_save / Path("artificial_trees"),
    # )

