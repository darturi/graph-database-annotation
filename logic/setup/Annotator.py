import json
from collections import defaultdict
import csv
import os
import pandas as pd
from pathlib import Path

def transform_csv(
    input_csv_path : Path,
    output_csv_path : Path,
    start_vertex_type : str,
    end_vertex_type : str,
    has_header=True
):
    with input_csv_path.open("r", newline="", encoding="utf-8") as infile, \
         output_csv_path.open("w", newline="", encoding="utf-8") as outfile:

        reader = csv.reader(infile, delimiter="|")
        writer = csv.writer(outfile, delimiter=",")

        # Write output header
        writer.writerow([
            "start_id",
            "start_vertex_type",
            "end_id",
            "end_vertex_type"
        ])

        # Skip input header if present
        if has_header:
            next(reader, None)

        for row in reader:
            start_id, end_id = row[0], row[1]

            writer.writerow([
                start_id,
                start_vertex_type,
                end_id,
                end_vertex_type
            ])

class TreeNode:
    """Represents a node in the Post/Comment tree structure."""

    def __init__(self, node_id):
        self.id = node_id
        self.new_id = None
        self.children = []
        self.type = "Post"

    def add_child(self, child_node):
        """Add a child TreeNode to this node."""
        child_node.type = "Comment"
        self.children.append(child_node)

    def __repr__(self):
        return f"TreeNode(id={self.id}, new_id={self.new_id}, children={len(self.children)})"

    def print_tree(self, prefix="", is_last=True):
        """
        Print the tree structure in a visually clear manner.

        Args:
            prefix: The prefix string for indentation (used internally for recursion)
            is_last: Whether this node is the last child of its parent

        Example output:
            └── 618475290624
                ├── 618475290625
                ├── 618475290626
                └── 1030792151045
                    ├── 1030792151046
                    └── 1030792151047
        """
        # Choose the connector based on whether this is the last child
        connector = "└── " if is_last else "├── "

        # Print current node
        if self.new_id is not None:
            print(f"{prefix}{connector}{self.id} ({self.type}) -> {self.new_id}")
        else:
            print(f"{prefix}{connector}{self.id} ({self.type})")

        # Prepare prefix for children
        # If this is the last child, add spaces; otherwise add a vertical line
        child_prefix = prefix + ("    " if is_last else "│   ")

        # Print all children
        for i, child in enumerate(self.children):
            child_is_last = (i == len(self.children) - 1)
            child.print_tree(child_prefix, child_is_last)

    def get_row_dict(self):
        r_dict = {
            "id": self.id,
            "type": self.type,
        }

        return r_dict | self.new_id

    def clear_annotations(self):
        self.new_id = None

        for child in self.children:
            child.clear_annotations()


class Annotator():
    def __init__(self, csv_dir : Path, start=1, load_post=True):
        """
        Initialize the annotator with CSV data directory.

        Args:
            csv_dir: Path to directory containing edge CSV files
        """
        self.current_root_id = start
        self.csv_dir = csv_dir
        self._edge_cache = None
        self.id_mapping_dict = {}

        self.load_post = load_post

        if self.load_post:
            self.root_type = "Post"
        else:
            self.root_type = "Comment"

        self.roots = set()
        self.ids = []

    def save_annotations(self, save_path : Path):
        if len(self.id_mapping_dict) == 0:
            print("No annotations to save")
            return

        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(self.id_mapping_dict, f, indent=4, ensure_ascii=False)

    def _load_all_edges(self):
        """
        Load all replyOf edges from CSV files.

        Returns:
            dict: Adjacency list mapping (node_type, node_id) -> [(child_type, child_id), ...]
        """
        if self._edge_cache is not None:
            return self._edge_cache

        edges = defaultdict(list)

        comment_post_file = self.csv_dir / Path('comment_replyOf_post_0_0.csv')
        with open(comment_post_file, 'r') as f:
            reader = csv.reader(f, delimiter='|')
            next(reader)  # Skip header
            for row in reader:
                child_id = int(row[0])  # Comment
                parent_id = int(row[1])  # Post

                if self.load_post:
                    # self.roots.add(parent_id)
                    edges[('Post', parent_id)].append(('Comment', child_id))
                else:
                    # Comment is a root (replies to post, not another comment)
                    self.roots.add(child_id)

        if self.load_post:
            self.roots = self._get_all_post_ids()

                    # Load comment->comment edges
        comment_comment_file = self.csv_dir / Path('comment_replyOf_comment_0_0.csv')
        with open(comment_comment_file, 'r') as f:
            reader = csv.reader(f, delimiter='|')
            next(reader)  # Skip header
            for row in reader:
                child_id = int(row[0])  # Comment (child)
                parent_id = int(row[1])  # Comment (parent)

                # If its just comments
                if not self.load_post:
                    if child_id in self.roots:
                        self.roots.remove(child_id)
                    if parent_id not in self.roots:
                        self.roots.add(parent_id)

                edges[('Comment', parent_id)].append(('Comment', child_id))

        self._edge_cache = edges
        return edges

    def _get_all_post_ids(self):
        """
        Get all Post IDs from the CSV file.

        Returns:
            list: List of Post node IDs
        """
        post_file = self.csv_dir / 'post_0_0.csv'
        post_ids = []

        with open(post_file, 'r') as f:
            reader = csv.reader(f, delimiter='|')
            next(reader)  # Skip header
            for row in reader:
                post_ids.append(int(row[0]))  # First column is the ID

        return post_ids

    def _get_all_comment_ids(self):
        """
        Get all Post IDs from the CSV file.

        Returns:
            list: List of Post node IDs
        """
        comment_file = self.csv_dir / 'comment_0_0.csv'
        comment_ids = []

        with open(comment_file, 'r') as f:
            reader = csv.reader(f, delimiter='|')
            next(reader)  # Skip header
            for row in reader:
                comment_ids.append(int(row[0]))  # First column is the ID

        return comment_ids

    def _build_tree(self, node_id, node_type : str):
        """
        Recursively build a tree from the edge adjacency list.

        Args:
            node_id: ID of the current node
            node_type: Type of the current node ('Post' or 'Comment')

        Returns:
            TreeNode: Root of the tree/subtree
        """
        node = TreeNode(node_id)
        node.type = node_type
        for child_type, child_id in self._edge_cache.get((node_type, node_id), []):
            child_tree = self._build_tree(child_id, child_type)
            node.add_child(child_tree)
        return node

    def probe_root(self, root_id):
        """
        Reconstructs the tree rooted at the Post node with the given ID.
        Uses cached edge data for O(1) lookups after initial load.

        Args:
            root_id: Integer ID of the node
        Returns:
            TreeNode: Root of the reconstructed tree
        """
        if self._edge_cache is None:
            self._edge_cache = self._load_all_edges()

        return self._build_tree(root_id, self.root_type)

    def ir_annotate_single_tree(self, tree : TreeNode):
        self.traversal_counter = self.current_root_id

        def annotate_dfs(node, depth):
            """
            Perform DFS traversal and annotate each node.

            Args:
                node: Current TreeNode being processed
                depth: Depth of current node (root has depth 0)

            Returns:
                tuple: (upper_bound, height) of the subtree rooted at this node
            """
            # Assign integer_id based on DFS traversal order
            integer_id = self.traversal_counter
            self.traversal_counter += 1

            # Base case: leaf node
            if not node.children:
                node.new_id = {
                    "integer_id": integer_id,
                    "upper_bound": integer_id,
                    "height": 0,
                    "depth": depth
                }

                # Update id tracker
                self.ids.append(integer_id)

                # Save to running dict
                if node.type not in self.id_mapping_dict.keys():
                    self.id_mapping_dict[node.type] = {}
                self.id_mapping_dict[node.type][node.id] = node.new_id

                return integer_id, 0

            # Recursive case: process all children
            max_upper_bound = integer_id
            max_height = 0

            for child in node.children:
                child_upper_bound, child_height = annotate_dfs(child, depth + 1)
                max_upper_bound = max(max_upper_bound, child_upper_bound)
                max_height = max(max_height, child_height + 1)

            # Annotate current node
            node.new_id = {
                "integer_id": integer_id,
                "upper_bound": max_upper_bound,
                "height": max_height,
                "depth": depth
            }

            # Update id tracker
            self.ids.append(integer_id)

            # Save to running dict
            if node.type not in self.id_mapping_dict.keys():
                self.id_mapping_dict[node.type] = {}
            self.id_mapping_dict[node.type][node.id] = node.new_id

            return max_upper_bound, max_height

        # Start DFS from root with depth 0
        annotate_dfs(tree, 0)

        # Update current_root_id for next tree
        next_id = self.traversal_counter
        self.current_root_id = next_id

        return next_id

    def s_annotate_single_tree(self, tree):
        def annotate_dfs(node, depth, string_id):
            """
            Perform DFS traversal and annotate each node with string ID.

            Args:
                node: Current TreeNode being processed
                depth: Depth of current node (root has depth 0)
                string_id: String ID for this node

            Returns:
                int: Height of the subtree rooted at this node
            """
            # Base case: leaf node
            if not node.children:
                node.new_id = {
                    "string_id": string_id,
                    "height": 0,
                    "depth": depth
                }

                # Update id tracker
                self.ids.append(string_id)

                # Save to running dict
                if node.type not in self.id_mapping_dict.keys():
                    self.id_mapping_dict[node.type] = {}
                self.id_mapping_dict[node.type][node.id] = node.new_id

                return 0

            # Recursive case: process all children
            max_height = 0

            for i, child in enumerate(node.children):
                # First child gets ".1", second gets ".2", etc.
                child_string_id = f"{string_id}.{i + 1}"
                child_height = annotate_dfs(child, depth + 1, child_string_id)
                max_height = max(max_height, child_height + 1)

            # Annotate current node
            node.new_id = {
                "string_id": string_id,
                "height": max_height,
                "depth": depth
            }

            # Update id tracker
            self.ids.append(string_id)

            # Save to running dict
            if node.type not in self.id_mapping_dict.keys():
                self.id_mapping_dict[node.type] = {}
            self.id_mapping_dict[node.type][node.id] = node.new_id

            return max_height

        # Start DFS from root with its string ID
        root_string_id = str(self.current_root_id)
        annotate_dfs(tree, 0, root_string_id)

        # Increment current_root_id for next tree
        self.current_root_id += 1

        return self.current_root_id

    def build_annotation_dict(self, annotate_func):
        if self._edge_cache is None:
            self._edge_cache = self._load_all_edges()

        print(f"Found {len(self.roots)} Post nodes to annotate")

        # Process each Post tree
        for idx, root_id in enumerate(self.roots):
            if (idx + 1) % 10000 == 0:
                print(f"Processing tree {idx + 1}/{len(self.roots)}...")

            # Reconstruct tree from CSV
            tree = self.probe_root(root_id)

            # Annotate tree
            annotate_func(tree)

    def save_revised_csv(self, annotate_func, save_path : Path, verbose=True, save_annotations=None):
        if len(self.id_mapping_dict.keys()) == 0:
            self.build_annotation_dict(annotate_func)

            if save_annotations is not None:
                self.save_annotations(save_annotations)

        file_addendum = None

        # POST
        # Load Post CSV
        post_path = self.csv_dir / Path('post_0_0.csv')
        post_df = pd.read_csv(post_path, sep="|")

        # Add columns to post df
        post_df['height'] = pd.Series([-1] * len(post_df), dtype="Int64")
        post_df['depth'] = pd.Series([0] * len(post_df), dtype="Int64") # Done bc it's always a root

        if annotate_func == self.s_annotate_single_tree:
            post_df['string_id'] = "N/A"
            file_addendum = "s"
            root_list = [str(r) for r in self.roots]
        elif annotate_func == self.ir_annotate_single_tree:
            post_df['integer_id'] = pd.Series([-1] * len(post_df), dtype="Int64")
            post_df['upper_bound'] = pd.Series([-1] * len(post_df), dtype="Int64")
            file_addendum = "ir"
            root_list = [r for r in self.roots]

        if self.load_post:
            # Update with annotations
            post_subsection = self.id_mapping_dict['Post']
            print(len(post_subsection))
            counter = 0

            post_df.set_index('id', inplace=True)

            for id_value, val_dict in post_subsection.items():
                for col, val in val_dict.items():
                    post_df.at[id_value, col] = val

                counter += 1
                if verbose and counter % 10000 == 0:
                    print(f"Updating Post Entry {counter}/{len(post_subsection)}...")

            post_df.reset_index(inplace=True)

            # Save updated post df as csv
            post_df.to_csv(save_path / Path(f'post_0_0_{file_addendum}.csv'), index=False)

            # Handle comment - post edges
            transform_csv(
                input_csv_path=self.csv_dir / Path('comment_replyOf_post_0_0.csv'),
                output_csv_path=save_path / Path('comment_replyOf_post_0_0.csv'),
                start_vertex_type="Comment",
                end_vertex_type="Post",
                has_header=True,
            )

        # COMMENT
        # Load Comment CSV
        comment_path = self.csv_dir / Path('comment_0_0.csv')
        comment_df = pd.read_csv(comment_path, sep="|")

        # Add columns to comment df
        comment_df['height'] = pd.Series([-1] * len(comment_df), dtype="Int64")
        comment_df['depth'] = pd.Series([-1] * len(comment_df), dtype="Int64")

        if annotate_func == self.s_annotate_single_tree:
            comment_df['string_id'] = "N/A"
        elif annotate_func == self.ir_annotate_single_tree:
            comment_df['integer_id'] = pd.Series([-1] * len(comment_df), dtype="Int64")
            comment_df['upper_bound'] = pd.Series([-1] * len(comment_df), dtype="Int64")

        # Update with annotations
        counter = 0
        comment_subsection = self.id_mapping_dict['Comment']

        # print(comment_subsection)

        comment_df.set_index('id', inplace=True)
        for id_value, val_dict in comment_subsection.items():
            for col, val in val_dict.items():
                comment_df.at[id_value, col] = val

            counter += 1
            if verbose and counter % 10000 == 0:
                print(f"Updating Comment Entry {counter}/{len(comment_subsection)}...")


        comment_df.reset_index(inplace=True)
        # Save updated comment df as csv
        comment_df.to_csv(save_path / Path(f'comment_0_0_{file_addendum}.csv'), index=False)

        # Update graph metadata .jsonl
        base_name = "_".join(save_path.parts[-1].split("_")[1:])
        print(f"Annotating {base_name}")
        metadata_path = os.getenv("PROJECT_PATH") / Path(f"graph_metadata/{base_name}_ir.json")
        with open(metadata_path, "w", encoding="utf-8") as f:
            s_record = {
                "graph_name": f"{base_name}_{file_addendum}",
                "roots": root_list,
                "id_list": self.ids,
            }
            json.dump(s_record, f, ensure_ascii=False)
            f.write("\n")

        # Handle comment - comment edges
        transform_csv(
            input_csv_path=self.csv_dir / 'comment_replyOf_comment_0_0.csv',
            output_csv_path=save_path / 'comment_replyOf_comment_0_0.csv',
            start_vertex_type="Comment",
            end_vertex_type="Comment",
            has_header=True,
        )


if __name__ == '__main__':
    csv_path = os.getenv("PROJECT_PATH") / Path("data") / Path("original_data/ldbc_data/social_network-sf0.1-CsvBasic-LongDateFormatter/dynamic")

    save_path =  os.getenv("PROJECT_PATH") / Path("data") / Path("annotated_ldbc_comment_only_01")

    ann = Annotator(csv_path, load_post=False)

    ann.save_revised_csv(ann.ir_annotate_single_tree, save_path=save_path)
    ann.id_mapping_dict = {}
    ann.ids = []
    ann.save_revised_csv(ann.s_annotate_single_tree, save_path=save_path)