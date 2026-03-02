import os
from pathlib import Path
from logic.setup.Annotator import Annotator
from logic.setup.ArtificalTreeGenerator import create_all_trees, create_forest
from dotenv import load_dotenv

if __name__ == '__main__':
    load_dotenv()

    csv_path = os.getenv("PROJECT_PATH") / Path("data/original_data/ldbc_data/social_network-sf1-CsvBasic-StringDateFormatter/dynamic")

    base_save = os.getenv("PROJECT_PATH") / Path("data")

    """# LDBC
    save_path = base_save / Path("annotated_ldbc_1")

    ann1 = Annotator(csv_path, load_post=True)
    ann1.save_revised_csv(ann1.ir_annotate_single_tree, save_path=save_path)
    ann1.id_mapping_dict = {}
    ann1.ids = []
    ann1.save_revised_csv(ann1.s_annotate_single_tree, save_path=save_path)"""

    # Comment Only LDBC
    save_path =  os.getenv("PROJECT_PATH") / Path("data") / Path("prepared/snb/sf1")
    ann2 = Annotator(csv_path, load_post=False)
    ann2.save_revised_csv(ann2.ir_annotate_single_tree, save_path=save_path)

    ann3 = Annotator(csv_path, load_post=False)
    ann3.save_revised_csv(ann3.s_annotate_single_tree, save_path=save_path)

    # Artificial
    create_all_trees(
        tree_path=os.getenv("PROJECT_PATH") / Path("data/prepared/artificial_trees"),
    )

    base_save = os.getenv("PROJECT_PATH") / Path("data/prepared")

    save_path = base_save / Path("artificial_forests")
    create_forest(
        forest_path=save_path,
        num_nodes=40,
        min_children=0,
        max_children=2,
        min_depth=1,
        max_depth=3
    )