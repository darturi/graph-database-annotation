from logic.setup.Annotator import Annotator
from logic.setup.ArtificalTreeGenerator import create_all_trees

if __name__ == '__main__':
    csv_path = "/Users/danielarturi/Desktop/COMP 400/Kuzu1/LDBC_Work/data/original_data/ldbc_data/social_network-sf0.1-CsvBasic-LongDateFormatter/dynamic"

    base_save = "/Users/danielarturi/Desktop/COMP 400/Kuzu1/LDBC_Work/data/"

    # LDBC
    save_path = f"{base_save}annotated_ldbc_01"
    ann1 = Annotator(csv_path, load_post=True)
    ann1.save_revised_csv(ann1.ir_annotate_single_tree, save_path=save_path)
    ann1.id_mapping_dict = {}
    ann1.ids = []
    ann1.save_revised_csv(ann1.s_annotate_single_tree, save_path=save_path)

    # Comment Only LDBC
    save_path = f"{base_save}annotated_ldbc_comment_only_01"
    ann2 = Annotator(csv_path, load_post=False)
    ann2.save_revised_csv(ann2.ir_annotate_single_tree, save_path=save_path)
    ann2.id_mapping_dict = {}
    ann2.ids = []
    ann2.save_revised_csv(ann2.s_annotate_single_tree, save_path=save_path)

    # Artificial
    create_all_trees()