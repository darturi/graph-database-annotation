import json
import os
import random

class Parametrizer:
    def __init__(self,
                 base_meta_path=os.getenv("GRAPH_METADATA_STORE"),
                 base_save_path="/Users/danielarturi/Desktop/COMP 400/Kuzu1/LDBC_Work/logic/query_assessment/parametrized_queries"
                 ):
        self.base_meta_path = base_meta_path
        self.base_save_path = base_save_path

        self.current_meta = None

    def set_metadata(self, metadata_name):
        full_path = os.path.join(self.base_meta_path, f"{metadata_name}.json")

        with open(full_path, "r") as f:
            self.current_meta = json.load(f)

    def sample_n(self, n):
        if self.current_meta is None:
            return []

        parameter_sample = []

        for i in range(n):
            parameter_sample.append(
                {
                    "$rootID": random.choice(self.current_meta["roots"]),
                    "$id1": random.choice(self.current_meta["id_list"]),
                    "$id2": random.choice(self.current_meta["id_list"]),
                    "$nodeID": random.choice(self.current_meta["id_list"])
                }
            )
        return parameter_sample

if __name__ == "__main__":
    p = Parametrizer()
    p.set_metadata("truebase_10_s.json")
    print(p.sample_n(10))