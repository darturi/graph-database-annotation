import copy
import json
import os
from logic.ExecutorDefinitions import Executor, ApacheExecutor
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from logic.query_assessment.CreateParametrizedQueries import Parametrizer

# By parsing sql contents
def get_all_graph_names(flag : str, sql_path : Path):
    graph_names = []

    for filename in Path.iterdir(sql_path):
        if flag not in filename:
            continue

        if "10_" not in filename and "100_" not in filename and "1000_" not in filename:
        # if "ldbc" not in filename:
            continue

        graph_names.append(filename[:-len("_creation.sql")])

    return graph_names


# def load_queries(query_file : str | Path, query_base_path=Path("/Users/danielarturi/Desktop/COMP 400/Kuzu1/LDBC_Work/queries")):
def load_queries(
        query_file: str | Path,
        query_base_path : Path
):
    query_path = query_base_path / Path(query_file)
    query_df = pd.read_csv(query_path, sep="|")
    return query_df

class Assessor:
    def __init__(self, graph_name: str, executor: Executor, save_logs: Path):
        self.executor = executor
        self.graph_name = graph_name

        self.save_logs = save_logs / Path(f"{graph_name}.json")

        self.parameter_generator = Parametrizer()
        self.parameter_generator.set_metadata(self.graph_name)

    def parametrize_query(self, query : str, param_dict : dict):
        q_copy = copy.deepcopy(query)
        for k, v in param_dict.items():
            q_copy = q_copy.replace(k, str(v))
        q_copy = q_copy.replace("$GRAPHNAME", self.graph_name)
        return q_copy

    def run_query_n(self, vanilla_p : str, annotated_p : str, heat=5, n=200):
        vanilla_dict = {
            "time" : [],
            "plans" : []
        }

        annotated_dict = {
            "time": [],
            "plans": []
        }

        heat_params = self.parameter_generator.sample_n(heat)
        run_params = self.parameter_generator.sample_n(n)

        vanilla_name, vanilla_query = vanilla_p
        annotated_name, annotated_query = annotated_p

        for heat_value in heat_params:
            _, _ = self.executor.execute_query(
                self.parametrize_query(vanilla_query, heat_value)
            )
            _, _ = self.executor.execute_query(
                self.parametrize_query(annotated_query, heat_value)
            )

        for run_value in run_params:
            vanilla_time, vanilla_plan = self.executor.collect_query_plan(
                self.parametrize_query(vanilla_query, run_value)
            )
            vanilla_dict['time'].append(vanilla_time)
            vanilla_dict['plans'].append(vanilla_plan)

            annotated_time, annotated_plan = self.executor.execute_query(
                self.parametrize_query(annotated_query, run_value)
            )
            annotated_dict['time'].append(annotated_time)
            annotated_dict['plans'].append(annotated_plan)

        return {
            vanilla_name : vanilla_dict,
            annotated_name : annotated_dict,
            "run_info" : run_params,
        }


    def run_all_query_n(self, query_df : pd.DataFrame, heat=1, n=1):
        log_dict = {}
        for _, row in query_df.iterrows():
            description_p, vanilla_p, ann_p = list(zip(query_df.columns, row.values))
            print(f"Processing {description_p[1]}")
            log_dict[description_p[1]] = self.run_query_n(vanilla_p, ann_p, heat=heat, n=n)

        with open(self.save_logs, "w", encoding="utf-8") as f:
            json.dump(log_dict, f, indent=2, ensure_ascii=False)

        return log_dict

def assess_db(ex, string_location : Path | str, ir_location : Path | str,
              result_log_base : Path,
              query_path : Path,
              sql_path : Path,
              heat=5, n=200
              ):
    query_df_ir = load_queries(
        query_file=ir_location,
        query_base_path=query_path
    )
    query_df_s = load_queries(
        query_file=string_location,
        query_base_path=query_path
    )

    graph_names_s = get_all_graph_names(flag="_s_", sql_path=sql_path)
    graph_names_ir = get_all_graph_names(flag="_ir_", sql_path=sql_path)

    for graph_name in tqdm(graph_names_s, desc="Processing string graphs"):
        print(f"Processing {graph_name}")
        ass = Assessor(
            graph_name=graph_name,
            executor=ex,
            save_logs=result_log_base
        )
        ass.run_all_query_n(
            query_df=query_df_s,
            heat=heat,
            n=n
        )

    for graph_name in tqdm(graph_names_ir, desc="Processing string graphs"):
        print(f"Processing {graph_name}")
        ass = Assessor(
            graph_name=graph_name,
            executor=ex,
            save_logs=result_log_base
        )
        ass.run_all_query_n(
            query_df=query_df_ir,
            heat=heat,
            n=n
        )



if __name__ == "__main__":
    ir_location_val = "artificial_tree_queries/Apache_IR.csv"
    s_location_val = "artificial_tree_queries/Apache_S.csv"

    ae = ApacheExecutor(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host='localhost',
        port=5456
    )

    project_path = os.getenv("PROJECT_PATH")

    assess_db(
        ex=ae,
        string_location=s_location_val,
        ir_location=ir_location_val,
        result_log_base=project_path / Path("results/result_logs"),
        query_path=project_path / Path("queries"),
        sql_path=project_path / Path("graph_init_sql"),
        heat=1,
        n=2
    )