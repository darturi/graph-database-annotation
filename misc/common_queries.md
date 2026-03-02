### Get All Apache Graphs

SELECT * FROM ag_catalog.ag_graph;

### Get count of nodes in a given graph

SELECT *
FROM cypher('artificialforest_40_ir', $$
    MATCH (n)
    RETURN count(n)
$$) AS (node_count bigint);


## setup
SET search_path = ag_catalog, "$user", public;


## for shared
docker compose -f docker/age/docker-compose.yml up -d

docker exec age_treebench psql -U postgresUser -d postgresDB -c "SELECT * FROM cypher('ldbc_comment_only_1_s', $$                                                                                                                                                                                                         
      MATCH (n:Comment)                                                                                                                                                                                                                                    
      RETURN n                                                                                                                                                                                                                                             
      LIMIT 1                                                                                                                                                                                                                                              
  $$) AS (n agtype); "

scp -r daniel@nymeria:/home/daniel/trees-in-graphs-bench/results/age/raw_yes_id_idx \
/Users/danielarturi/trees-in-graphs-bench/results/age/

scp -r daniel@nymeria:/home/daniel/trees-in-graphs-bench/results/kuzu/raw \
/Users/danielarturi/trees-in-graphs-bench/results/kuzu/


docker exec age_treebench psql -U postgresUser -d postgresDB -c ""

docker exec age_treebench psql -U postgresUser -d postgresDB -c "SELECT * FROM ag_catalog.ag_graph;"

docker exec age_treebench psql -U postgresUser -d postgresDB -c "SELECT * FROM ag_catalog.ag_label WHERE graph = (SELECT graphid FROM ag_catalog.ag_graph WHERE name = 'sf1_plain');"

docker exec age_treebench psql -U postgresUser -d postgresDB -c "SELECT * FROM ag_catalog.alter_graph('artificial_forests_40', 'RENAME', 'artificial_forests_40_plain');"
docker exec age_treebench psql -U postgresUser -d postgresDB -c "SELECT * FROM ag_catalog.alter_graph('artificial_forest_40_plain', 'RENAME', 'artificial_forests_40_plain');"

docker exec age_treebench psql -U postgresUser -d postgresDB -c "SELECT * FROM ag_catalog.alter_graph('artificial_trees_truebase_10', 'RENAME', 'artificial_trees_truebase_10_plain');"
docker exec age_treebench psql -U postgresUser -d postgresDB -c "SELECT * FROM ag_catalog.alter_graph('artificial_trees_truebase_100', 'RENAME', 'artificial_trees_truebase_100_plain');"
docker exec age_treebench psql -U postgresUser -d postgresDB -c "SELECT * FROM ag_catalog.alter_graph('artificial_trees_truebase_1000', 'RENAME', 'artificial_trees_truebase_1000_plain');"
docker exec age_treebench psql -U postgresUser -d postgresDB -c "SELECT * FROM ag_catalog.alter_graph('artificial_trees_truebase_10000', 'RENAME', 'artificial_trees_truebase_10000_plain');"
docker exec age_treebench psql -U postgresUser -d postgresDB -c "SELECT * FROM ag_catalog.alter_graph('artificial_trees_truebase_100000', 'RENAME', 'artificial_trees_truebase_100000_plain');"
docker exec age_treebench psql -U postgresUser -d postgresDB -c "SELECT * FROM ag_catalog.alter_graph('artificial_trees_ultratall_10', 'RENAME', 'artificial_trees_ultratall_10_plain');"
docker exec age_treebench psql -U postgresUser -d postgresDB -c "SELECT * FROM ag_catalog.alter_graph('artificial_trees_ultratall_100', 'RENAME', 'artificial_trees_ultratall_100_plain');"
docker exec age_treebench psql -U postgresUser -d postgresDB -c "SELECT * FROM ag_catalog.alter_graph('artificial_trees_ultratall_1000', 'RENAME', 'artificial_trees_ultratall_1000_plain');"
docker exec age_treebench psql -U postgresUser -d postgresDB -c "SELECT * FROM ag_catalog.alter_graph('artificial_trees_ultratall_10000', 'RENAME', 'artificial_trees_ultratall_10000_plain');"
docker exec age_treebench psql -U postgresUser -d postgresDB -c "SELECT * FROM ag_catalog.alter_graph('artificial_trees_ultratall_100000', 'RENAME', 'artificial_trees_ultratall_100000_plain');"
docker exec age_treebench psql -U postgresUser -d postgresDB -c "SELECT * FROM ag_catalog.alter_graph('artificial_trees_ultrawide_10', 'RENAME', 'artificial_trees_ultrawide_10_plain');"
docker exec age_treebench psql -U postgresUser -d postgresDB -c "SELECT * FROM ag_catalog.alter_graph('artificial_trees_ultrawide_100', 'RENAME', 'artificial_trees_ultrawide_100_plain');"
docker exec age_treebench psql -U postgresUser -d postgresDB -c "SELECT * FROM ag_catalog.alter_graph('artificial_trees_ultrawide_1000', 'RENAME', 'artificial_trees_ultrawide_1000_plain');"
docker exec age_treebench psql -U postgresUser -d postgresDB -c "SELECT * FROM ag_catalog.alter_graph('artificial_trees_ultrawide_10000', 'RENAME', 'artificial_trees_ultrawide_10000_plain');"
docker exec age_treebench psql -U postgresUser -d postgresDB -c "SELECT * FROM ag_catalog.alter_graph('artificial_trees_ultrawide_100000', 'RENAME', 'artificial_trees_ultrawide_100000_plain');"
docker exec age_treebench psql -U postgresUser -d postgresDB -c "SELECT * FROM ag_catalog.alter_graph('snb_sf1', 'RENAME', 'snb_sf1_plain');"



docker exec age_treebench psql -U postgresUser -d postgresDB -c "SELECT * FROM ag_catalog.alter_graph('artificial_forests_40', 'RENAME', 'artificial_forests_40_plain');"
docker exec age_treebench psql -U postgresUser -d postgresDB -c "SELECT * FROM ag_catalog.alter_graph('artificial_forests_40', 'RENAME', 'artificial_forests_40_plain');"


tmux new -s age-init -d './experiments/age/run_age_experiment.sh --clean' && tmux attach -t age-init

tmux new -s age-init -d './experiments/age/run_age_experiment.sh --no-docker' && tmux attach -t age-init


cd /home/daniel/trees-in-graphs-bench                                                                                                                                                                                                                    
python3 -c "                                                                                                                                                                                                                                             
from pathlib import Path                                                                                                                                                                                                                                 
from experiments.experiement_infrastructure.AssessmentLogic import get_all_graph_names                                                                                                                                                                   
                                                                                                                                                                                                                                                           
sql_path = Path('docker/age/sql_setup')                                                                                                                                                                                                                  
graphs = get_all_graph_names('_dewey', sql_path)                                                                                                                                                                                                         
print(f'Found {len(graphs)} graphs:')                                                                                                                                                                                                                    
for g in sorted(graphs):                                                                                                                                                                                                                                 
    print(f'  {g}')                                                                                                                                                                                                                                      
print()                                                                                                                                                                                                                                                  
print('sf1_dewey in list:', 'sf1_dewey' in graphs)                                                                                                                                                                                                       
"

for schema in sf1_dewey artificial_forest_40_dewey truebase_10_dewey truebase_100_dewey truebase_1000_dewey truebase_10000_dewey truebase_100000_dewey ultratall_10_dewey ultratall_100_dewey                
  ultratall_1000_dewey ultratall_10000_dewey ultratall_100000_dewey ultrawide_10_dewey ultrawide_100_dewey ultrawide_1000_dewey ultrawide_10000_dewey ultrawide_100000_dewey; do                               
    if [[ "$schema" == "sf1_dewey" ]]; then                                                                                                                                                                    
      node="Comment"                                                                                                                                                                                           
    else                                                                                                                                                                                                       
      node="TreeNode"                                                                                                                                                                                          
  fi                                                                                                                                                                                                         
  echo "Creating index on $schema.$node..."                                                                                                                                                                  
  docker exec age_treebench psql -U postgresUser -d postgresDB -c "CREATE INDEX treenode_string_id_prefix_idx ON $schema.\"$node\" USING BTREE ((agtype_access_operator(VARIADIC ARRAY[properties,           
  '\"string_id\"'::agtype])) text_pattern_ops);"                                                                                                                                                               
done


