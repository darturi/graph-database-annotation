SELECT *
FROM cypher('$GRAPHNAME', $$
MATCH (root:$NODE_TYPE {__id__: $rootID})<-[:$REL_TYPE*1..]-(leaf:$NODE_TYPE)
WHERE NOT EXISTS((leaf)<-[:$REL_TYPE]-())
RETURN leaf
$$) as (leaf agtype);