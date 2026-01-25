#!/bin/bash

# Configuration
CONTAINER="age_treebench"  # Replace with your actual container name
DB_USER="postgresUser"
DB_NAME="postgresDB"
SQL_DIR="./graph_init_sql"  # Directory containing your .sql files
DATA_DIR="./data"
CONTAINER_DATA_PATH="/age/graph_data"

# Copy data directory to container if it doesn't exist
if ! docker exec "$CONTAINER" test -d "$CONTAINER_DATA_PATH"; then
    echo "Data directory not found in container. Copying data..."
    docker cp "$DATA_DIR" "$CONTAINER:$CONTAINER_DATA_PATH"
    if [ $? -eq 0 ]; then
        echo "✓ Successfully copied data to container"
    else
        echo "✗ Error copying data to container"
        exit 1
    fi
else
    echo "Data directory already exists in container"
fi

# Ensure AGE extension is loaded
echo "Loading AGE extension..."
docker exec "$CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -c "CREATE EXTENSION IF NOT EXISTS age; LOAD 'age'; SET search_path = ag_catalog, '\$user', public;"

# Function to convert types in batches
convert_types_batch() {
    local graph_name=$1
    local label=$2
    local set_clause=$3
    local batch_size=25000
    local total_processed=0
    local processed=1

    echo "  Converting types for $label in $graph_name..."

    while [ "$processed" -gt 0 ]; do
        processed=$(docker exec "$CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -t -A -c "
            SELECT * FROM cypher('$graph_name', \$\$
                MATCH (n:$label)
                WHERE n._converted IS NULL
                WITH n LIMIT $batch_size
                SET $set_clause,
                    n._converted = true
                RETURN count(n)
            \$\$) as (count agtype);
        " 2>/dev/null | tr -d '[:space:]')

        # Handle empty or error response
        if [ -z "$processed" ] || ! [[ "$processed" =~ ^[0-9]+$ ]]; then
            processed=0
        fi

        total_processed=$((total_processed + processed))

        if [ "$processed" -gt 0 ]; then
            echo "    Processed batch: $processed (total: $total_processed)"
        fi
    done

    # Remove temporary marker
    docker exec "$CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -c "
        SELECT * FROM cypher('$graph_name', \$\$
            MATCH (n:$label)
            WHERE n._converted IS NOT NULL
            REMOVE n._converted
            RETURN count(n)
        \$\$) as (count agtype);
    " > /dev/null 2>&1

    echo "  ✓ Converted $total_processed nodes for $label"
}

# Execute each SQL file
for file in "$SQL_DIR"/*.sql; do
    if [ -f "$file" ]; then
        echo "========================================="
        echo "Executing: $(basename $file)"
        echo "========================================="

        docker exec -i "$CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" < "$file"

        if [ $? -eq 0 ]; then
            echo "✓ Successfully executed $(basename $file)"
        else
            echo "✗ Error executing $(basename $file)"
            exit 1
        fi

        # Extract graph name from filename (remove _creation.sql)
        filename=$(basename "$file")
        graph_name="${filename%_creation.sql}"

        # Get vertex labels for this graph
        labels=$(docker exec "$CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -t -A -c "
            SELECT name FROM ag_catalog.ag_label
            WHERE graph = (SELECT graphid FROM ag_catalog.ag_graph WHERE name = '$graph_name')
            AND kind = 'v'
            AND name NOT IN ('_ag_label_vertex');
        " 2>/dev/null)

        # Determine which attributes to convert based on filename
        if [[ "$filename" == *"_ir_"* ]]; then
            set_clause="n.integer_id = toInteger(n.integer_id), n.upper_bound = toInteger(n.upper_bound), n.height = toInteger(n.height), n.depth = toInteger(n.depth)"
        elif [[ "$filename" == *"_s_"* ]]; then
            set_clause="n.height = toInteger(n.height), n.depth = toInteger(n.depth)"
        else
            echo "  Skipping type conversion (no _ir_ or _s_ in filename)"
            labels=""
        fi

        # Convert types for each vertex label
        for label in $labels; do
            if [ -n "$label" ]; then
                convert_types_batch "$graph_name" "$label" "$set_clause"
            fi
        done

        echo ""
    fi
done

echo "All SQL files executed successfully!"