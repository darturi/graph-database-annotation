import time
from abc import abstractmethod, ABC
import psycopg2
import kuzu

class Executor(ABC):
    @abstractmethod
    def execute_query(self, query_string):
        pass

    @abstractmethod
    def update_db(self, new_dbname):
        pass

    @abstractmethod
    def build_string_index(self, index_name='string_id_index'):
        pass

    @abstractmethod
    def build_ir_index(self, index_name='ir_index'):
        pass

    @abstractmethod
    def collect_query_plan(self, query_string):
        pass


class ApacheExecutor(Executor):
    # def __init__(self, dbname, user='danielarturi', password='', host='localhost', port=5432):
    def __init__(self, dbname, user : str, password : str, host : str, port : int):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

        self.conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        self.conn.autocommit = True

        self.cursor = self.conn.cursor()

        # Load AGE extension
        self.cursor.execute("CREATE EXTENSION IF NOT EXISTS age;")
        self.cursor.execute("LOAD 'age';")
        self.cursor.execute('SET search_path = ag_catalog, "$user", public;')

    def execute_query(self, query_string : str):
        start = time.perf_counter()
        self.cursor.execute(query_string)
        result = self.cursor.fetchall()
        end = time.perf_counter()

        return (end - start) * 1000, result

    def update_db(self, new_dbname : str):
        old_dbname = self.dbname
        self.dbname = new_dbname

        self.conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        self.conn.autocommit = True

        self.cursor = self.conn.cursor()

        # Load AGE extension
        self.cursor.execute("LOAD 'age';")
        self.cursor.execute('SET search_path = ag_catalog, "$user", public;')

        print(f"updated connection from {old_dbname} to {new_dbname}")

    def build_string_index(self, index_name='string_id_index'):
        pass

    def build_ir_index(self, index_name='ir_index'):
        pass

    def collect_query_plan(self, query_string : str):
        _, plan = self.execute_query(f"EXPLAIN {query_string}")
        time_elapsed, _ = self.execute_query(query_string)

        return time_elapsed, plan


class KuzuExecutor(Executor):
    def __init__(self, db_path: str):
        """
        Initialize Kuzu executor with a database path.

        Args:
            db_path: Path to the Kuzu database directory
        """
        self.db_path = db_path
        self.db = kuzu.Database(db_path)
        self.conn = kuzu.Connection(self.db)

    def execute_query(self, query_string: str):
        """
        Execute a Cypher query and return timing and results.

        Returns:
            tuple: (time_in_ms, list of result tuples)
        """
        start = time.perf_counter()
        result = self.conn.execute(query_string)

        # Fetch all results
        results = []
        while result.has_next():
            results.append(result.get_next())

        end = time.perf_counter()

        return (end - start) * 1000, results

    def update_db(self, new_db_path: str):
        """
        Switch to a different Kuzu database.

        Args:
            new_db_path: Path to the new Kuzu database directory
        """
        old_path = self.db_path
        self.db_path = new_db_path

        # Close existing connection and open new one
        del self.conn
        del self.db

        self.db = kuzu.Database(new_db_path)
        self.conn = kuzu.Connection(self.db)

        print(f"Updated connection from {old_path} to {new_db_path}")

    def build_string_index(self, index_name='string_id_index'):
        """Build an index on the string_id property."""
        try:
            self.conn.execute(f"CREATE INDEX {index_name} ON TreeNode(string_id)")
            print(f"Created index {index_name} on TreeNode(string_id)")
        except Exception as e:
            print(f"Index creation failed (may already exist): {e}")

    def build_ir_index(self, index_name='ir_index'):
        """Build an index on the integer_id property."""
        try:
            self.conn.execute(f"CREATE INDEX {index_name} ON TreeNode(integer_id)")
            print(f"Created index {index_name} on TreeNode(integer_id)")
        except Exception as e:
            print(f"Index creation failed (may already exist): {e}")

    def collect_query_plan(self, query_string: str):
        """
        Get query execution plan and timing.

        Returns:
            tuple: (time_in_ms, query plan as string)
        """
        # Get the plan using EXPLAIN
        plan_result = self.conn.execute(f"EXPLAIN {query_string}")
        plan_lines = []
        while plan_result.has_next():
            plan_lines.append(plan_result.get_next())

        # Execute and time the actual query
        time_elapsed, _ = self.execute_query(query_string)

        return time_elapsed, plan_lines

    def profile_query(self, query_string: str):
        """
        Profile a query to get detailed execution statistics.

        Returns:
            tuple: (time_in_ms, profile output)
        """
        start = time.perf_counter()
        result = self.conn.execute(f"PROFILE {query_string}")

        profile_lines = []
        while result.has_next():
            profile_lines.append(result.get_next())

        end = time.perf_counter()

        return (end - start) * 1000, profile_lines
