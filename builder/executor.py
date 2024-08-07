from py2neo import Graph

from utils_ops.envHandler import getenv

class QueryExecutor:
    def __init__(self):
        """
        Initializes the QueryExecutor instance.

        This function creates an instance of the QueryExecutor class and initializes the graph_connection_executor attribute with a Graph object. The Graph object is created using the NEO4J_URI and NEO4J_USER environment variables.

        Parameters:
            None

        Returns:
            None
        """

        self.graph_connection_executor = Graph(
            getenv("NEO4J_URI"),
            auth=(getenv("NEO4J_USER"), getenv("NEO4J_PASSWORD")),
        )

    def __call__(self, *args, **kwargs):
        """
        Calls the `result` method with the provided arguments.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            The result of the `result` method.
        """
        return self.result(*args, **kwargs)

    def result(self, query: str):
        """
        Executes a query on the graph connection executor and returns the data.

        Args:
            query (str): The query to be executed.

        Returns:
            The data returned by the query.

        Raises:
            Any exceptions raised by the graph connection executor.
        """
        return self.graph_connection_executor.run(query).data()