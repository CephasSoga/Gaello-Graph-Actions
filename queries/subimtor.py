

class Submitor:

    @staticmethod
    def fetch_most_recent_nodes(limit: int):
        """
        Returns a Neo4j query string that fetches the most recent nodes from the graph database.

        Args:
            limit (int): The maximum number of nodes to return.

        Returns:
            str: A Neo4j query string that fetches the most recent nodes from the graph database.
        """
        return f"""
            MATCH (n) 
            RETURN n 
            ORDER BY n.timestamp DESC 
            LIMIT {limit}

        """
    
    @staticmethod
    def follow_path_from_root(id: int | str, depth: int):
        """
        Returns a Neo4j query string that fetches nodes connected to a root node within a specified depth.

        Args:
            id (int | str): The ID of the root node.
            depth (int): The maximum depth of the path to follow.

        Returns:
            str: A Neo4j query string that fetches the connected nodes.
        """
        return f"""
            MATCH (root:Node {{id: "{id}"}})-[*..{depth}]-(connected:Node)
            RETURN connected
        """