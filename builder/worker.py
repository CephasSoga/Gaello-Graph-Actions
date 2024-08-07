import time
import subprocess
import multiprocessing
from typing import Any, List,  Dict
from functools import lru_cache
import concurrent.futures as futures

import spacy

from builder.parser import Parser
from builder.ops import kwds_similarity
from queries.subimtor import Submitor
from builder.executor import QueryExecutor

NLP_MODEL = "en_core_web_sm"
CACHE_SIZE = 128
DOWNLOAD_TIME_IN_SECONDS = 20
MATCH_THRESHOLD = 0.4  # tolerant if < 0.5, intolerant otherwise # make it tolerant to avoid false negatives
FILTER_THRESHOLD = 0.6 # make it intolerant to avoid false positives
CORE_MULTIPLIER = 2 # showed th best performaces on a machine of following specificiations: {System Type:x64-based PC, CPU(s):1 Processor(s) Installed, Intel64 Family 6 Model 78 Stepping 3 GenuineIntel ~2607 Mhz,  4 cores, 16GiB RAM}
FETCH_LIMIT = 100 # Can grow arbitraly as graph get bigger
SEARCH_DEPTH = 2 # too much bigger is not performence wise beneficial

class GraphManipulator:
    def __init__(self):
        """
        Initializes the GraphManipulator object.

        This method attempts to load the spacy model specified by the `NLP_MODEL`
        constant. If the model is not found, it downloads the model using the
        `spacy download` command. After downloading the model, it loads the model
        and assigns it to the `nlp` attribute of the object.

        The method also initializes the `parser`, `submitor`, `graph_executor`, and
        `cpu_cores` attributes of the object.

        Parameters:
            None

        Returns:
            None
        """

        try: 
            _ = spacy.load(NLP_MODEL)
        except IOError:
            subprocess.run(["python",  "-m", "spacy", "download", NLP_MODEL])
            time.sleep(DOWNLOAD_TIME_IN_SECONDS)
        finally:
            self.nlp = spacy.load(NLP_MODEL)

        self.parser = Parser()
        self.submitor = Submitor()
        self.graph_executor = QueryExecutor()
        self.cpu_cores = multiprocessing.cpu_count()

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        """
        Calls the `context` method with the provided arguments.

        Parameters:
            *args: Variable length argument list.
            **kwds: Arbitrary keyword arguments.

        Returns:
            Any: The result of the `context` method.
        """
        return self.context(*args, **kwds)

    @lru_cache(maxsize=CACHE_SIZE)
    def cache_parse(self, qx: str) -> List[str]:
        """
        Cache and parse the given query string.

        Args:
            qx (str): The query string to be parsed.

        Returns:
            List[str]: A list of parsed keywords from the query string.

        This function uses the `lru_cache` decorator to cache the parsed keywords for faster retrieval. The parsed keywords are obtained by calling the `parser` method with the query string and the `nlp` object.

        Note:
            The `CACHE_SIZE` parameter determines the maximum number of cached query strings.
        """
        return self.parser(qx, self.nlp)
    
    @staticmethod
    def match_nodes(id: str, node1_kwds: List[str], node2_kwds: List[str]):
        """
        Match two nodes based on their keyword similarity.

        Parameters:
            id (str): The ID of the node.
            node1_kwds (List[str]): A list of keywords associated with the first node.
            node2_kwds (List[str]): A list of keywords associated with the second node.

        Returns:
            str or None: The ID of the node if the keyword similarity is above the match threshold, otherwise None.
        """
        return id if  kwds_similarity(node1_kwds, node2_kwds) > MATCH_THRESHOLD else None
    
    @lru_cache(maxsize=CACHE_SIZE)
    def parallel_extraction(self, nodes: List[Dict]):
        """
        Extracts keywords from a list of nodes in parallel.

        Args:
            nodes (List[Dict]): A list of nodes, where each node is a dictionary with a 'content' key.

        Returns:
            Dict[str, List[str]]: A dictionary where the keys are the node IDs and the values are the extracted keywords.
        """
        extracted_kwds = {}

        max_workers = min(32, self.cpu_cores * CORE_MULTIPLIER) # Adjust as needed

        with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures_ = {executor.submit(self.cache_parse, node.get("content")): node for node in nodes}
            kwds = [fx.results() for fx in futures.as_completed(futures_)]

        for index, node in enumerate(nodes):
            extracted_kwds[node.get("id")] = kwds[index]

        return extracted_kwds
    
    @lru_cache(maxsize=CACHE_SIZE)
    def parallel_search(self, single_target_kwds: List[str], nodes_kwds: Dict[str, List[str]]):
        """
        This function performs a parallel search for a matching node in a graph based on keyword similarity.
        It uses a ThreadPoolExecutor to distribute the workload across multiple threads.

        Parameters:
        - single_target_kwds (List[str]): A list of keywords representing the target node to search for.
        - nodes_kwds (Dict[str, List[str]]): A dictionary where the keys are node IDs and the values are lists of keywords associated with each node.

        Returns:
        - str: The ID of the matching node if a match is found. If no match is found, returns None.
        """
        max_workers = min(32, self.cpu_cores * CORE_MULTIPLIER) # Adjust as needed
        with  futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures_ = {executor.submit(self.match_nodes, id, single_target_kwds,  node_kwds): node_kwds for id, node_kwds in nodes_kwds.items()}

            for fx in futures.as_completed(futures_):
                result = fx.result()
                if result is not None:
                    executor.shutdown(wait=True, cancel_futures=True)
                    return result
            return None
        
    def fetch_top_nodes(self, fetch_limit: int):
        """
        Fetches the top nodes from the graph based on the specified fetch limit.

        Args:
            fetch_limit (int): The maximum number of nodes to fetch.

        Returns:
            List[Dict]: A list of dictionaries representing the top nodes. Each dictionary contains the node information.
                        If no nodes are found, returns None.
        """
        result = self.graph_executor.result(
                self.submitor.fetch_most_recent_nodes(fetch_limit)
        )
        if not result:
            return None
        return [record["n"] for record in result if record]
        
    def find_first_match(self, request: str, fetch_limit: int):
        """
        Finds the first match for a given request in a graph of nodes.

        Args:
            request (str): The request to search for.
            fetch_limit (int): The maximum number of nodes to fetch in each iteration.

        Returns:
            str or None: The ID of the matching node if a match is found. If no match is found, returns None.
        """
        offset = 0
        round = 1
        match = None

        target_kwds = self.cache_parse(request)

        while not match:
            nodes = self.fetch_top_nodes(fetch_limit)

            if not nodes:
                break

            nodes_kwds = self.parallel_extraction(nodes)

            _result = self.parallel_search(target_kwds, nodes_kwds)
            if _result:
                match = _result
                break

            offset += fetch_limit
            round += 1

        return match if match else None
    

    def find_related_nodes(self, node_id: str, depth: int):
        """
        Finds the related nodes of a given node ID in a graph.

        Args:
            node_id (str): The ID of the node.
            depth (int): The depth of the search.

        Returns:
            list or None: A list of related node IDs if found, otherwise None.
        """
        _result = self.graph_executor(
            self.submitor.follow_path_from_root(node_id, depth)
        )

        if not _result:
            return None
        
        return [record.get("connected") for record in _result]
    

    def make_unrestricted_context(self, request):
        """
        Generates an unrestricted context for a given request.

        Args:
            request (Any): The request for which to generate the context.

        Returns:
            List[str] or None: A list of unrestricted context content strings, or None if no context is found.

        This function first finds the top match for the given request using the `find_first_match` method with a fetch limit.
        If no top match is found, it returns None.
        Otherwise, it finds the related nodes of the top match using the `find_related_nodes` method with a search depth.
        If no related nodes are found, it returns None.
        Finally, it returns a list of the content strings from the related nodes.
        """
        context = []
        top_match = self.find_first_match(request, FETCH_LIMIT)
        if not top_match:
            return None
        context = self.find_related_nodes(top_match, SEARCH_DEPTH)
        if not context:
            return None
        return [n.get("content") for n in context]
    
    def make_restricted_context(self, request, restricted_words):
        """
        Generates a restricted context based on the given request and restricted words.

        Args:
            request (str): The request string.
            restricted_words (List[str]): The list of restricted words.

        Returns:
            List[str] or None: The restricted context as a list of strings, or None if no context is found.
        """
        context = self.make_unrestricted_context(request)
        if not context:
            return None
        restricted_context = [c for c in context if not any(word in c.lower() for word in restricted_words)]
        if not restricted_context:
            return None
        return restricted_context

    def filter_context_by_relevance(self, request: str, context: List[str]) -> List[str]:
        """
        Filters the context list based on relevance to the request.

        Args:
            request: The user's search query.
            context: A list of document strings.

        Returns:
            A list of documents from the context that are relevant to the request.
        """
        request_keywords = self.cache_parse(request)
        filtered_context = []

        for document in context:
            document_keywords = self.cache_parse(document)
            score = self.match_nodes(request_keywords, document_keywords)
            if score >= FILTER_THRESHOLD:
                filtered_context.append(document)

        return filtered_context
    
    def context(self, request: str, mode: str = "filtered", restricted_words: List[str] = []):
        """
        Generate the context based on the given request and mode.

        Args:
            request (str): The user's search query.
            mode (str, optional): The mode to generate the context. Defaults to "filtered".
                - "unrestricted": Generate the unrestricted context.
                - "restricted": Generate the restricted context.
                - "filtered": Generate the filtered context.
            restricted_words (List[str], optional): The list of restricted words. Defaults to [].

        Returns:
            List[str] or None: The generated context as a list of strings, or None if no context is found.

        Raises:
            ValueError: If the mode is not one of "unrestricted", "restricted", or "filtered".
        """
        if mode == "unrestricted":
            return self.make_unrestricted_context(request)
        elif mode == "restricted":
            return self.make_restricted_context(request, restricted_words)
        elif mode == "filtered":
            ctx = self.make_unrestricted_context(request)
            return self.filter_context_by_relevance(request, ctx)
        else:
            raise ValueError("Invalid mode. Use 'unrestricted', 'restricted', or 'filtered'.")


