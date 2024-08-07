from typing import Any, List, Union

import spacy
from spacy import Language
from spacy.tokens import Doc

from typing import Any


class Parser:

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        """
        Calls the `parse` method with the provided arguments.

        Args:
            *args (Any): Variable length argument list.
            **kwds (Any): Arbitrary keyword arguments.

        Returns:
            Any: The result of the `parse` method.
        """
        return self.parse(*args, **kwds)
    
    def minimize_tokens_count(self, doc: Doc, nlp: Language) -> str:
        """
        Removes stop words from a given Doc object and returns the resulting string.

        Parameters:
            doc (Doc): The Doc object to process.
            nlp (Language): The spaCy Language object used to access the stop words.

        Returns:
            str: The resulting string after removing stop words.
        """
        stop_words = nlp.Defaults.stop_words
        min_qx = [token.text for token in doc if token.is_alpha and token.text.lower() not in stop_words]
        return "".join(min_qx)
    
    def get_nouns_only(self, doc: Doc) -> List[str]:
        """
        Get a list of nouns from a given spaCy Doc object.

        Parameters:
            doc (Doc): The spaCy Doc object to extract nouns from.

        Returns:
            List[str]: A list of nouns extracted from the Doc object.
        """
        nouns = [token.text for token in doc if token.pos_ == 'NOUN']
        return nouns
    
    def parse(self, qx: str, nlp: Language) -> Union[None, List[str]]:
        """
        Parses the given query string using the provided spaCy Language object.

        Args:
            qx (str): The query string to parse.
            nlp (Language): The spaCy Language object used for parsing.

        Returns:
            Union[None, List[str]]: A list of nouns extracted from the parsed query string,
            or None if the parsed query string is empty after removing stop words.
        """
        doc = nlp(qx)
        min_qx = self.minimize_tokens_count(doc, nlp)
        if not min_qx:
            return None
        return self.get_nouns_only(nlp(min_qx))
        


# Example usage
if __name__ == "__main__":
    import time
    import tqdm

    s = time.perf_counter()
    nlp = spacy.load("en_core_web_sm")
    parser = Parser()
    requests = ["What is the weather like in New York City next week?", "What about the weather in May?", "What about the weather in May?", "What is standing in New York City next week?", "Somewhere in New York City next week"]
    for request in tqdm.tqdm(requests, "Parsing texts..."):
        s_ = time.perf_counter()
        print(f"====Request: {request}=====")
        nouns = parser(request, nlp)
        print("nouns:", nouns)
        e_ = time.perf_counter()
        print(f"Time for request: {e_ - s_:0.4f} seconds\n")
    e = time.perf_counter()

    print(f"Time for all: {e - s:0.4f} seconds")