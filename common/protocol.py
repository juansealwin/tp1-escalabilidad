from enum import Enum
import logging

class QueryType:
    class Enum(Enum):
        QUERY1 = "Query1"
        QUERY2 = "Query2"
        QUERY3 = "Query3"
        QUERY4 = "Query4"
        QUERY5 = "Query5"
        
    def validate_query_type(line):
        for query_type in QueryType.Enum:
            if line == query_type.value:
                return query_type.value
        
        logging.info(f"[!] Wrong first message: {line}...")
        return None
