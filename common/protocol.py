from enum import Enum
import logging

class QueryType(Enum):
    QUERY1 = "Query1"
    QUERY2 = "Query2"
    QUERY3 = "Query3"
    QUERY4 = "Query4"
    QUERY5 = "Query5"
        
    @staticmethod    
    def validate_query_type(line):
        for query_type in QueryType:
            if line == query_type.value:
                return query_type.value
        
        logging.info(f"[!] Wrong first message: {query_type}...")
        return None
