from fuzzywuzzy import fuzz
import json

import re

def clean_filename(input_str):
    return re.sub('[^-a-zA-Z0-9]', '_', input_str)

def is_similar_name(requested_author: str, returned_author: str, similarity_threshold) -> bool:
    r = fuzz.ratio(requested_author.lower(),returned_author.lower())
    print('')
    print (f'{requested_author} vs {returned_author} - Similarity score:  {r}')
    print('')
    return True if r > similarity_threshold else False 

def json_dumps(batch)->str:
    return json.dumps(batch)

