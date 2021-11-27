from fuzzywuzzy import fuzz

def is_similar_name(requested_author: str, returned_author: str) -> bool:
    r = fuzz.ratio(requested_author.lower(),returned_author.lower())
    print('')
    print (f'Scoring of r  {r}')
    print('')
    return True if r > 50 else False 
