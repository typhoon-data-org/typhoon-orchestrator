import pprint

def my_echo(x):
    print("--**--")
    print("ECHO")
    print("--**--")
    print("Type:", type(x))
    print("--**--")
    pprint.pprint(str(x))
    print("--**--")
    print("--**--")
    print("")
    return x

# def echo(x):
#     print("--**--")
#     print("ECHO", x)
#     print("--**--")
#     return x
