# Nested function definitions and call invocations to test CallBuilder
def calculate_double(val):
    return val * 2

def process_value(val):
    doubled = calculate_double(val)
    return doubled

res = process_value(10)
print(res)
