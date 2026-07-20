# Try-except exception flow structure to test basic parser tolerance
try:
    val = 1 / 0
except ZeroDivisionError:
    val = 0
print(val)
