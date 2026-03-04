import numpy as np
import sys

def schoolbook(array_first, array_second):
    n = len(array_first)
    result = np.zeros((n, n))
    for i in range(n):
        for j in range(n):
            for k in range(n):
                result[i, j] += array_first[i, k] * array_second[k, j]
    return result

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python matrix_mult.py <n> <val_a> <val_b>")
        print("Example: python matrix_mult.py 5 1 2")
        sys.exit(1)

    n     = int(sys.argv[1])
    val_a = int(sys.argv[2])
    val_b = int(sys.argv[3])

    array_first  = np.full((n, n), val_a)
    array_second = np.full((n, n), val_b)

    output = schoolbook(array_first, array_second)
