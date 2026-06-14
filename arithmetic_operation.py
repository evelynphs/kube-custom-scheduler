import math
import sys

def compute(a, b, c, d):
    results = {}

    results["sqrt(log10(a * b * c))"]         = math.sqrt(math.log10(a * b * c))
    results["ln(sqrt(a^2 + b^2))"]            = math.log(math.sqrt(a**2 + b**2))
    results["log10(ln(d) * sqrt(c))"]         = math.log10(math.log(d) * math.sqrt(c))
    results["cbrt(a^3 + b^3 + c^3)"]         = (a**3 + b**3 + c**3) ** (1/3)

    results["sqrt(ln(cbrt(a*b*c*d)))"]        = math.sqrt(math.log((a * b * c * d) ** (1/3)))
    results["log10(sqrt(a^2 + b^2 + c^2))"]  = math.log10(math.sqrt(a**2 + b**2 + c**2))
    results["ln(log10(a*c) + sqrt(b+d))"]    = math.log(math.log10(a * c) + math.sqrt(b + d))

    results["sqrt(ln(log10(a*b) + cbrt(c*d)))"] = math.sqrt(
        math.log(math.log10(a * b) + (c * d) ** (1/3))
    )
    results["ln(sqrt(log10(a^b) + c/d))"]    = math.log(
        math.sqrt(math.log10(a**b) + c / d)
    )
    results["cbrt(ln(a*d) * log10(b*c) + sqrt(a+c))"] = (
        math.log(a * d) * math.log10(b * c) + math.sqrt(a + c)
    ) ** (1/3)

    return results

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("Usage: python complex_math.py <a> <b> <c> <d>")
        print("Example: python complex_math.py 4 8 100 20")
        sys.exit(1)

    a, b, c, d = float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4])

    results = compute(a, b, c, d)