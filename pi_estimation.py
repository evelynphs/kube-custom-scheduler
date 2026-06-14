import random
import sys

PI = 3.1415926535

def estimate_pi(n_points):
    points_in_circle = 0

    for _ in range(n_points):
        x = random.random() * random.choice([-1, 1])
        y = random.random() * random.choice([-1, 1])
        if x**2 + y**2 <= 1:
            points_in_circle += 1

    pi_estimate = (points_in_circle / n_points) * 4
    return pi_estimate

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python monte_carlo_pi.py <number_of_points> [seed]")
        print("Example: python monte_carlo_pi.py 20000")
        print("Example: python monte_carlo_pi.py 20000 42")
        sys.exit(1)

    n_points = int(sys.argv[1])
    seed = int(sys.argv[2]) if len(sys.argv) >= 3 else 42
    random.seed(seed)

    pi_estimate = estimate_pi(n_points)