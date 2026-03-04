import sys
import string

def word_count(text):
    text = text.lower()
    text = text.translate(str.maketrans('', '', string.punctuation))
    words = text.split()

    counts = {}
    for word in words:
        counts[word] = counts.get(word, 0) + 1

    return counts

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python word_count.py <text>")
        print('Example: python word_count.py "Hello world hello"')
        sys.exit(1)

    text = ' '.join(sys.argv[1:])
    counts = word_count(text)