# Text cleaning and parsing for the messages received from Kafka
# (same steps as the word count lab, applied line by line instead of on an RDD)

# %%
from collections import Counter

# %%
stop_words = set(['the', 'a', 'an', 'of', 'in', 'it', 'to', 'and', 'or', 'is',
                  'are', 'was', 'were', 'be', 'been', 'being', 'for', 'on',
                  'with', 'as', 'by', 'at', 'from'])
# Lab 1 punctuation + the typographic quotes and dashes used in Gutenberg books
punctuation = '.,!:;?()[]{}"\'-_`/*“”‘’—'

# %%
def clean_line(line):
    line = line.replace('—', ' ').replace('--', ' ')  # dashes join words: split them
    words = line.lower().split()                     # lower case + split on spaces
    words = [w.strip(punctuation) for w in words]    # remove punctuation
    return [w for w in words if w != '' and w not in stop_words]  # remove blanks and stop words

# %%
def write_word_count(word_count, count_file):
    # sorted from most to least frequent word
    with open(count_file, 'w', encoding='utf-8') as out:
        for word, count in word_count.most_common():
            out.write(f"{word}\t{count}\n")

# %% Quick test without Kafka
if __name__ == '__main__':
    example = 'The Project Gutenberg eBook of “Beyond Good and Evil”—by Nietzsche.'
    print(clean_line(example))
    print(Counter(clean_line(example)).most_common())
