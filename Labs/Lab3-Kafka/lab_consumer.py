# %%
from collections import Counter
from confluent_kafka import Consumer
from text_processing import clean_line, write_word_count

# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'book-cleaner',
        'auto.offset.reset': 'smallest',
        'enable.auto.commit': False}  # never save progress: each run re-reads the whole book

consumer = Consumer(conf)

# %%
topic='book'
consumer.subscribe([topic])

# %%
# Configuration
MAX_EMPTY_POLLS = 10  # Ends after ~10 seconds of silence
MAX_ERRORS = 5        # Ends after 5 consecutive errors
empty_polls = 0
error_count = 0

clean_file = 'book_clean.txt'
count_file = 'word_count.txt'
word_count = Counter()
lines_received = 0

with open(clean_file, 'w', encoding='utf-8') as out:
    while True:
        msg = consumer.poll(1.0)

        # 1. Handle "No Message" (Timeout)
        if msg is None:
            empty_polls += 1
            if empty_polls >= MAX_EMPTY_POLLS:
                print("Closing: No new messages received.")
                break
            continue

        # 2. Handle Errors
        if msg.error():
            error_count += 1
            print(f"Consumer error: {msg.error()}")
            if error_count >= MAX_ERRORS:
                print("Closing: Too many consecutive errors.")
                break
            continue

        # 3. Handle Success
        # Reset counters when we actually get data
        empty_polls = 0
        error_count = 0

        line = msg.value().decode('utf-8')
        words = clean_line(line)
        lines_received += 1
        if words:
            out.write(' '.join(words) + '\n')  # one cleaned line per message
            word_count.update(words)

# Clean up
consumer.close()

# %% Word count sorted from most to least frequent
write_word_count(word_count, count_file)

print(f"{lines_received} lines received, {len(word_count)} distinct words")
print(f"Cleaned text -> {clean_file}, word count -> {count_file}")
print("Top 10:", word_count.most_common(10))
