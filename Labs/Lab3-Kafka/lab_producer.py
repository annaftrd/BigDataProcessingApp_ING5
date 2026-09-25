# %%
import socket
from confluent_kafka import Producer

# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)

# %%
topic='book'
book_file='pg4363.txt'

# %% Send the book line by line
with open(book_file, encoding='utf-8-sig') as f:
  for line in f:
    line = line.rstrip('\n')
    if not line.strip():  # skip empty lines
      continue
    producer.produce(
      topic=topic,
      value=line
    )
    producer.poll(0)

producer.flush()
print(f"Done: book sent to '{topic}'")
