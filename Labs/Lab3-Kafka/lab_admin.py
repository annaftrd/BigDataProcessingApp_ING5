# %%
import os
import urllib.request
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

# %% Download the book from Project Gutenberg
url = 'https://www.gutenberg.org/cache/epub/4363/pg4363.txt'
book_file = 'pg4363.txt'

if not os.path.exists(book_file):
    urllib.request.urlretrieve(url, book_file)
    print(f"Download done : {book_file}")
else:
    print(f"Already downloaded : {book_file}")

# %%
config =  {
  'bootstrap.servers': 'localhost:9092',
}

admin_client = AdminClient(config)

# %% Create the topic (wait for the result so errors are not silently lost)
topic='book'
futures = admin_client.create_topics(
  [NewTopic(topic, num_partitions=1, replication_factor=1)]
)
try:
    futures[topic].result()
    print(f"Topic '{topic}' created")
except KafkaException as e:
    if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
        print(f"Topic '{topic}' already exists")
    else:
        raise

# %%
x = admin_client.list_topics()
for  t in x.topics.keys():
  print(t)

# %%
#admin_client.delete_topics([topic])

# %%
