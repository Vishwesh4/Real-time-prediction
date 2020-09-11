import os
from google.cloud import pubsub_v1
import time
from google.cloud import storage

# publisher = pubsub_v1.PublisherClient.from_service_account_json("./gcloudcredentials.json")
publisher = pubsub_v1.PublisherClient()
topic_name = 'projects/big-data-lab-266809/topics/{topic}'.format(
    project_id=os.getenv('GOOGLE_CLOUD_PROJECT'),
    topic='to-kafka',  # Set this to something appropriate.
)

#publisher.create_topic(topic_name)
# client = storage.Client.from_service_account_json("./gcloudcredentials.json")
client = storage.Client()

bucket = client.get_bucket("ch16b024")
blob = bucket.get_blob("small_temp.csv")
print("Loading data...")
x = blob.download_as_string()
x = x.decode('utf-8')
data = x.split('\n')
print("Done. Pushing data to kafka server...")
for lines in data[1:]:
    if len(lines)==0:
        break
    #Sleeps for 10 seconds
    time.sleep(10)
    publisher.publish(topic_name, lines.encode(), spam=lines)
