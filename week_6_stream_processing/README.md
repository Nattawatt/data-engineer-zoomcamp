# WEEK6 STREAM PROCESSING KAFKA

## What is stream processing

Exchange the data via someplace like notice board and cusomers who interested in that topic will get the message from producer.

![Alt text](images/notice_board.jpg)

Data exchange in stream processing

Actually, It is the same like notice board but it happen in computer and a bit more complicated.

![Alt text](images/stream_processing.JPG)

## What is Kafka

![Alt text](images/notice_board_kafka.jpg)

Producer - The one who create message or event

Consumer - The one who recieve message or event

What is a topic?

![Alt text](images/topic.JPG)

The topic contains the message from event along a time.

Componance of messages

![Alt text](images/message.JPG)

**Why Kafka ???**

### robustness or reliability

- even if your nodes are going down you will still ascess the data because kafka have replication so it replicates data.

### flexibility

- Topic can small or big
- You can have hundreds of consumers
- A lot of Integration

### Scalability

- Kafka can scale with no limit

