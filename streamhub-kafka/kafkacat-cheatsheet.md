# Kafkacat (KCat) Cheatsheet

## List Brokers with metadata

```shell
user@workstation:~$ kcat -b localhost:9092,localhost:9093,localhost:9094 -L
```

## Single consumer with OffsetOldest

```shell
user@workstation:~$ kcat -C -b localhost:9092 -t TOPIC_NAME -o beginning
```

## Single consumer with OffsetOldest and message metadata

```shell
user@workstation:~$ kcat -C -b localhost:9092 -t TOPIC_NAME -o beginning \
  -f '\nKey (%K bytes): %k
  Value (%S bytes): %s
  Timestamp: %T
  Partition: %p
  Offset: %o
  Headers: %h\n'
```
