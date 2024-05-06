- A demo project for learning kafka's feature:
+ Topics
+ Producer: Callback, produce message with keys
+ Consumer: Consumer inside group, Incremental Cooperative Rebalance, Static group membership

- A demo kafka broker run on WSL:
+ command: kafka-server-start.sh ~/kafka_2.13-3.7.0/config/kraft/server.properties => run the kafka server

- Note: In case having problem when connecting to kafka server run on WSL (localhost) from Java applition run following
+ 1. sudo sysctl -w net.ipv6.conf.all.disable_ipv6=1
+ 2. sudo sysctl -w net.ipv6.conf.default.disable_ipv6=1
