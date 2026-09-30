# RocketMQ source

The `rocketmq` source consumes messages from iQIYI RocketMQ 5.x through the
RocketMQ Java `SimpleConsumer`. The client dependency uses
`org.apache.rocketmq:rocketmq-client-java:5.0.0`, matching the RocketMQ 5.x
release notes.

The iQIYI RocketMQ repository currently documents client version `5.0.0`. That
client can block shared async workers during long polling on very small
containers, so avoid single-core deployments for this source unless the client
version is upgraded.

The default `await_duration` is 15 seconds to stay within the common RocketMQ
Proxy long-polling ceiling of 20 seconds. If your proxy is configured with a
different `grpcClientConsumerMaxLongPollingTimeoutMillis`, keep
`await_duration` at or below that limit.

Sensitive consumer settings are read from environment variables by default:

* `ROCKETMQ_CG`: RocketMQ CG/access key
* `ROCKETMQ_TOKEN`: RocketMQ token/secret key
* `ROCKETMQ_GROUP`: RocketMQ consumer group

End-to-end acknowledgements are enabled by default. In this mode, the source
acknowledges a RocketMQ message only after the event is positively acknowledged
by downstream pipeline components. Setting `acknowledgments: false` changes this
behavior to acknowledge after the event is written to the in-memory buffer; this
can lose messages if the process exits before sinks finish writing.

Messages that repeatedly fail mapping or buffer writes are retried until
`max_delivery_attempts`, then acknowledged and skipped with the message id in
the error log so one bad message does not block later messages in the batch.

Example pipeline:

```yaml
rocketmq-pipeline:
  source:
    rocketmq:
      endpoints: "proxy001.rocketmq.qiyi.middle:8081;proxy002.rocketmq.qiyi.middle:8081"
      topic: "my-topic"
      tag: "*"
      max_messages: 16
      max_delivery_attempts: 5
      acknowledgments: true
      await_duration: "15s"
      invisible_duration: "30s"
      acknowledgment_timeout: "2m"
      close_timeout: "5s"
  sink:
    - stdout:
```

The source writes the message body to the `message` field as UTF-8 text and adds
RocketMQ metadata under `rocketmq`.
