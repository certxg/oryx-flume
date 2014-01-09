# oryx-flume

A custom [Apache Flume](http://flume.apache.org/) Sink implementation that sends events to an instance of Cloudera Oryx's serving layer. Oryx adds the events to its in-memory model for near real-time recommendations.

## Getting started

1. **Build oryx-flume**

```bash
$ git clone https://github.com/cloudera/oryx-flume.git
$ cd oryx-flume
$ mvn clean package
$ ls target
oryx-flume-[version].jar    
```

2. **Add jar to the Flume classpath**

```bash
$ scp target/oryx-flume-[version].jar [user]@[flume.agent.host]:/home/[user]
```

On the Flume Agent host:

Create a plugins.d directory. The plugins.d directory is typically located at `$FLUME_HOME/plugins.d`. Or if using Cloudera Manager `/usr/lib/flume-ng/plugins.d` or `/var/lib/flume-ng/plugins.d`

```bash
$ cd $FLUME_HOME/plugins.d
$ mkdir -p oryx-flume/lib
mv ~/oryx-flume-[version].jar oryx-flume/lib
```

## Configuration

Example Flume Agent configuration:

```
[AgentName].sinks.[SinkName].type = com.cloudera.oryx.contrib.flume.OryxEventSink
[AgentName].sinks.[SinkName].channel = MemChannel
[AgentName].sinks.[SinkName].batchSize = 100
[AgentName].sinks.[SinkName].oryxHostname = oryx.serving.layer.com
[AgentName].sinks.[SinkName].oryxPort = 80
[AgentName].sinks.[SinkName].oryxEndpoint = /ingest
[AgentName].sinks.[SinkName].oryxEventParser = com.cloudera.oryx.contrib.flume.OryxJSONEventParser
[AgentName].sinks.[SinkName].oryxFields = $.user.id, $.product.product-code, 1.0
```

* `.batchSize`: The maximum number of events to take from the channel per transaction. This is an optional property that defaults to `100` when not set.

* `.oryxHostname`: The hostname running the Oryx serving layer instance.

* `.oryxPort`: The port the Oryx serving layer instance is listening on. This is an optional property that defaults to `80` when not set.

* `.oryxEndpoint`: The endpoint path for Oryx's REST API. This is an optional property that defaults to `/ingest` when not set.

* `.oryxEventParser`: An `OryxEventParser` implementation. Parses a Flume Event and extracts the configured fields to send to Oryx (see `.oryxFields`). The current version of this sink ships with a JSON event parser: `com.cloudera.oryx.contrib.flume.OryxJSONEventParser`. You can create your own event parser by implementing the `com.cloudera.oryx.contrib.flume.OryxEventParser` interface.

* `.oryxFields`: A list of fields to extract from an event and send to Oryx. Multiple `.oryxFields` can be specified by using a numeric postfix (i.e. exploding an event):
    
```
.oryxFields = user,item[,strength]
.oryxFields.0 = user,item0[,strength0]
.oryxFields.1 = user,item1[,strength1]
.oryxFields.2 = user,item2[,strength2]
```

`OryxJSONEventParser` uses [`JsonPath`](https://code.google.com/p/json-path) to extract the configured fields from the JSON objects and therefore `.oryxFields` must be specified as paths.
For example, to extract `user-id` and `product-code` from the following JSON object:

```json
{
  "user": {
    "id": "fd156752df3d",
    "email": "kinley@cloudera.com"
  },
  "product": {
    "group": "running shoes",
    "product-name": "Inov-8 Road-X Lite",
    "product-code": "488589e7c994",
    "search-terms": "natural running"
  }
} 
```

`.oryxFields` should be defined in the Flume configuration as follows:

```
[AgentName].sinks.[SinkName].oryxFields = $.user.id, $.product.product-code, 1.0
```

See the JsonPath site for more information: https://code.google.com/p/json-path
