# Kappa Bluemix
A single query to process historic and realtime data.

Kappa Bluemix is a prototype implementation of a [Kappa Architecture](http://milinda.pathirage.org/kappa-architecture.com/)  using IBM Message Hub as the immutable log. Batch and streamed data are both processed identically, meaning developers only need write one query and set of logic.

An Elastic Search-like query is posted, which returns a websocket, through which the current result and and any future updates will be sent.

### How It Works
Kappa Bluemix assumes all data is stored in JSON format on a Message Hub queue. When a query is submitted a new consumer is created and it's position is set to the start of the queue (where the earliest message is). A URL to a websocket connection is returned to the client that submitted the query. Kappa Bluemix then processes all the messages in the queue in the order they were added and outputs the result to the websocket connection.  

Once the process has reached the front of the queue (where the newest message is) it keeps the websocket connection open and waits for any new data to be added to the queue, automatically processing it and updating the client.

Queries are shared across clients, so multiple clients with identical queries make use of the same consumer and processing. If the client closes the websocket connection and there are no other clients using the same query, Kappa Bluemix will stop waiting for more data to arrive and shutdown the consumer.

### API
Three types of query can be submitted, *count*, *search* and *group*, by posting JSON query data to ```/rest/query/count```, ```/rest/query/search``` and ```/rest/query/group```. On posting a query a websocket URL will be returned, the client should open a connection to the websocket to receive the result.

### Query Format
The query data is loosely based on the [Elastic Search](https://www.elastic.co/guide/en/elasticsearch/reference/current/_introducing_the_query_language.html) query format.

#### Match
Used when running a count or search query of data in Message Hub. Matches against a specific field name and value.
```json
{
  "match": {
    "field name": "value"
  }
}
```

e.g.
```json
{
  "match": {
    "twitter_username": "@shawdm"
  }
}
```

#### Sort
Applied to a search query to order the output. Sort element is an array of sorting criteria (currently only a single sort criteria is implemented). A sorting criteria object specifies the field that should be sorted on, the order (ascending or descending) and a limit of how many results should be returned.
```json
{
  "match": {
    "field name": "value"
  },
  "sort":[{
    "field name" : {
      "order" : "desc|asc",
      "limit":10
      }
  }]
}
```

e.g.
```json
{
  "match": {
    "field name": "username"
  },
  "sort":[{
    "timestamp" : {
      "order" : "desc|asc",
      "limit":10
    }
  }]
}
```

#### Group
Group data based on a field value and show the count for each grouping.  Additional sort attribute allows the grouped counts to be ordered.  A group operation outputs a special *count* field which is not contained in the source data.

```json
{
  "group_by":{
    "field":"field name"
  },
  "sort":[{
    "count" : {
      "order" : "desc|asc",
      "limit":10
    }
  }]
}
```

e.g.

```json
{
  "group_by":{
    "field":"username"
  },
  "sort":[{
    "count" : {
      "order" : "desc|asc",
      "limit":10
    }
  }]
}
```


## Prerequisites
To build and run the sample, you must have the following installed:
* [git](https://git-scm.com/)
* [Gradle](https://gradle.org/)
* Java 7+
* [Message Hub Service Instance](https://console.ng.bluemix.net/catalog/services/message-hub/) provisioned in [IBM Bluemix](https://console.ng.bluemix.net/)

## Building the Sample
Install the project using gradle:

```shell
gradle build war
 ```

You should see a directory called `target` created in your project home directory. A WAR file is created under `target/defaultServer`, as well as a copy of the server.xml file.

## Deployment Prerequisites
To deploy applications using the IBM WebSphere Application Server Liberty Buildpack, you are required to accept the IBM Liberty license and IBM JRE license by following the instructions below:

1. Move `manifest.yml.template` to `manifest.yml`
2. Read the current IBM [Liberty-License][http://public.dhe.ibm.com/ibmdl/export/pub/software/websphere/wasdev/downloads/wlp/8.5.5.7/lafiles/runtime/en.html] and the current IBM [JVM-License][http://www14.software.ibm.com/cgi-bin/weblap/lap.pl?la_formnum=&li_formnum=L-JWOD-9SYNCP&title=IBM%C2%AE+SDK%2C+Java+Technology+Edition%2C+Version+8.0&l=en].
3. Extract the `D/N: <License code>` from the Liberty-License and JVM-License.
4. Add the following environment variables and extracted license codes to the `manifest.yml` file in the directory from which you push your application. For further information on the format of
the `manifest.yml` file refer to the [manifest documentation][].

```yaml
env:
    IBM_JVM_LICENSE: <jvm license code>
    IBM_LIBERTY_LICENSE: <liberty license code>
```

## Deploy the Sample to Bluemix
Now we can push the app to Bluemix:
```shell
cf push
 ```

## Deploy the Sample to Local Liberty
* add VCAP_SERVICES in to server.env
* install bin/installUtility install wssecurity-1.1

## Building and Deploying Docker Container

### Build an image from the Dockerfile
```docker build -t kappa-bluemix-image:0.1 .```

### Test Image Locally
```docker run -p 127.0.0.1:9080:9080 --name kappa-bluemix-container -t kappa-bluemix-image:0.1```

# Todo
* OS license.
* Design for running queries in parallel (or move to openwhisk).
* Move to ETS github.
