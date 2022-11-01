# Recipes

Each recipe is a self-contained module.

## Recipe overview

1. [Deserializing JSON from Kafka](kafka-json-to-pojo) -> [Detailed explanation](https://docs.immerok.cloud/docs/cookbook/deserializing-json-from-kafka-to-apache-flink-pojo/)
2. [Continuously Reading CSV Files](continuous-file-reading) -> [Detailed explanation](https://docs.immerok.cloud/docs/cookbook/continuously-reading-csv-files-with-apache-flink/)
3. [Exactly-once with Apache Kafka®](kafka-exactly-once) -> [Detailed explanation](https://docs.immerok.cloud/docs/cookbook/exactly-once-with-apache-kafka-and-apache-flink/)
4. [Writing an Application in Kotlin™](kotlin) -> [Detailed explanation](https://docs.immerok.cloud/docs/cookbook/writing-application-in-kotlin/)
5. [Joining and Deduplicating Data](table-deduplicated-join) -> [Detailed explanation](https://docs.immerok.cloud/docs/cookbook/joining-and-deduplicating-data/)
6. [Splitting Apache Kafka® events](split-stream) -> [Detailed explanation](https://docs.immerok.cloud/docs/cookbook/splitting-apache-kafka-events-to-different-outputs-with-apache-flink/)
7. [Capturing Late Data](late-data-to-sink) -> [Detailed explanation](https://docs.immerok.cloud/docs/cookbook/capturing-late-data-and-send-to-separate-sink-with-apache-flink/)
8. [Creating Dead Letter Queues](kafka-dead-letter) -> [Detailed explanation](https://docs.immerok.cloud/docs/cookbook/creating-dead-letter-queues-from-and-to-apache-kafka-with-apache-flink/)
9. [Using Session Windows](session-window) -> [Detailed explanation](https://docs.immerok.cloud/docs/cookbook/using-session-windows/)
10. [Serializing Timestamped Data](latest-transaction) -> [Detailed explanation](https://docs.immerok.cloud/docs/cookbook/serializing-timestamped-data/)
11. [Reading Apache Kafka Headers](kafka-headers) -> [Detailed explanation](https://docs.immerok.cloud/docs/cookbook/reading-apache-kafka-headers-with-apache-flink/)
12. [Upgrading Flink (Table API)](compiled-plan) -> [Detailed explanation](https://docs.immerok.cloud/docs/cookbook/compiled-plan/)
13. [Alerting when Problems Persist](pattern-matching-cep) -> [Detailed explanation](https://docs.immerok.cloud/docs/cookbook/pattern-matching-with-cep/)
14. [Migrating state away from Kryo](kryo-migration) -> [Detailed explanation](https://docs.immerok.cloud/docs/cookbook/migrating-state-away-from-kryo/)
15. [Write Parquet Files](write-parquet-files) -> [Detailed explanation](https://docs.immerok.cloud/docs/cookbook/read-from-apache-kafka-write-to-parquet-files-with-apache-flink/)

### Requirements

* Maven
* Java 11

### IDE setup

#### Spotless

_These instructions assume you use IntelliJ IDEA._

* Install the latest version of the [google-java-format plugin](https://plugins.jetbrains.com/plugin/8527-google-java-format)
    * Note: _For the time being_ the custom version used in Flink development is also fine.
* Enable the google-java-format as explained [here](https://github.com/google/google-java-format#intellij-android-studio-and-other-jetbrains-ides)
* Settings/Preferences -> Editor -> Code style
    * Tick "Enabled Editorconfig support"
    * Scala/Kotlin -> Imports -> Clear the import layout at the bottom
        * This is only relevant for the Scala/Kotlin recipes; you can likely skip this and just call spotless from the command-line
* Settings/Preferences -> Tools -> Actions on Save
  * Enable "Reformat code" and select Java/Scala/Kotlin file types
  * Enable "Optimize imports"

Note: You can also run `mvn spotless:apply` on the command-line / IDE to reformat the code.
