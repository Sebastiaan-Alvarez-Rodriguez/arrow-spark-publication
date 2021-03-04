# Credits
Here we list the credits, grouped on functionality/topic.

## Hooking into Spark
We looked at [MongoDB-Spark connector](https://github.com/mongodb/mongo-org.apache.spark),
to find out how a program should hook into Spark.
Many thanks to [MongoDB](https://www.mongodb.com/) for creating and open-sourcing this project.

## Accessing Arrow from JVM Languages
We modified an experimental bridge to
access [Arrow Datasets](https://github.com/zhztheplayer/arrow-1/tree/ARROW-7808),
using the Java Native Interface to communicate with the C++ Dataset implementation.
Many thanks to [Hongze Zhang](https://github.com/zhztheplayer) for building this bridge.