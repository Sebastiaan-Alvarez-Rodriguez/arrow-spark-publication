### Requirements
There are a few requirements to get this project compiling.
All of these requirements come from building Arrow:
`openjdk-8-jdk openjdk-11-jdk libboost-all-dev automake bison flex g++ libevent-dev libssl-dev libtool make pkg-config maven`.

**Note**:
Once the Arrow JNI connector is integrated into Arrow, we won't have to build anymore. 
Additionally, there will be no special requirements.
This installation guide is for all the people out there who cannot wait any longer to use the Dataset API in Spark. 



### A quick but important note
This implementation uses the Arrow Dataset API, `C++` implementation, and an experimental `JNI` bridge (included in [/arrow](/arrow)) to port functions to the JVM.

This means that we have to compile `C++` code, greatly reducing portability of the final `.jar` file.

 > For best performance, compile this project on the machine you want to execute this code on.
 > If that is not possible, try to compile on a machine closely resembling the target machine (same OS, OS versions, compiler versions, glibc, etc).
 
 > For best portability, compile on a relatively old machine. 
 > Compiling on very old systems generally means the generated output works on both old and newer machines, due to backward compatibility often present in newer systems.


### Building Dependencies
For building dependencies, we assume we build in a directory pointed to by `$BUILDDIR`.
First, we prepare the Arrow implementation with Java Native Interface bridge (C++ side):
```bash
cd $BUILDDIR
git clone https://github.com/Sebastiaan-Alvarez-Rodriguez/arrow.git -b OAP-1852-DAS5
cd $BUILDDIR/arrow/cpp
cmake . -DARROW_PARQUET=ON -DARROW_DATASET=ON -DARROW_JNI=ON -DARROW_ORC=ON -DARROW_CSV=ON
sudo make install -j12
```
Next, we prepare the Arrow implementation with Java Native Interface bridge (Java side):
```bash
cd $BUILDDIR/arrow/java/
mvn clean install -P arrow-jni -pl format,memory,vector -am -Darrow.cpp.build.dir=$BUILDIR/arrow/cpp/build/release -Dmaven.test.skip=true -Dcheckstyle.skip -Dos.detected.name=linux -Dos.detected.arch=x86_64 -Dos.detected.classifier=linux-x86_64
```
 > If you are compile on other architectures than Linux, manually change the Maven OS identifiers yourself

### Building Modules
*Once* the dependencies have been built, we can start building our modules.
There exist 3 modules:
 1. [`arrow-spark-connector`](/arrow-spark-connector) is the main module. It contains our connector.
 2. [`arrow-spark-benchamrk`](/arrow-spark-benchmark) is a secondary module. It provides us a commandline interface which allows us to benchmark our connector.
 3. [`arrow-spark-test`](/arrow-spark-test) is a secondary module. It contains unit-tests for our connector.

Here, we will describe how to build the project's main module, [`arrow-spark-connector`](/arrow-spark-connector).
The other modules can be built in an equivalent way.

Now, we assume the root dir of the clone of this repository is pointed to by `$PROJECTDIR`.
Finally, we build our system, using 1 of the following 3 options.



#### Option 1: Full build
This build will contain all dependencies to sustain itself. The `jar` file is a bit large because of this. 
 ```bash
$PROJECTDIR/gradlew -b $PROJECTDIR/arrow-spark-connector/build.gradle shadowJar
```
Once completed, our main output is `$PROJECTDIR/arrow-spark-connector/build/libs/arrow-spark-connector-1.0-all.jar`.


#### Option 2: Light build
When deploying applications using `spark-submit`, Spark tends to copy the submitted `jar` file to the work directories of all worker nodes.
In circumstances like these, it would be great to have a smaller `arrow-spark-connector` module.
To decrease the size of this module, we provide another build target:
```bash
$PROJECTDIR/gradlew -b $PROJECTDIR/arrow-spark-connector/build.gradle lightJar
```
This `jar` is more lightweight, because it uses Spark-3.0.1 libraries available when using `spark-submit`.
Note that Spark may change its available libraries in the future, making the system incompatible.
The jars we *require in the class-path* are as follows:
```
scala-library-2.12.10.jar
scala-reflect-2.12.10.jar
scala-xml_2.12-1.2.0.jar
spark-core_2.12-3.0.1.jar
spark-sql_2.12-3.0.1.jar
avro-1.8.2.jar
hadoop-common-2.7.4.jar
```
Normally, Spark provides these libraries to us, when using `spark-submit`.

Once completed, our produced output is `$PROJECTDIR/arrow-spark-connector/build/libs/arrow-spark-connector-1.0-light.jar`.

#### Option 3: Essential build
Should you be really finicky with `jar` sizes, and you are willing to do extra work, you can build the smallest `jar` possible like so:
```bash
$PROJECTDIR/gradlew -b $PROJECTDIR/arrow-spark-connector/build.gradle essentialJar
``` 
This jar is as lightweight as it gets. It ships no dependencies at all (except for the JNI bridge).
It is up to the user to provide **all** following dependencies to the `jar` *in the classpath*:
```
scala-library-2.12.10.jar
scala-reflect-2.12.10.jar
scala-xml_2.12-1.2.0.jar
spark-core_2.12-3.0.1.jar
spark-sql_2.12-3.0.1.jar
avro-1.8.2.jar
hadoop-common-2.7.4.jar

log4j-api-scala_2.12-12.0.jar
log4j-api-2.12.0.jar
log4j-core-2.12.0.jar
```
(Note: The top 7 ones are provided by Spark when executing `spark-submit`)
Our suggestion is to place all listed dependency `jar` files inside `<spark-root>/jars/`.
Sparks class-path variable points to that directory, which means that the master and worker nodes should be able to find the dependencies there.
  
Once completed, our produced output is `$PROJECTDIR/build/libs/arrow-spark-connector-1.0-essential.jar`.

#### A final word about classpaths
Many new users to the JVM try to execute a `jar` named `x.jar`, while simultaneously changing the classpath like:
```bash
java -cp some-dep-1.0.0.jar -jar x.jar
```
This is **not** valid.
`jar` files specify their own `Class-Path` variable in their manifest. `java` only considers that classpath, and does not regard `-cp` flags.

In our generated `jar` files, you can satisfy dependencies for `x.jar` by placing all dependency `.jar` files
in the same directory as `x.jar`.

Alternatively, in the context of Spark, it *is* possible to specify classpath variables when using `spark-submit`, by using:
```bash
/path/to/spark-submit
--class org.path.to.Mainclass
--conf spark.driver.extraClassPath=some-dep-1.0.0.jar,some-other-dep.jar
--conf spark.executor.extraClassPath=some-dep-1.0.0.jar,some-other-dep.jar
x.jar
```

In fact, we can even send our own jars to spark workers, if needed:
```bash
/path/to/spark-submit
--class org.path.to.Mainclass
--jars "some-dep-1.0.0.jar,some-other-dep.jar"
x.jar
```
Any jars we sent do not have to add them to the classpath, because Spark already handles that.
*Note*, However, it makes not much sense to make a very tiny jar so you don't have to send all dependencies along, and then manually sending dependencies along.
It might be more wise to use the `--conf` variant above, and make the classpath point to some jars in a directory that all nodes can reach.