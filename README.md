# Easy Commit re-implementation
This is the implementation of the Easy Commit commitment protocol by Group 7 of the Distributed Systems
course of 2020-2021 at the faculty of EEMCS of the Delft University of Technology.
This codebase was created to reproduce [this paper](https://openproceedings.org/2018/conf/edbt/paper-65.pdf) 
by Gupta and Sadoghi.

# Scala console

After launching `sbt` you can switch to the _scala-console_. There we took care that you
get an already initialized Vert.x-instance and the necessary imports to start playing around.

```
sbt
> console
scala> vertx.deployVerticle(nameForVerticle[l.tudelft.distribted.ec.HttpVerticle])
scala> vertx.deploymentIDs
```

From here you can freely interact with the Vert.x API inside the sbt-scala-shell.


# Fat-jar

Take a look at the _build.sbt_ and search for the entry _packageOptions_. Enter the fully qualified class name 
of your primary verticle. This will be used as entry point for a generated fat-jar.

To create the runnable fat-jar use:
```
sbt assembly
```


# Dockerize

The project also contains everything you need to create a Docker-container. Simply run the following command to package your fat-jar inside a Docker-container

```
sbt docker
```

To run use

```
docker run -p 8666:8666 default/vertx-scala
```

Point your browser to [http://127.0.0.1:8666/hello](http://127.0.0.1:8666/hello) and enjoy :)
