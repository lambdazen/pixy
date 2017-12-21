Pixy is a declarative vendor-independent graph query language that works on any Blueprints-compatible graph database. 
Please refer to the [Wiki](https://github.com/lambdazen/pixy/wiki) for more information. 

Pixy is available under the liberal Apache 2.0 license and is developed and maintained by 
[LambdaZen](http://lambdazen.com), the maker of the [Bitsy Graph Database](https://github.com/lambdazen/bitsy).

## Building it

Use latest [Apache Maven](https://maven.apache.org/) to build this project. Requires at least version 3.5.0.

For quick build (runs no tests nor any other plugin like javadoc)

```
mvn clean install -Dtest=void
```

For UT-only build (will run UTs too)

```
mvn clean install
```

For full build (will run UTs and ITs)

```
mvn clean install -Dit
```
