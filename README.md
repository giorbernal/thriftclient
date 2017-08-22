# thriftclient
JDBC Client to execute queries in a Spark Thrift Server
To execute this simple client, a thrift-server must be prevoiously started.
Next commands must be executed:
> mvn clean package

extract the generated *-assembly.zip file, unzip it and execute:
> run.sh <thriftserver_ipAddress> <thriftserver_port>

This client can access to this [server](https://github.com/giorbernal/thriftserver-concept)

In the generated *lib* folder can be found the **driver** and another .jar requiered to create a connection to a Thrift-Server by using a standard client such **DBVisualizer** or **SQuirreL**.