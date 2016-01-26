# Cassandra Demo Application

### demo Keyspace

CREATE KEYSPACE demo WITH replication = {'class': 'SimpleStrategy','replication_factor': '1'};

### users table

CREATE TABLE users (firstname text, lastname text, age int, email text, city text, PRIMARY KEY (lastname));


#### References

* http://www.planetcassandra.org/try-cassandra/
* https://academy.datastax.com/demos/getting-started-apache-cassandra-and-java-part-i