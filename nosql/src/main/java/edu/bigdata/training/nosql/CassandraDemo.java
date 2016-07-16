package edu.bigdata.training.nosql;

import com.datastax.driver.core.*;

/**
 *
 * @author myhome
 */
//Refer : "BigdataDemo" google doc refer "Cassandra demo" for more instructions
public class CassandraDemo {

    public static void main(String a[]) {
        Cluster cluster;
        Session session;

        // Connect to the cluster and keyspace "demo"
        cluster = Cluster.builder().addContactPoint("192.168.99.100").build();
        session = cluster.connect("demo");

        // Insert one record into the users table
        session.execute("INSERT INTO users (user_id, first, last, age) VALUES ('jsmith', 'John', 'Smith', 42)");

        // Use select to get the user we just entered
        ResultSet results = session.execute("SELECT * FROM users WHERE user_id='jsmith'");
        for (Row row : results) {
            System.out.format("%s %d\n", row.getString("first"), row.getInt("age"));
        }

        // Update the same user with a new age
        session.execute("update users set age = 36 WHERE user_id='jsmith'");

        // Select and show the change
        results = session.execute("select * from users WHERE user_id='jsmith'");
        for (Row row : results) {
            System.out.format("%s %d\n", row.getString("first"), row.getInt("age"));
        }

        // Delete the user from the users table
        session.execute("DELETE FROM users WHERE user_id='jsmith'");

        // Show that the user is gone
        results = session.execute("SELECT * FROM users");
        for (Row row : results) {
            System.out.format("%s %d %s %s %s\n", row.getString("last"), row.getInt("age"), row.getString("first"));
        }

        // Clean up the connection by closing it
        cluster.close();
    }
}
