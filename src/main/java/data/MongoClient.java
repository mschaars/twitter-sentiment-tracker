package data;

import com.mongodb.*;

import java.net.UnknownHostException;

/**
 * Created by Michael on 25.11.2014.
 */
public class MongoClient {

    private static com.mongodb.MongoClient mongoClient = null;

    public static DB getDBInstance(String host, String database, WriteConcern writeConcern) {
        if (mongoClient == null) {
            try {
                mongoClient = new com.mongodb.MongoClient(host, 27017);
                mongoClient.setWriteConcern(writeConcern);

            } catch (UnknownHostException e) {
                System.out.println("Failed to connect to MongoDB server");
            }
        }

        return mongoClient.getDB(database);
    }

}
