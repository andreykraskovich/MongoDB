import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.List;

public class Mongo {
    private static MongoDatabase database;

    public void init(String title) {
        MongoClient mongoClient = new MongoClient("127.0.0.1", 27017);
        database = mongoClient.getDatabase(title);
    }

    public MongoCollection<Document> getCollection(String title) {
        return database.getCollection(title);
    }

    public void addStudents(List<String[]> studentsCVS, MongoCollection<Document> studentsMongo) {
        for (String[] line : studentsCVS) {
            String name = line[0];
            String age = line[1];
            String courses = line[2];

            Document student =
                    new Document().append("name", name).append("age", age).append("courses", courses);

            studentsMongo.insertOne(student);
        }
    }

    public void deleteDatabase(String title) {
        database.drop();
    }
}
