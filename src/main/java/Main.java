import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import com.opencsv.exceptions.CsvException;
import org.bson.Document;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class Main {
    private static final String pathToCsv = "src/main/resources/mongo.csv";
    private static List<String[]> linesCSV;
    public static void main(String[] args) {
        MongoClient mongoClient = new MongoClient("localhost", 27017);
        Mongo mongo = new Mongo();
        mongo.init("Students");
        MongoCollection<Document> collectionStudent = mongo.getCollection("Students");


        List<String[]> linesCSV = getListCSV(pathToCsv);

        mongo.addStudents(linesCSV, collectionStudent);

        System.out.println("Общее кол-во студентов: " + collectionStudent.countDocuments() + "\n");

        FindIterable<Document> iterableGteForty =
                collectionStudent.find(new Document("age", new Document("$gte", "40")));

        System.out.println("Студенты, чей возраст старше 40:");
        for (Document doc : iterableGteForty) {
            System.out.println(doc);
        }

        System.out.println(
                "\n"
                        + "Самый молодой студент: "
                        + collectionStudent.find().sort(new BasicDBObject("age", 1)).first().getString("name"));

        System.out.println(
                "\n"
                        + "Курсы самого возвратного студента: "
                        + collectionStudent
                        .find()
                        .sort(new BasicDBObject("age", -1))
                        .first()
                        .getString("courses"));

    }

    public static List<String[]> getListCSV(String path) {
        try (Reader reader = Files.newBufferedReader(Paths.get(path))) {
            CSVParser parser =
                    new CSVParserBuilder()
                            .withSeparator(',')
                            .withFieldAsNull(CSVReaderNullFieldIndicator.EMPTY_QUOTES)
                            .build();
            CSVReader csvReader = new CSVReaderBuilder(reader).withCSVParser(parser).build();
            linesCSV = csvReader.readAll();
        } catch (java.io.IOException IOException) {
            IOException.printStackTrace();
        } catch (CsvException e) {
            e.printStackTrace();
        }
        return linesCSV;
    }
}
