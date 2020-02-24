import com.mongodb.client.FindIterable;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoDBJDBC{
    public static void main( String args[] ){
        try{
            // 连接到 mongodb 服务
            MongoClient mongoClient = new MongoClient( "localhost" , 27017 );

            // 连接到数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase("goodsdb");
            System.out.println("mongo数据库连接成功");

            MongoCollection<Document> collection = mongoDatabase.getCollection("goodsinfo");
            //测试插入数据
            //collection.insertOne(new Document().append("name","迪丽热巴"));
            //查询数据
            FindIterable<Document> documents = collection.find();
            System.out.println(documents);


            }catch(Exception e){
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }
    }
}