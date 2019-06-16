import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.Semaphore;

public class DataStore {
    public static void main(String args[]) throws Exception {
        String key, dataFilepath, JSONFilepath;
        JSONObject jsonObject = new JSONObject();
        key = "";
        int maxlength = 32;
        DataStoreOperations dataStoreOperations = new DataStoreOperations();
        System.out.println("Enter DataStore file path: ");      //     filepath : /Users/swetha/DataStore/myData
        Scanner user_input = new Scanner(System.in);
        dataFilepath = user_input.nextLine();
        
        File file = dataStoreOperations.getFile(dataFilepath);

        if (file.length()!=0)
            dataStoreOperations.makeHashMap(dataFilepath);

        int choice = 0;
        do{
            System.out.println("Options \n Enter (1) create, (2) read, (3) delete (4) Exit menu");
            choice = user_input.nextInt();
            user_input.nextLine();
            if(choice==1){
                System.out.println("Enter key to create: ");
                key = user_input.nextLine();
                if (key.length() > maxlength)
                    key = key.substring(0, maxlength);
                if(key.isEmpty()){
                    System.out.println("Invalid key");
                    break;
                }
                System.out.println("Enter JSON Object Filepath: ");//           /Users/swetha/JSONData/example_1.json
                JSONFilepath = user_input.nextLine();

                File JSONfile = dataStoreOperations.getFile(JSONFilepath);
                if(JSONfile.length()>16000 || JSONFilepath.isEmpty()){
                    System.out.println("File either empty or exceeds size");
                    break;
                }
                dataStoreOperations.createKey(key, JSONFilepath);
            }

            else if(choice==2){

                System.out.println("Enter key to read: ");
                key = user_input.nextLine();
                jsonObject = dataStoreOperations.readKey(key);
                System.out.println("Value: " + jsonObject.toString());
            }

            else if(choice==3){

                System.out.println("Enter key to delete: ");
                key = user_input.nextLine();
                dataStoreOperations.deleteKey(key);
            }
            else{
                dataStoreOperations.pushHashMap(dataStoreOperations.dataMap, dataFilepath);
                choice = 4;
            }


        }while(choice!=4);

    }
}

class DataStoreOperations implements Serializable {
    String key;
    JSONObject value = new JSONObject();
    static Semaphore semaphore = new Semaphore(1);
    static int flag = 0;

    Map<String, JSONObject> dataMap = new HashMap<String, JSONObject>();

    File getFile(String filepath){
       File file = new File(filepath);
       return file;
    }

    void makeHashMap(String filepath) throws IOException {
        InputStream is = Files.newInputStream(Paths.get(filepath));
        ObjectInputStream  op  = new ObjectInputStream(is);
        Map<String, JSONObject> data = new HashMap<String, JSONObject>();
        Map<String, JSONObject> data1 = new HashMap<>();
        try {
            while (true) {
                data = (Map<String, JSONObject>) op.readObject();
                if (data instanceof DataStoreOperations)
                    break;
            }
        } catch (StreamCorruptedException | ClassNotFoundException | EOFException e){
            System.out.println(" ");
        }
        dataMap = data;
    }

    void createKey(String key, String JSONFilepath) throws IOException, ParseException {
        JSONParser parser = new JSONParser();
        Object obj = parser.parse(new FileReader(JSONFilepath));
        JSONObject value= (JSONObject) obj;
        dataMap.put(key, value);
        flag = 1;
    }

    JSONObject readKey(String key){
        JSONObject value = new JSONObject();
        String message = "Key doesn't exist";
        if(dataMap.containsKey(key)){
            value = dataMap.get(key);
        }
        else {
            value.put(key, "Key doesn't exist");
        }
        return value;
    }

    void deleteKey(String key){
        if(dataMap.containsKey(key)){
            dataMap.remove(key);
            flag = 1;
        }
        else
            System.out.println("Key doesn't exist");
    }

    void pushHashMap(Map<String, JSONObject> map, String filepath) throws IOException, InterruptedException {
        FileOutputStream fileOutputStream = new FileOutputStream(filepath);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
        if (flag == 1) {
            semaphore.acquire();
            objectOutputStream.writeObject(dataMap);
            semaphore.release();
        }
    }
}
