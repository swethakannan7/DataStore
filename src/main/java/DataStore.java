import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.Iterator;
import java.util.Scanner;
import java.util.concurrent.Semaphore;

public class DataStore {
    String filename;
    int maxlength = 32;
    int ttl=0;
    static String DataStorefilepath = "Untitled";

    public static void main(String[] args) throws IOException, ParseException {


        Scanner user_input = new Scanner(System.in);

        System.out.println("Enter Key: ");
        String key = user_input.nextLine();
        if(key.length()>32)
            key = key.substring(0,32);

        System.out.println("Enter JSON File path: ");
        String JSONfilepath = user_input.nextLine();

        System.out.println("Enter DataStore filepath: ");
        DataStorefilepath = user_input.nextLine();
        if(DataStorefilepath.equals("")){
            DataStorefilepath = "Untitled";
        }

        JSONObject getFile = new JSONObject();
        JSONParser parser = new JSONParser();
        Object obj = parser.parse(new FileReader(JSONfilepath));
        getFile =  (JSONObject) obj;

//        System.out.println("Do you want to assign TTL? ");
//        String option = user_input.nextLine();

//         if(option.equals("Yes")){
//             System.out.println("Enter TTL? ");
//             ttl = user_input.nextInt();
//         }
//         else
//             ttl = 0;

        DataStoreOperations d = new DataStoreOperations();

        d.createKey(key, getFile);

        JSONObject result = new JSONObject();
        System.out.println("Enter key to read: ");
        String toreadkey = user_input.nextLine();
        result = d.readKey(toreadkey);
        System.out.println(result);

        System.out.println("Enter key to delete: ");
        String todeletekey = user_input.nextLine();
        d.deleteKey(todeletekey);

    }
}

class DataStoreOperations {
    static Semaphore semaphore = new Semaphore(1);
    DataStore d = new DataStore();

    public void createKey(String key, JSONObject value) throws IOException, ParseException {
//        if(ttl!=0){
//            System.out.println("INSIDE TTL OPTION");
//            DataStoreEssentials d = new DataStoreEssentials();
//            d.push(key, ttl);
//        }

        int flag=0;
        File file = new File(d.DataStorefilepath);

        JSONObject obj = new JSONObject();
        JSONArray kvpair = new JSONArray();

        obj.put(key,value);

        if(file.length()==0){
            kvpair.add(obj);
        }

        else
        {
            JSONParser parser = new JSONParser();
            FileReader reader = new FileReader(d.DataStorefilepath);
            JSONArray jsonArray = (JSONArray) parser.parse(reader);

            Iterator<?> i = jsonArray.iterator();

            while (i.hasNext()) {
                JSONObject new_array = (JSONObject) i.next();

                boolean result = new_array.containsKey(key);
                if(result){
                    flag = 1;
                    break;
                }

                else {
                    System.out.println("Does it contain the key" + new_array.containsKey(key));
                    kvpair.add(new_array);
                }
            }
            kvpair.add(obj);
        }
        if(flag==0){
            FileWriter file_writer = new FileWriter(file);
            BufferedWriter bw = new BufferedWriter(file_writer);
            bw.write(kvpair.toJSONString());
            bw.close();
        }
        else
            System.out.println("Key already exists");

    }

    public JSONObject readKey(String key) throws IOException, ParseException {
        int flag = 0;
        System.out.println(key);
        JSONParser parser = new JSONParser();
        JSONObject obj = new JSONObject();
        try {
            FileReader reader = new FileReader(d.DataStorefilepath);
            JSONArray jsonArray = (JSONArray) parser.parse(reader);

            Iterator<?> i = jsonArray.iterator();

            while (i.hasNext()) {
                obj = (JSONObject) i.next();
                boolean result = obj.containsKey(key);
                if(result){
                    return obj;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        JSONObject message = new JSONObject();
        message.put(key, "Key doesn't exist");
        return message;
    }

    public void deleteKey(String key) throws IOException, ParseException {

        try {

            semaphore.acquire();

            int flag = 0;
            File file = new File(d.DataStorefilepath);

            JSONObject obj = new JSONObject();
            JSONArray kvpair = new JSONArray();
            JSONParser parser = new JSONParser();
            FileReader reader = new FileReader(d.DataStorefilepath);
            JSONArray jsonArray = (JSONArray) parser.parse(reader);
            Iterator<?> i = jsonArray.iterator();
            try {

                while (i.hasNext()) {
                    JSONObject new_array = (JSONObject) i.next();
                    boolean result = new_array.containsKey(key);
                    if (result) {
                        flag = 1;
                        kvpair.remove(key);
                    } else if (!new_array.isEmpty() && !new_array.containsKey(key))
                        kvpair.add(new_array);

                }
                if (flag != 0) {
                    FileWriter file_writer = new FileWriter(file);
                    BufferedWriter bw = new BufferedWriter(file_writer);
                    bw.write(kvpair.toJSONString());
                    bw.close();
                } else
                    System.out.println("Key doesn't exists");
            } finally {
                semaphore.release();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

//class DataStoreEssentials{
//    Node head;
//    class Node{
//        String key;
//        Date expiryTime;
//        Node next;
//        Node(String key, Date expiryTime){
//            key = key;
//            expiryTime = expiryTime;
//            next = null;
//        }
//    }
//
//    public Date calculateExpiry(int ttl){
//        System.out.println("INSIDE CALCULATE CALENDAR");
//        Calendar cal = Calendar.getInstance();
//        Date now = cal.getTime();
//        if(ttl==0){
//
//        }
//        cal.add(Calendar.SECOND, ttl);
//        Date later = cal.getTime();
//        System.out.println(later);
//        return later;
//    }
//
//    public void push(String key, int ttl){
//        System.out.println("INSIDE PUSH");
//        Date expiry_date = calculateExpiry(ttl);
//        Node new_node = new Node(key, expiry_date);
//        new_node.next = head;
//        head = new_node;
//    }
//}
