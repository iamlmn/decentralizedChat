package org.gossip.services;
import java.util.HashMap;

public class RandomNameGenerator {

    private static HashMap<Integer, String> map = new HashMap<>();

    public RandomNameGenerator() {
        map = new HashMap<>();
        map.put(8080, "Yoda");
        map.put(8081, "Lara" );
        map.put(8082, "Sarah");
        map.put(8083, "Sana");
        map.put(8084, "Lala" );
        map.put(8085, "Bob");
        map.put(8086, "Eminem");
        map.put(8087, "Harry" );
        map.put(8088, "Kim");
        map.put(8089, "Kylie");
        map.put(8090, "Justin" );
        map.put(8091, "Drake");
        map.put(8092, "Obama");
        map.put(8093, "Crusher" );
        map.put(8094, "Tom");
        map.put(8095, "Jerry");
        map.put(8096, "Jessy" );
        map.put(8097, "Jerry");
        map.put(8098, "Walter");
        map.put(8099, "White" );

    }
    
    public static String getUserName(int port) {
        return map.get(port);
    }
}


        