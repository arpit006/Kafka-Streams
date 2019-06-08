package com.kafka.streamsapp.reader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class FileReader {

    public static String readFileAsString(String inFilePath) {
        File file = new File(inFilePath);
        byte[] bytes = new byte[0];
        try {
            bytes = Files.readAllBytes(file.toPath());
            return new String(bytes, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }
}
