package assignment4;

import java.io.*;

public class FileHandler {


    public static String readFileData(String filepath) throws IOException {
        String output = "";

        try(BufferedReader br = new BufferedReader(new FileReader(filepath))) {
            for(String line; (line = br.readLine()) != null; ) {
                output += line;
            }
            // line is not visible here.
        }

        return output;
    }




    public static void writeToFile(String filepath, String data) throws FileNotFoundException, UnsupportedEncodingException {
        PrintWriter writer = new PrintWriter(filepath, "UTF-8");
        writer.println(data);
        writer.close();
    }


}

