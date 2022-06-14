package assignment4;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class App {
    //---- Count cold, flu,  and snow in all cases -----------------------
    public static final String COLD_REGEX = "\\((cold|COLD|Cold),\\d+\\)";
    public static final String FLU_REGEX = "\\((flu|FLU|Flu),\\d+\\)";
    public static final String SNOW_REGEX = "\\((snow|SNOW|Snow),\\d+\\)";

    private static HashMap<String, Integer> countForEachWord;

    private static JavaSparkContext sparkContext;
    private static SparkConf sparkConf;

    public static void main(String[] args) {
        try {
            //-- delete the file containing reduced output --
            FileUtils.deleteDirectory(new File("CountData"));

        } catch (IOException e) {
            e.printStackTrace();
        }

        //Store the total counts against each keyword
        countForEachWord = new HashMap<String, Integer>();

        //Initialise
        countForEachWord.put("cold",0);
        countForEachWord.put("snow",0);
        countForEachWord.put("flu",0);

        try
        {
            String content = "";
            File dir = new File("inputFiles");
            File[] directoryListing = dir.listFiles();
            if (directoryListing != null) {
                for (File file : directoryListing) {
                    String fn = file.getName();
                    if(fn.contains(".json"))
                    {
                        content += FileHandler.readFileData("inputFiles/" + fn);
                    }
                }

                FileHandler.writeToFile("allTweets.json", content);
            }

            applyMapReduce("allTweets.json");
        }
        catch(Exception e)
        {
            e.printStackTrace();;
        }


        System.out.println("Count for word \"cold\" is "+ countForEachWord.get("cold"));
        System.out.println("Count for word \"flu\" is "+ countForEachWord.get("flu"));
        System.out.println("Count for word \"snow\" is "+ countForEachWord.get("snow"));

        String highestFreeqWord = countForEachWord.get("cold") > countForEachWord.get("flu") ? (countForEachWord.get("cold") > countForEachWord.get("snow") ? "cold" :"snow") : (countForEachWord.get("flu") > countForEachWord.get("snow") ? "flu" :"snow");
        String lowestFreeqWord = countForEachWord.get("cold") < countForEachWord.get("flu") ? (countForEachWord.get("cold") < countForEachWord.get("snow") ? "cold" :"snow") : (countForEachWord.get("flu") < countForEachWord.get("snow") ? "flu" :"snow");

        System.out.println("The word with highest freequency "+ " is " + highestFreeqWord);
        System.out.println("The word with lowest freequency "+ " is " + lowestFreeqWord);

    }


    private static void applyMapReduce(String filename)
    {
        sparkConf = new SparkConf().setMaster("local").setAppName("MapReduce");

        sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> inputFile = sparkContext.textFile(filename);

        JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split("[\\s\"]")).iterator());

        JavaPairRDD countData = wordsFromFile.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);

        countData.saveAsTextFile("CountData");

        int count = findOccuranceInMaster(COLD_REGEX);
        int current_value = countForEachWord.get("cold");
        countForEachWord.put("cold",count+current_value);

        count = findOccuranceInMaster(FLU_REGEX);
        current_value = countForEachWord.get("flu");
        countForEachWord.put("flu",count+current_value);

        count = findOccuranceInMaster(SNOW_REGEX);
        current_value = countForEachWord.get("snow");
        countForEachWord.put("snow",count+current_value);
    }

    private static int findOccuranceInMaster(String regex)
    {
        int count = 0;
        try {

            String content = FileHandler.readFileData("CountData/part-00000");
            ArrayList<String> keyValues = findKeyValue(content, regex);
            for (String keyValue : keyValues)
            {
                count += findValue(keyValue);
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
        return count;
    }

    private static ArrayList findKeyValue(String content, String regex)
    {
        ArrayList<String> list = new ArrayList<>();
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(content);
        while (matcher.find())
        {
            list.add(matcher.group(0)) ;
        }

        return list;
    }

    private static int findValue(String keyValue)
    {
        String[] array = keyValue.split(",");
        if(array.length == 2)
        {
            return Integer.valueOf(array[1].substring(0,array[1].length()-1)).intValue();
        }

        return -1;
    }






}