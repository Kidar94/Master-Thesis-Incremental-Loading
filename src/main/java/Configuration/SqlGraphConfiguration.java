package Configuration;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class SqlGraphConfiguration {

    private static final String PATH_TO_CONFIG_FILE = "src/main/resources/config.properties";
    private static String datasetDirectory;
    private static String datasetFiles;
    private static String vertexesOutput;

    public SqlGraphConfiguration()  {
        Properties configProperties= new Properties();
        FileInputStream in=null;
        try {
            in=new FileInputStream(PATH_TO_CONFIG_FILE);
            configProperties.load(in);
            datasetDirectory=String.valueOf(configProperties.getProperty("Dataset"));
            datasetFiles=String.valueOf(configProperties.getProperty("DatasetFiles"));
            vertexesOutput=String.valueOf(configProperties.getProperty("VertexesOutput"));

        }   catch (IOException e)   {
            e.printStackTrace();
        }
    }

    public static String getDatasetDirectory()  {
        return datasetDirectory;
    }

    public static String getDatasetFiles()  { return datasetFiles;}

    public static String getVertexesOutput()    { return vertexesOutput;}
}
