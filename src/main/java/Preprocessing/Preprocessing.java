package Preprocessing;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import Configuration.SqlGraphConfiguration;
import com.google.code.externalsorting.ExternalSort;


public class Preprocessing {

    private String input;
    private SqlGraphConfiguration sqlGraphConfiguration= new SqlGraphConfiguration();
    private String tmpDirectory=sqlGraphConfiguration.getTemporaryFilesDirectory();

    public Preprocessing(String input)  { this.input=input; }

    public String externalSorting()   {
        String output="";
        try {
            File fileInput= new File(input);
            String fileInputName=fileInput.getName();
            fileInputName=fileInputName.substring(0,fileInputName.indexOf("0"));
            File tempFileOutput = File.createTempFile(fileInputName, "sorted.csv", new File(tmpDirectory));
            tempFileOutput.deleteOnExit();
            output=tempFileOutput.getPath();
            CsvSortingComparator comparator = new CsvSortingComparator();
            List<File> sortInBatch= ExternalSort.sortInBatch(fileInput, comparator);
            ExternalSort.mergeSortedFiles(sortInBatch, tempFileOutput, comparator);
        }   catch (IOException e)   {
            e.printStackTrace();
        }
        return output;
    }
}