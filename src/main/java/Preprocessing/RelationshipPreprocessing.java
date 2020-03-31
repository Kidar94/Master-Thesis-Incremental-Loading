package Preprocessing;

import java.io.*;
import java.util.ArrayList;

public class RelationshipPreprocessing {

    public RelationshipPreprocessing() {

    }

    public ArrayList<String> relationshipDivide(String fileName) throws IOException {
        ArrayList<String> output= new ArrayList<>();
        BufferedReader br=null;
        BufferedWriter bw1=null;
        BufferedWriter bw2=null;
        String output1="";
        String output2="";
        try {
            File fileInput= new File(fileName);
            String fileInputName1=fileInput.getName();
            File tempFileOutput1 = File.createTempFile(fileInputName1, "divided_source.csv", new File("D:/MT/Import/Test/tmp"));
            tempFileOutput1.deleteOnExit();
            output1=tempFileOutput1.getPath();

            File fileInput2= new File(fileName);
            String fileInputName2=fileInput.getName();
            File tempFileOutput2 = File.createTempFile(fileInputName2, "divided_target.csv", new File("D:/MT/Import/Test/tmp"));
            tempFileOutput2.deleteOnExit();
            output2=tempFileOutput2.getPath();

            br= new BufferedReader(new FileReader(fileName));
            bw1= new BufferedWriter(new FileWriter(output1));
            bw2= new BufferedWriter(new FileWriter(output2));

            String line="";
            while((line=br.readLine())!=null)   {
                int indx=line.indexOf("|");
                String source=line.substring(0,indx);
                bw1.write(source);
                bw1.newLine();
                line=line.substring(indx+1);
                String target=line;
                bw2.write(target);
                bw2.newLine();
            }
        }   catch (IOException e)   {
            e.printStackTrace();
        }   finally {
            if(br!=null)    {
                br.close();
            }
            if(bw1!=null)   {
                bw1.close();
            }
            if(bw2!=null)   {
                bw2.close();
            }
        }
        output.add(output1);
        output.add(output2);
        return output;
    }

}
