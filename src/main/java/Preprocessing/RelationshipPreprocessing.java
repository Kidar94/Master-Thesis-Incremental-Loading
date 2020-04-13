package Preprocessing;

import Configuration.SqlGraphConfiguration;

import java.io.*;
import java.util.ArrayList;

public class RelationshipPreprocessing {
    private SqlGraphConfiguration sqlGraphConfiguration= new SqlGraphConfiguration();
    private String tmpDirectory=sqlGraphConfiguration.getTemporaryFilesDirectory();

    public RelationshipPreprocessing() {

    }

    public ArrayList<String> relationshipDivide(String fileName) throws IOException {
        ArrayList<String> output= new ArrayList<>();
        BufferedReader br=null;
        BufferedWriter bw1=null;
        BufferedWriter bw2=null;
        String output1="";
        String output2="";
        String attributeJson="";
        try {
            File fileInput= new File(fileName);
            String fileInputName1=fileInput.getName();
            File tempFileOutput1 = File.createTempFile(fileInputName1, "divided_source.csv", new File(tmpDirectory));
            tempFileOutput1.deleteOnExit();
            output1=tempFileOutput1.getPath();

            File fileInput2= new File(fileName);
            String fileInputName2=fileInput.getName();
            File tempFileOutput2 = File.createTempFile(fileInputName2, "divided_target.csv", new File(tmpDirectory));
            tempFileOutput2.deleteOnExit();
            output2=tempFileOutput2.getPath();
            boolean bool=false;
            String target="";
            String attributeValues="";
            String attributeValue="";
            boolean bool2=false;


            br= new BufferedReader(new FileReader(fileName));
            bw1= new BufferedWriter(new FileWriter(output1));
            bw2= new BufferedWriter(new FileWriter(output2));
            ArrayList<String> attributes=new ArrayList<>();

            String line=br.readLine();
            int indx=line.indexOf("|");
            String source=line.substring(0,indx);
            bw1.write(source);
            bw1.newLine();
            line=line.substring(indx+1);
            String attribute=line;
            bw2.write(attribute);
            bw2.newLine();
            if (attribute.contains("|"))   {
                indx=attribute.indexOf("|");
                attribute=attribute.substring(indx+1);
                bool=true;
            }
            while(attribute.contains("|"))  {
                indx=attribute.indexOf("|");
                attributes.add(attribute.substring(0,indx));
                attribute=attribute.substring(indx+1);
            }
            if(bool)    {
                attributes.add(attribute);
            }   else    {
                attributeJson=null;
            }
            int j=0;
            int indx2;
            while((line=br.readLine())!=null)   {
                indx=line.indexOf("|");
                source=line.substring(0,indx);
                bw1.write(source);
                bw1.newLine();
                line=line.substring(indx+1);
                target=line;
                if(j<attributes.size()) {
                    attributeJson="{";
                    indx2=target.indexOf("|");
                    attributeValues=target.substring(indx2+1);
                    target=target.substring(0,indx2);
                    bool2=true;
                }
                indx2=attributeValues.indexOf("|");
                while(indx2>0)  {
                    attributeValue=attributeValues.substring(0,indx2);
                    attributeJson=attributeJson+"\""+attributes.get(j)+"\""+":"+"\""+attributeValue+"\""+",";
                    j++;
                    indx2=attributeValues.indexOf("|");
                }
                if(bool2)   {
                    attributeJson=attributeJson+"\""+attributes.get(j)+"\""+":"+"\""+attributeValues+"\""+"}";
                }
                bw2.write(target+"|"+attributeJson);
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
