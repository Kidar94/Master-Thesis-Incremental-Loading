package Parsing;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import Configuration.SqlGraphConfiguration;
import org.json.JSONArray;
import org.json.JSONObject;


public class Parsing {
    private String output;

    private JSONObject attributeJson=null;
    private JSONArray atttributeJson2= null;
    private SqlGraphConfiguration sqlGraphConfiguration= new SqlGraphConfiguration();
    private String tmpDirectory=sqlGraphConfiguration.getTemporaryFilesDirectory();



    public Parsing()   {
    }

    public String VAParsing(String input) throws IOException {
        BufferedReader br=null;
        BufferedWriter bw= null;
        String jsonAttributes="";

        try {
            File fileInput= new File(input);
            String fileInputName=fileInput.getName();
            File tempFileOutput = File.createTempFile(fileInputName, "parsed.csv", new File(tmpDirectory));
            tempFileOutput.deleteOnExit();
            output=tempFileOutput.getPath();
            br=new BufferedReader(new FileReader(input));
            bw= new BufferedWriter(new FileWriter(output));
            String jsonKeys=br.readLine();
            ArrayList<String> jsonKeysArray= new ArrayList<>();
            String jsonKeys2=jsonKeys;
            while((jsonKeys2.indexOf("|"))>=0)  {
                int keyIndx= jsonKeys2.indexOf("|");
                String key= jsonKeys2.substring(0,keyIndx);
                jsonKeysArray.add(key);
                jsonKeys2=jsonKeys2.substring(keyIndx+1);
            }
            jsonKeysArray.add(jsonKeys2);
            jsonKeysArray.add("Type");
            String line;
            while((line=br.readLine())!=null)  {
                int j=0;
                jsonAttributes="{";
                while((line.indexOf("|"))>=0)  {
                    int indx=line.indexOf("|");
                    String attribute=line.substring(0,indx);
                    line=line.substring(indx+1);
                    jsonAttributes=jsonAttributes+"\""+jsonKeysArray.get(j)+"\""+":"+"\""+attribute+"\""+",";
                    j++;
                }
                Path path= Paths.get(input);
                String type= path.getFileName().toString();
                type=type.substring(0,type.indexOf("_"));
                jsonAttributes=jsonAttributes+"\""+jsonKeysArray.get(j)+"\""+":"+"\""+line+"\""+","+"\"Type\":"+"\""+type+"\""+"}";
                bw.write(jsonAttributes);
                bw.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }   finally {
            if (br!=null)   {
                br.close();
            }
            if (bw!=null)   {
                bw.close();
            }
        }
        return output;
    }

    public String addingAttributes(String parsedVertexesFile, String sortedAttributeFile) throws IOException {
        BufferedReader brVertex= null;
        BufferedReader brAttribute= null;
        BufferedWriter outpuut= null;
        ArrayList<String> attributesList= new ArrayList<>();
        String output2="";
        try {
            brVertex=new BufferedReader(new FileReader(parsedVertexesFile));
            brAttribute= new BufferedReader(new FileReader(sortedAttributeFile));

            File fileInput= new File(sortedAttributeFile);
            String fileInputName=fileInput.getName();
            File tempFileOutput = File.createTempFile(fileInputName, "added.csv", new File(tmpDirectory));
            tempFileOutput.deleteOnExit();
            output2=tempFileOutput.getPath();
            outpuut= new BufferedWriter(new FileWriter(output2));
            String line=brAttribute.readLine();
            int indx=line.indexOf("|");
            String key1=line.substring(indx+1);
            String identifier1="";
            String identifier2="";
            String line2="";

            while((line=brAttribute.readLine())!=null)  {
                indx=line.indexOf("|");
                identifier1=line.substring(0,indx);
                String value1=line.substring(indx+1);
                boolean bool= identifier1.equals(identifier2);
                while(!bool)    {
                    if(line2!="")   {
                        if(attributesList.size()==1)    {
                            attributeJson.put(key1, attributesList.get(0));
                            outpuut.write(attributeJson.toString());
                            outpuut.newLine();
                        }   else if(attributesList.size()==0)    {
                            outpuut.write(attributeJson.toString());
                            outpuut.newLine();
                        }   else {
                            attributeJson.put(key1, attributesList);
                            outpuut.write(attributeJson.toString());
                            outpuut.newLine();
                        }
                        attributesList= new ArrayList<String>();
                    }
                    line2=brVertex.readLine();
                    if(line2!=null) {
                        attributeJson = new JSONObject(line2);
                        identifier2=attributeJson.getString("id");
                        bool= identifier1.equals(identifier2);
                    }
                }
                if(bool)    {
                    attributesList.add(value1);
                }
            }
            if(line2!="")   {
                attributeJson.put(key1, attributesList);
                outpuut.write(attributeJson.toString());
                outpuut.newLine();
            }

        }   catch(IOException e)    {
            e.printStackTrace();
        }   finally {
            if(brVertex!=null)  {
                brVertex.close();
            }
            if(brAttribute!=null)   {
                brAttribute.close();
            }
            if(outpuut!=null)   {
                outpuut.close();
            }
        }
        return output2;
    }
}
