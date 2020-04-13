package Relabeling;

import Configuration.SqlGraphConfiguration;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class Relabeling {
    private SqlGraphConfiguration sqlGraphConfiguration= new SqlGraphConfiguration();
    private String tmpDirectory=sqlGraphConfiguration.getTemporaryFilesDirectory();
    private String outputDirectory=sqlGraphConfiguration.getVertexesOutput();

    public Relabeling() {}

    public ArrayList<ArrayList> attributingNewIds(ArrayList<String> filesToMerge, String vertexesOutput, HashMap<String, ArrayList> relationshipPerVertexType, ArrayList<String> relationshipNamesList)  throws IOException {
        BufferedWriter bw = null;
        BufferedReader br = null;
        BufferedReader brRelationships=null;
        BufferedWriter bwRelationships=null;
        String mergeVertexes = vertexesOutput + "/vertexes.csv";
        ArrayList<String> outputFiles= new ArrayList<>();
        String id="";
        Long i = 0L;
        RelationshipRelabeling relationshipRelabeling= new RelationshipRelabeling();
        ArrayList<String> dividedRelationshipList= new ArrayList<>();
        ArrayList<String> newLabels= null;
        ArrayList<String> vertexesTypeIds= new ArrayList<>();
        ArrayList<String>   relationshipList= new ArrayList<>();

        ArrayList<ArrayList> output= new ArrayList<>();

        try {
            String vertexesoutputFileName=outputDirectory+"/vertexes.csv";
            bw = new BufferedWriter(new FileWriter(vertexesoutputFileName));
            for (String path : filesToMerge) {
                newLabels= new ArrayList<>();
                File fileInput= new File(path);
                String fileInputName=fileInput.getName();
                int indxName=fileInputName.indexOf("_");
                String vertexesType=fileInputName.substring(0,indxName);
                Long firstId=i;
                String newFile="";
                String firstlastId=i.toString();

                try {
                    br = new BufferedReader(new FileReader(path));
                    String line = "";
                    while ((line = br.readLine()) != null) {
                        bw.write(i + "|" + line);
                        bw.newLine();
                        int indx2= line.indexOf("\"id\":\"");
                        line=line.substring(indx2+6);
                        indx2=line.indexOf("\"");
                        id=line.substring(0,indx2);
                        newLabels.add(id);
                        i++;
                    }
                    Long i2=i-1;
                    firstlastId=firstlastId+"-"+i2.toString();
                    vertexesTypeIds.add(vertexesType+"|"+firstlastId);
                    String typeRelationship="";
                    ArrayList<String> relationshipPaths= new ArrayList<>();

                    for (String type:relationshipPerVertexType.keySet())  {
                        if(type.equals(vertexesType))   {
                            typeRelationship=type;
                        }
                    }
                    relationshipPaths=relationshipPerVertexType.get(typeRelationship);

                    for(String relationshipPath:relationshipPaths)    {
                        newFile=relationshipRelabeling.relabeling(relationshipPath, newLabels, firstId);
                        dividedRelationshipList.add(newFile);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    if (br != null) {
                        br.close();
                    }
                }
            }

        }   catch  (IOException e)  {
            e.printStackTrace();
        }   finally {
            if(bw!=null)    {
                bw.close();
            }
        }

        ArrayList<ArrayList> newNames= new ArrayList<>();
        String newName="";
        ArrayList<String> sourceTargetNames=null;
        BufferedWriter bwEdges=null;
        BufferedWriter  bwEdgesSubFile=null;
        BufferedReader brSource=null;
        BufferedReader brTarget=null;
        String lineSource;
        String lineTarget;
        String edgeAttributes;
        String newLine;
        Integer edgesId=0;

        for(String name:relationshipNamesList)  {
            sourceTargetNames= new ArrayList<>();
            for(int j=0; j<dividedRelationshipList.size(); j++)  {

                newName=dividedRelationshipList.get(j);

                if(newName.contains(name))  {
                    sourceTargetNames.add(newName);
                }
            }
            newNames.add(sourceTargetNames);
        }


        try {
            String edgesoutputFileName=outputDirectory+"/edges.csv";
            bwEdges= new BufferedWriter(new FileWriter(edgesoutputFileName));

            for(ArrayList<String> sourceTarget:newNames) {
                try {
                    if(sourceTarget.get(0).contains("source"))  {
                        brSource=new BufferedReader(new FileReader(sourceTarget.get(0)));
                        brTarget=new BufferedReader(new FileReader(sourceTarget.get(1)));
                    }   else    {
                        brSource=new BufferedReader(new FileReader(sourceTarget.get(1)));
                        brTarget=new BufferedReader(new FileReader(sourceTarget.get(0)));
                    }
                    File file= new  File(sourceTarget.get(0));
                    String fileName2=file.getName();

                    int indexName=fileName2.indexOf("_");
                    int indexName2=fileName2.indexOf("_", indexName+1);
                    int indexName3=fileName2.indexOf("_", indexName2+1);
                    String relationshipName=fileName2.substring(0,indexName3);
                    File tempFileOutput=file.createTempFile(relationshipName, "relabeled.csv", new File(tmpDirectory));
                    tempFileOutput.deleteOnExit();
                    String path=tempFileOutput.getPath();
                    bwEdgesSubFile= new BufferedWriter(new FileWriter(path));
                    relationshipList.add(path);
                    String filePath=sourceTarget.get(0);
                    File fileInput=new File(filePath);
                    String fileName=fileInput.getName();
                    int index1=fileName.indexOf("_");
                    fileName=fileName.substring(index1+1);
                    index1=fileName.indexOf("_");
                    fileName=fileName.substring(0,index1);
                    lineSource=brSource.readLine();
                    lineTarget=brTarget.readLine();

                    while((lineSource=brSource.readLine())!=null) {
                        edgeAttributes="";
                        lineTarget=brTarget.readLine();
                        index1=lineTarget.indexOf("|");
                        if(index1>0)    {
                            edgeAttributes=lineTarget.substring(index1+1);

                            lineTarget=lineTarget.substring(0,index1);
                            lineTarget=lineTarget+"|"+fileName+"|"+edgeAttributes;
                        }   else    {
                            lineTarget=lineTarget+"|"+fileName+"|";
                        }

                        newLine=edgesId.toString()+"|"+lineSource+"|"+lineTarget;
                        bwEdges.write(newLine);
                        bwEdges.newLine();
                        bwEdgesSubFile.write(newLine);
                        bwEdgesSubFile.newLine();
                        edgesId++;
                    }

                }   catch (IOException e)   {
                    e.printStackTrace();
                }   finally {
                    if(brSource!=null)  {
                        brSource.close();
                    }
                    if(brTarget!=null)  {
                        brTarget.close();
                    }
                    if(bwEdgesSubFile!=null)    {
                        bwEdgesSubFile.close();
                    }
                }

            }
        }   catch(IOException e)    {
            e.printStackTrace();
        }   finally {
            if(bwEdges!=null)   {
                bwEdges.close();
            }
        }
        int debug=0;
        output.add(vertexesTypeIds);
        output.add(relationshipList);
        return output;
    }
}
