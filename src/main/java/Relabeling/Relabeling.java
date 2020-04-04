package Relabeling;

import java.io.*;
import java.nio.Buffer;
import java.util.ArrayList;
import java.util.HashMap;

public class Relabeling {
    public Relabeling() {}

    public ArrayList<String> attributingNewIds(ArrayList<String> filesToMerge, String vertexesOutput, HashMap<String, ArrayList> relationshipPerVertexType, ArrayList<String> relationshipNamesList)  throws IOException {
        BufferedWriter bw = null;
        BufferedReader br = null;
        BufferedReader brRelationships=null;
        BufferedWriter bwRelationships=null;
        String mergeVertexes = vertexesOutput + "/vertexes.csv";
        String output = "";
        ArrayList<String> outputFiles= new ArrayList<>();
        String id="";
        Long i = 0L;
        RelationshipRelabeling relationshipRelabeling= new RelationshipRelabeling();
        ArrayList<String> dividedRelationshipList= new ArrayList<>();
        ArrayList<String> newLabels= null;

        try {
            bw = new BufferedWriter(new FileWriter("D:/MT/Import/Test/outputVertexes.csv"));
            for (String path : filesToMerge) {
                newLabels= new ArrayList<>();
                File fileInput= new File(path);
                String fileInputName=fileInput.getName();
                //File tempFileOutput = File.createTempFile(fileInputName, "relabeled.csv", new File("D:/MT/Import/Test/tmp"));
                //tempFileOutput.deleteOnExit();
                //output=tempFileOutput.getPath();
                int indxName=fileInputName.indexOf("_");
                String vertexesType=fileInputName.substring(0,indxName);
                Long firstId=i;
                String newFile="";

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
                    outputFiles.add(output);
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
            bwEdges= new BufferedWriter(new FileWriter("D:/MT/Import/Test/outputEdges.csv"));
            

            for(ArrayList<String> sourceTarget:newNames) {
                try {
                    if(sourceTarget.get(0).contains("source"))  {
                        brSource=new BufferedReader(new FileReader(sourceTarget.get(0)));
                        brTarget=new BufferedReader(new FileReader(sourceTarget.get(1)));
                    }   else    {
                        brSource=new BufferedReader(new FileReader(sourceTarget.get(1)));
                        brTarget=new BufferedReader(new FileReader(sourceTarget.get(0)));
                    }
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
        return outputFiles;
    }
}
