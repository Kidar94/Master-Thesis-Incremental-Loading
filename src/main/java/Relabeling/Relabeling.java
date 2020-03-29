package Relabeling;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class Relabeling {
    public Relabeling() {}


    public ArrayList<String> attributingNewIds3(ArrayList<String> filesToMerge, String vertexesOutput, HashMap<String, ArrayList> relationshipPerVertexType)  throws IOException {
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

        for (String path : filesToMerge) {
            ArrayList<String> newLabels= new ArrayList<>();
            File fileInput= new File(path);
            String fileInputName=fileInput.getName();
            File tempFileOutput = File.createTempFile(fileInputName, "relabeled.csv", new File("D:/MT/Import/Test/tmp"));
            tempFileOutput.deleteOnExit();
            output=tempFileOutput.getPath();
            int indxName=fileInputName.indexOf("_");
            String vertexesType=fileInputName.substring(0,indxName);

            try {
                bw = new BufferedWriter(new FileWriter(output));
                br = new BufferedReader(new FileReader(path));
                String line = "";
                while ((line = br.readLine()) != null) {
                    i++;
                    bw.write(i + "|" + line);
                    bw.newLine();
                    int indx2= line.indexOf("\"id\":\"");
                    line=line.substring(indx2+6);
                    indx2=line.indexOf("\"");
                    id=line.substring(0,indx2);
                    newLabels.add(id);
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
                    relationshipRelabeling.relabeling(relationshipPath, newLabels);

                }

                outputFiles.add(output);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (bw != null) {
                    bw.close();
                }
                if (br != null) {
                    br.close();
                }
            }
        }
        return outputFiles;
    }

    public ArrayList<String> attributingNewIds(ArrayList<String> filesToMerge, String vertexesOutput, HashMap<String, ArrayList> relationshipPerVertexType)  throws IOException {
        BufferedWriter bw = null;
        BufferedReader br = null;
        BufferedReader brRelationships=null;
        BufferedWriter bwRelationships=null;
        String mergeVertexes = vertexesOutput + "/vertexes.csv";
        String output = "";
        ArrayList<String> outputFiles= new ArrayList<>();
        ArrayList<Long> newLabels= new ArrayList<>();
        String id="";
        Long idInt=0L;
        Long i = 0L;
        for (String path : filesToMerge) {
            File fileInput= new File(path);
            String fileInputName=fileInput.getName();
            File tempFileOutput = File.createTempFile(fileInputName, "relabeled.csv", new File("D:/MT/Import/Test/tmp"));
            tempFileOutput.deleteOnExit();
            output=tempFileOutput.getPath();
            int indxName=fileInputName.indexOf("_");
            String vertexesType=fileInputName.substring(0,indxName);

            newLabels.add(i);
            try {
                bw = new BufferedWriter(new FileWriter(output));
                br = new BufferedReader(new FileReader(path));
                String line = "";
                while ((line = br.readLine()) != null) {
                    i++;
                    bw.write(i + "|" + line);
                    bw.newLine();
                    int indx2= line.indexOf("\"id\":\"");
                    line=line.substring(indx2+6);
                    indx2=line.indexOf("\"");
                    id=line.substring(0,indx2);
                    idInt=Long.valueOf(id);
                    newLabels.add(idInt);
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
                    brRelationships= new BufferedReader(new FileReader(relationshipPath));
                    bwRelationships= new BufferedWriter(new FileWriter(relationshipPath+"relabeled2.csv"));
                    String line2="";
                    while((line2=brRelationships.readLine())!=null)  {
                        Long lineValue=Long.valueOf(line2);
                        int newLabel=newLabels.indexOf(lineValue);
                        bwRelationships.write(newLabel);
                        bwRelationships.newLine();
                    }

                }

                outputFiles.add(output);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (bw != null) {
                    bw.close();
                }
                if (br != null) {
                    br.close();
                }
                if(brRelationships!=null)   {
                    brRelationships.close();
                }
                if(bwRelationships!=null)   {
                    bwRelationships.close();
                }
            }
        }
        return outputFiles;
    }
}
