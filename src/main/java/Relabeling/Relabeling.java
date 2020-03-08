package Relabeling;

import java.io.*;
import java.util.ArrayList;

public class Relabeling {
    public Relabeling() {}

    public String attributingNewIds(ArrayList<String> filesToMerge, String vertexesOutput)  throws IOException  {
        BufferedWriter bw=null;
        BufferedReader br= null;
        String mergeVertexes=vertexesOutput+"/vertexes.csv";
        String output="";

        try {
            bw= new BufferedWriter(new FileWriter(mergeVertexes));
            int i=0;
            for (String path: filesToMerge) {
                try {
                    br= new BufferedReader(new FileReader(path));
                    String line="";
                    while((line=br.readLine())!=null)   {
                        i++;
                        bw.write(i+"|"+line);
                        bw.newLine();
                    }
                }   catch (IOException e)   {
                    e.printStackTrace();
                } finally {
                    if (br!= null)  {
                        br.close();
                    }
                }
            }
        }   catch(IOException e)    {
            e.printStackTrace();
        }   finally {
            if(bw!=null)    {
                bw.close();
            }
        }
        return mergeVertexes;
    }
}
