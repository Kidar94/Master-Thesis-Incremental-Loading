package Preprocessing;
import java.util.Comparator;

public class CsvSortingComparator implements Comparator<String> {

    @Override
    public int compare(String input1, String input2) {
        int indx1=input1.indexOf("|");
        int indx2=input2.indexOf("|");
        String idstr1=input1.substring(0,indx1);
        String idstr2=input2.substring(0,indx2);
        Long id1=0L;
        Long id2=0L;
        try {
            id1= Long.valueOf(idstr1);
        }   catch (NumberFormatException e) {
            id1=-1L;
        }
        try {
            id2=Long.valueOf(idstr2);
        }   catch(NumberFormatException e)  {
            id2=id1-1;
        }
        return Long.compare(id1,id2);
    }
}
