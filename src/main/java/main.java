import java.io.IOException;
import SqlGraphLoading.SqlGraphLoading;

public class main {

    public static void main(String args[]) throws IOException {
        int debug=0;
        SqlGraphLoading sqlGraphLoading = new SqlGraphLoading();
        sqlGraphLoading.importVertices();
        debug=1;

    }
}
