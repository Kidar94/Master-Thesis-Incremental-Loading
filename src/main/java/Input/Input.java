package Input;

import java.io.File;

public class Input {
    private String input;

    public Input(String input)  {
        this.input=input;
    }

    public void listFiles() {
        File directory = new File(input);
        File[] contents = directory.listFiles();
        for (File f : contents) {
            System.out.println(f.getAbsolutePath());
        }
    }
}
