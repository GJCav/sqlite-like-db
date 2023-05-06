import java.io.File;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {
    public static void main(String[] args) {
        try {
            DatabaseFile db = new DatabaseFile(new File("test.db"));
            db.print_headers();
        } catch (DBException e) {
            e.printStackTrace();
        }

    }
}