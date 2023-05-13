import java.io.*;

public class Main {
    public static void main(String[] argv) {
        try {
            RandomAccessFile ram = new RandomAccessFile("1.dat", "rwd");
            ram.seek(0);
            ram.write(new byte[128]);
            ram.seek(0);
            int sz = ram.read(new byte[128]);

            ram.seek(512);
            ram.write(new byte[128]);
            ram.seek(512);
            sz = ram.read(new byte[128]);
            System.out.println(sz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
