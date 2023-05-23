package jcav.test;
import jcav.filelayer.*;
import jcav.filelayer.DBFile;

import java.io.File;
import java.nio.file.Files;

public class CreateDB {
    public static void main(String[] argv) {
        try {
            if(Files.exists(new File("test.db").toPath())){
                Files.delete(new File("test.db").toPath());
            }
            DBFile db = DBFile.create("test.db");
            db.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
