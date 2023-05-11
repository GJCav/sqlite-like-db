package db;

import db.exception.DBRuntimeError;

import java.util.ArrayList;
import java.util.List;

public abstract class Cell {
    protected byte type = 0;
    public int cell_id = -1;

    public Cell(int cell_id, byte[] data) {
        this.cell_id = cell_id;
        if (data.length < 1) {
            throw new DBRuntimeError("data too short to be an abstract cell");
        }
        this.type = data[0];
    }

    public byte get_type() { return type; }
}
