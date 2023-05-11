package db;

import db.exception.DBRuntimeError;

public class FreeCell extends Cell {
    public int next;

    public FreeCell(int cell_id, byte[] data) {
        super(cell_id, data);
        if (get_type() != CellType.FREE) {
            throw new DBRuntimeError("Invalid cell type, expect FREE, read "
                    + CellType.to_string(get_type()));
        }
        if (data.length < 5) {
            throw new DBRuntimeError("data too short to be a free cell");
        }
        this.next = Bytes.to_int(data, 1);
    }
}
