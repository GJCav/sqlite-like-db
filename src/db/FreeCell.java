package db;

import db.exception.DBRuntimeError;

public class FreeCell extends Cell {
    public FreeCell(int cell_id, byte[] data) {
        super(cell_id, data);
        if (get_type() != CellType.FREE) {
            throw new DBRuntimeError("Invalid cell type, expect FREE, read "
                    + CellType.to_string(get_type()));
        }
        if (data.length < 5) {
            throw new DBRuntimeError("data too short to be a free cell");
        }
    }

    public int get_next() {
        return Bytes.to_int(data, 1);
    }

    public void set_next(int next) {
        byte[] val = Bytes.from_int(next);
        System.arraycopy(val, 0, data, 1, 4);
    }

    public static FreeCell create(int next, int cell_size) {
        if (cell_size < 5) {
            throw new IllegalArgumentException("cell_size must be at least 5");
        }

        byte[] data = new byte[cell_size];
        data[0] = CellType.FREE;
        return new FreeCell(-1, data);
    }
}
