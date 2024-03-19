package com.databend.ktobend;

public class MergeIntoEffectRows {
    private Integer insertRows;
    private Integer updateRows;

    public MergeIntoEffectRows(Integer insertRows, Integer updateRows) {
        this.insertRows = insertRows;
        this.updateRows = updateRows;
    }

    public Integer getInsertRows() {
        return insertRows;
    }

    public Integer getUpdateRows() {
        return updateRows;
    }

    public void setInsertRows(Integer insertRows) {
        this.insertRows = insertRows;
    }

    public void setUpdateRows(Integer updateRows) {
        this.updateRows = updateRows;
    }
}
