package com.lxf.salestrain;

import org.apache.spark.ml.linalg.VectorUDT;

public class TrainBean {
    private String id;
    private VectorUDT v;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public VectorUDT getV() {
        return v;
    }

    public void setV(VectorUDT v) {
        this.v = v;
    }
}
