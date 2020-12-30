package com.github.brezp.es.client.entity;

/**
 * 支持的ES版本
 */
public enum EsVersion {

    V5_6("v5.6"),
    V1_7("v1.7"),
    V2_3("v2.3"),
    V6_8("v6.8"),
    V7_9("v7.9"),
    DEFAULT("v5.6");

    private final String op;

    EsVersion(String op) {
        this.op = op;
    }

    public String getOp() {
        return op;
    }

    public static EsVersion VALUE(String val) {
        for (EsVersion esVersion : EsVersion.values()) {
            if (esVersion.op.equals(val)) {
                return esVersion;
            }
        }
        return null;
    }

}
