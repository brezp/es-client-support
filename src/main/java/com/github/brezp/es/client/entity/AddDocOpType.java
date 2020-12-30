package com.github.brezp.es.client.entity;

import java.util.Locale;

/**
 * Add Doc 操作的类型
 */
public enum AddDocOpType {
    /**
     * 覆盖写：
     * Index the source. If there an existing document with the id, it will
     * be replaced.
     */
    INDEX(0),

    /**
     * 尝试写入，若docID已存在则放弃写入：
     * Creates the resource. Simply adds it to the index, if there is an existing
     * document with the id, then it won't be removed.
     */
    CREATE(1);

    private final byte op;
    private final String lowercase;

    AddDocOpType(int op) {
        this.op = (byte) op;
        this.lowercase = this.toString().toLowerCase(Locale.ROOT);
    }

    public byte getId() {
        return op;
    }

    public String toString() {
        return lowercase;
    }

    public static AddDocOpType fromId(byte id) {
        switch (id) {
            case 0:
                return INDEX;
            case 1:
                return CREATE;
            default:
                throw new IllegalArgumentException("Unknown opType: [" + id + "]");
        }
    }

    public static AddDocOpType fromString(String sOpType) {
        String lowerCase = sOpType.toLowerCase(Locale.ROOT);
        for (AddDocOpType opType : AddDocOpType.values()) {
            if (opType.toString().equals(lowerCase)) {
                return opType;
            }
        }
        throw new IllegalArgumentException("Unknown opType: [" + sOpType + "]");
    }
}
