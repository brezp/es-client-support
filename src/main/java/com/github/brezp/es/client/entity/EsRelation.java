package com.github.brezp.es.client.entity;

/**
 * copy from ES7.9 org.apache.lucene.search.TotalHits.Relation
 */
public enum EsRelation {
    EQUAL_TO,

    GREATER_THAN_OR_EQUAL_TO;

    public static EsRelation parseRelation(String relation) {
        if ("gte".equals(relation)) {
            return EsRelation.GREATER_THAN_OR_EQUAL_TO;
        } else if ("eq".equals(relation)) {
            return EsRelation.EQUAL_TO;
        } else {
            throw new IllegalArgumentException("invalid total hits relation: " + relation);
        }
    }
}
