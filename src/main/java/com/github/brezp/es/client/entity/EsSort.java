package com.github.brezp.es.client.entity;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.search.sort.SortOrder;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by turner on 2018/6/28.
 */

public class EsSort {

    private List<Tuple<String, SortOrder>> sortNameOrders = new LinkedList<>();

    private static SortOrder defaultSortOrder = SortOrder.ASC;

    public EsSort() {
    }

    public EsSort addSort(String sortName, String sortOrder) {
        SortOrder order = (sortOrder == null ? SortOrder.ASC : sortOrder.equals("desc") ? SortOrder.DESC : SortOrder.ASC);
        sortNameOrders.add(new Tuple<>(sortName, order));
        return this;
    }

    public EsSort(String sortName, String sortOrder) {
        addSort(sortName, sortOrder);
    }

    public EsSort(List<Tuple<String, SortOrder>> sortNameOrders) {
        this.sortNameOrders = sortNameOrders;
    }

    /**
     * 使用例子 fromFields(field1, field2, field3)，默认排序order为desc
     *
     * @param fields
     * @return
     */
    public static EsSort fromFields(SortOrder order, String... fields) {
        List<Tuple<String, SortOrder>> sortNameOrders = new LinkedList<>();
        for (String field : fields) {
            sortNameOrders.add(new Tuple<>(field, order == null ? defaultSortOrder : order));
        }
        return new EsSort(sortNameOrders);
    }

    public List<Tuple<String, SortOrder>> sortPairs() {
        return sortNameOrders;
    }
}
