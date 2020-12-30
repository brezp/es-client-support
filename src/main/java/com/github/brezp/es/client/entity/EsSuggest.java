package com.github.brezp.es.client.entity;

import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;

/**
 * Created by jiahong on 2018/07/09.
 */
public class EsSuggest {
    private CompletionSuggestionBuilder builder;
    private String sugField;
    private String aggField;

    public EsSuggest(String field, String prefix, int size) {
        this.aggField = "completion#"+field;
        this.sugField = field + "_suggest" ;
        builder = new CompletionSuggestionBuilder(
                this.sugField);
        builder.size(size).text(prefix);
    }

    public CompletionSuggestionBuilder getSuggestion() {
        return builder;
    }

    public String getAggField() {
        return aggField;
    }

    public String getSugField() {
        return sugField;
    }
}