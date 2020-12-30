package com.github.brezp.es.client.util;

import io.searchbox.action.AbstractMultiIndexActionBuilder;
import io.searchbox.action.GenericResultAbstractAction;


public class GetAliasesSpecificName extends GenericResultAbstractAction {

    private String alias;

    protected GetAliasesSpecificName(GetAliasesSpecificName.Builder builder, String alias) {
        super(builder);
        this.alias = alias;
        setURI(buildURI());
    }

    @Override
    public String getRestMethodName() {
        return "GET";
    }

    @Override
    protected String buildURI() {
        return super.buildURI() + "/_alias/" + alias;
    }

    public static class Builder extends AbstractMultiIndexActionBuilder<GetAliasesSpecificName, Builder> {
        protected String alias = "*";

        public GetAliasesSpecificName.Builder alias(String alias) {
            this.alias = alias;
            return this;
        }
        @Override
        public GetAliasesSpecificName build() {
            return new GetAliasesSpecificName(this, alias);
        }
    }
}
