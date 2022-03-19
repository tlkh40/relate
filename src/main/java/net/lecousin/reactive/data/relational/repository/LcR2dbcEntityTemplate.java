package net.lecousin.reactive.data.relational.repository;

import net.lecousin.reactive.data.relational.LcReactiveDataRelationalClient;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;

public class LcR2dbcEntityTemplate extends R2dbcEntityTemplate {

    private final LcReactiveDataRelationalClient lcClient;

    public LcR2dbcEntityTemplate(LcReactiveDataRelationalClient lcClient) {
        super(lcClient.getSpringClient(), lcClient.getDataAccess());
        this.lcClient = lcClient;
    }

    public LcReactiveDataRelationalClient getLcClient() {
        return lcClient;
    }
}
