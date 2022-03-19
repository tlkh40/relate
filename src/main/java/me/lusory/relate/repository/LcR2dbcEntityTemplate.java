package me.lusory.relate.repository;

import me.lusory.relate.LcReactiveDataRelationalClient;
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
