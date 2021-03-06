/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.snapshotlifecycle.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.snapshotlifecycle.action.PutSnapshotLifecycleAction;

import java.io.IOException;

public class RestPutSnapshotLifecycleAction extends BaseRestHandler {

    public RestPutSnapshotLifecycleAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.PUT, "/_slm/policy/{name}", this);
    }

    @Override
    public String getName() {
        return "slm_put_lifecycle";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String snapLifecycleName = request.param("name");
        try (XContentParser parser = request.contentParser()) {
            PutSnapshotLifecycleAction.Request req = PutSnapshotLifecycleAction.Request.parseRequest(snapLifecycleName, parser);
            req.timeout(request.paramAsTime("timeout", req.timeout()));
            req.masterNodeTimeout(request.paramAsTime("master_timeout", req.masterNodeTimeout()));
            return channel -> client.execute(PutSnapshotLifecycleAction.INSTANCE, req, new RestToXContentListener<>(channel));
        }
    }
}
