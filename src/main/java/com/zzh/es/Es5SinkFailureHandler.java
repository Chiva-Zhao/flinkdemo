package com.zzh.es;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.util.ExceptionUtils;
import org.elasticsearch.action.ActionRequest;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Optional;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-13 16:27
 **/
@Slf4j
public class Es5SinkFailureHandler implements ActionRequestFailureHandler {
    @Override
    public void onFailure(ActionRequest action, Throwable failure, int i, RequestIndexer indexer) throws Throwable {
        if (ExceptionUtils.findThrowable(failure, SocketTimeoutException.class).isPresent()) {
            return;
        } else {
            Optional<IOException> fails = ExceptionUtils.findThrowable(failure, IOException.class);
            if (fails.isPresent()) {
                IOException ioe = fails.get();
                if (ioe != null && ioe.getMessage() != null && ioe.getMessage().contains("max retry timeout")) {
                    // request retries exceeded max retry timeout
                    log.error(ioe.getMessage());
                    return;
                }
            }
            throw failure;
        }
    }
}
