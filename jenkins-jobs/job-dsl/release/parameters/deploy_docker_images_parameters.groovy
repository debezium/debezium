return { parametersContext ->
    parametersContext.with {
        stringParam('STREAMS_TO_BUILD_COUNT', '2', 'How many most recent streams should be built')
        stringParam('TAGS_PER_STREAM_COUNT', '1', 'How any most recent tags per stream should be built')
        booleanParam('SKIP_UI', true, 'Should UI image be skipped (skipped by default due to build failure on CI)?')
    }
}
