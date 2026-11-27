/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import io.debezium.ai.decision.api.Answer;
import io.debezium.ai.decision.api.ChoiceAnswer;
import io.debezium.ai.decision.api.ChoiceQuestion;
import io.debezium.ai.decision.api.DecisionRequest;
import io.debezium.ai.decision.api.DecisionResponse;
import io.debezium.ai.decision.api.DecisionService;
import io.debezium.ai.decision.api.ScoreAnswer;
import io.debezium.ai.decision.api.ScoreQuestion;
import io.debezium.ai.decision.api.Usage;
import io.debezium.ai.decision.api.YesNoAnswer;
import io.debezium.ai.decision.api.YesNoQuestion;

/**
 * Real, in-memory {@link DecisionService} for tests. It records the requests and returns either computed canned
 * answers or whatever the configured responder produces.
 *
 * @author Jiri Pechanec
 */
public class StubDecisionService implements DecisionService {

    private static final AtomicReference<StubDecisionService> LAST = new AtomicReference<>();

    public final List<DecisionRequest> requests = new ArrayList<>();
    public Function<DecisionRequest, DecisionResponse> responder = StubDecisionService::cannedResponse;
    public boolean closed;

    StubDecisionService() {
        LAST.set(this);
    }

    /**
     * @return the service most recently created by the {@link StubDecisionServiceFactory}
     */
    public static StubDecisionService last() {
        return LAST.get();
    }

    @Override
    public synchronized DecisionResponse decide(final DecisionRequest request) {
        requests.add(request);
        return responder.apply(request);
    }

    @Override
    public void close() {
        closed = true;
    }

    /**
     * Answers every question with its first option, the first level or probability {@code 0.75}.
     */
    public static DecisionResponse cannedResponse(final DecisionRequest request) {
        final Map<String, Answer> answers = new LinkedHashMap<>();
        for (final var question : request.questions()) {
            final Answer answer = switch (question) {
                case ChoiceQuestion choice -> {
                    final Map<String, Double> probabilities = new LinkedHashMap<>();
                    choice.options().keySet().forEach(option -> probabilities.put(option, 1.0 / choice.options().size()));
                    yield new ChoiceAnswer(choice.name(), choice.options().firstEntry().getKey(), 0.9, probabilities);
                }
                case ScoreQuestion score -> new ScoreAnswer(score.name(), 0.5, 0.8, Map.of(0, 0.5, 1, 0.5));
                case YesNoQuestion yesNo -> new YesNoAnswer(yesNo.name(), 0.75);
            };
            answers.put(question.name(), answer);
        }
        return new DecisionResponse("stub-1", answers, new Usage(1, 1));
    }
}
