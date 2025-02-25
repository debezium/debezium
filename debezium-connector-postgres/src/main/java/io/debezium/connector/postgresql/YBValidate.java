package io.debezium.connector.postgresql;

import io.debezium.DebeziumException;
import io.debezium.connector.postgresql.transforms.yugabytedb.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class to store all the validation methods.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YBValidate {
    private static final Logger LOGGER = LoggerFactory.getLogger(YBValidate.class);
    private static final String RANGE_BEGIN = "0";
    private static final String RANGE_END = "65536";

    public static void completeRangesProvided(List<String> slotRanges) {
        List<Pair<Integer, Integer>> pairList = slotRanges.stream()
                .map(entry -> {
                    String[] parts = entry.split(",");
                    return new Pair<>(Integer.valueOf(parts[0]), Integer.valueOf(parts[1]));
                })
                .sorted(Comparator.comparing(Pair::getFirst))
                .collect(Collectors.toList());

        int rangeBegin = Integer.valueOf(RANGE_BEGIN);

        for (Pair<Integer, Integer> pair : pairList) {
            if (rangeBegin != pair.getFirst()) {
                LOGGER.error("Error while validating ranges: {}", pairList);
                throw new DebeziumException(
                    String.format("Tablet range starting from hash_code %d is missing", rangeBegin));
            }

            rangeBegin = pair.getSecond();
        }

        // At this point, if the range is complete, rangeBegin will be pointing to the RANGE_END value.
        if (rangeBegin != Integer.valueOf(RANGE_END)) {
            LOGGER.error("Error while validating ranges: {}", pairList);
            throw new DebeziumException(
                String.format("Incomplete ranges provided. Range starting from hash_code %d is missing", rangeBegin));
        }
    }

    public static void slotAndPublicationsAreEqual(List<String> slotNames, List<String> publicationNames) {
        if (slotNames.size() != publicationNames.size()) {
            throw new DebeziumException(
                String.format("Number of provided slots does not match the number of provided " +
                                "publications. Slots: %s, Publications: %s", slotNames, publicationNames));
        }
    }

    public static void slotRangesMatchSlotNames(List<String> slotNames, List<String> slotRanges) {
        if (slotNames.size() != slotRanges.size()) {
            throw new DebeziumException(
                    String.format("Number of provided slots does not match the number of provided " +
                            "slot ranges. Slots: %s, Slot ranges: %s", slotNames, slotRanges));
        }
    }
}
