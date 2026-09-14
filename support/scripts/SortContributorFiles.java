/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Sorts and de-duplicates (or validates the sort order and uniqueness of) the contributor name files.
 * <p>
 * These files are append-only lists that every contributor touches. When names are appended to the
 * end, two concurrent pull requests always modify the same line and always conflict. Keeping the
 * files sorted spreads insertions across the whole file, so two contributors only conflict when
 * their names are immediate alphabetical neighbours.
 * <p>
 * The root pom runs this from the {@code validate} phase through the JDK source launcher
 * ({@code java SortContributorFiles.java}), so it is compiled and run by whichever JDK Maven is
 * using and needs no build step and no runtime beyond that JDK. This deliberately avoids scripting
 * runtimes such as Groovy, whose bundled bytecode reader must be released against a JDK before it
 * can read that JDK's class files, and which therefore broke the build on every early-access JDK.
 * <p>
 * Driven by the {@code format.names.goal} property, mirroring {@code format.imports.goal} used by
 * impsort:
 * <ul>
 * <li>{@code sort} (default) rewrites the files in sorted order with duplicate lines dropped, like
 * {@code impsort:sort}</li>
 * <li>{@code check} (CI) fails the build if a file is not sorted or contains duplicate lines, like
 * {@code impsort:check}</li>
 * </ul>
 *
 * @author Chris Cranford
 */
public class SortContributorFiles {

    // Files to keep sorted, relative to the build root. Comparison is on the whole line, which for
    // Aliases.txt means the GitHub login (the first field) is the sort key.
    private static final List<String> TARGETS = List.of(
            "COPYRIGHT.txt",
            "jenkins-jobs/scripts/config/Aliases.txt",
            "jenkins-jobs/scripts/config/FilteredNames.txt");

    // Deterministic across platforms and locales: case-insensitive, then case-sensitive as a
    // tie-breaker so the ordering is total and two machines never disagree.
    private static final Comparator<String> COMPARATOR = String.CASE_INSENSITIVE_ORDER.thenComparing(Comparator.naturalOrder());

    private static final int MAX_REPORTED_OFFENDERS = 10;

    public static void main(String[] args) throws IOException {
        if (args.length != 2 || !(args[1].equals("sort") || args[1].equals("check"))) {
            fail("Usage: SortContributorFiles <basedir> <sort|check>");
        }

        final Path basedir = Path.of(args[0]);
        final boolean check = args[1].equals("check");
        final List<String> invalid = new ArrayList<>();

        for (String name : TARGETS) {
            final Path file = basedir.resolve(name);
            if (!Files.isRegularFile(file)) {
                fail("Contributor file '" + name + "' does not exist");
            }

            // Blank lines carry no meaning in these files; drop them so a stray trailing newline does not
            // sort to the top of the file.
            final List<String> original = Files.readAllLines(file, StandardCharsets.UTF_8).stream()
                    .filter(line -> !line.isBlank())
                    .toList();
            // The comparator only treats identical lines as equal, so distinct() drops exactly the
            // entries that would otherwise sort next to each other as duplicates.
            final List<String> sorted = original.stream().sorted(COMPARATOR).distinct().toList();

            if (original.equals(sorted)) {
                System.out.println(name + ": sorted (" + sorted.size() + " entries)");
            }
            else if (check) {
                if (reportDuplicates(name, original) | reportUnsorted(name, original)) {
                    invalid.add(name);
                }
            }
            else {
                Files.write(file, sorted, StandardCharsets.UTF_8);
                final int dropped = original.size() - sorted.size();
                System.out.println(name + ": sorted " + sorted.size() + " entries" + (dropped == 0 ? "" : ", dropped " + dropped + " duplicate(s)"));
            }
        }

        if (!invalid.isEmpty()) {
            fail("Contributor file(s) not sorted or containing duplicates: " + String.join(", ", invalid) + ". Run 'mvn validate' to fix them.");
        }
    }

    // Report every duplicated entry with the lines it appears on; returns whether any were found.
    private static boolean reportDuplicates(String name, List<String> lines) {
        final Map<String, List<Integer>> occurrences = new LinkedHashMap<>();
        for (int i = 0; i < lines.size(); i++) {
            occurrences.computeIfAbsent(lines.get(i), k -> new ArrayList<>()).add(i + 1);
        }

        final List<Map.Entry<String, List<Integer>>> duplicates = occurrences.entrySet().stream()
                .filter(entry -> entry.getValue().size() > 1)
                .toList();
        if (duplicates.isEmpty()) {
            return false;
        }

        System.err.println(name + " contains " + duplicates.size() + " duplicate entr" + (duplicates.size() == 1 ? "y" : "ies") + ":");
        for (Map.Entry<String, List<Integer>> entry : duplicates.subList(0, Math.min(duplicates.size(), MAX_REPORTED_OFFENDERS))) {
            System.err.println("  '" + entry.getKey() + "' appears on lines " + entry.getValue());
        }
        if (duplicates.size() > MAX_REPORTED_OFFENDERS) {
            System.err.println("  ... and " + (duplicates.size() - MAX_REPORTED_OFFENDERS) + " more");
        }
        return true;
    }

    // Report the first few out-of-order entries so the contributor can see what to move; returns
    // whether any were found.
    private static boolean reportUnsorted(String name, List<String> lines) {
        final List<Integer> offenders = new ArrayList<>();
        for (int i = 1; i < lines.size(); i++) {
            if (COMPARATOR.compare(lines.get(i - 1), lines.get(i)) > 0) {
                offenders.add(i);
            }
        }
        if (offenders.isEmpty()) {
            return false;
        }

        System.err.println(name + " is not sorted; " + offenders.size() + " entr" + (offenders.size() == 1 ? "y is" : "ies are") + " out of order:");
        for (int i : offenders.subList(0, Math.min(offenders.size(), MAX_REPORTED_OFFENDERS))) {
            System.err.println("  line " + (i + 1) + ": '" + lines.get(i) + "' should come before '" + lines.get(i - 1) + "'");
        }
        if (offenders.size() > MAX_REPORTED_OFFENDERS) {
            System.err.println("  ... and " + (offenders.size() - MAX_REPORTED_OFFENDERS) + " more");
        }
        return true;
    }

    private static void fail(String message) {
        System.err.println(message);
        System.exit(1);
    }
}
