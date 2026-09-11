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
import java.util.List;

/**
 * Sorts (or validates the sort order of) the contributor name files.
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
 * <li>{@code sort} (default) rewrites the files in sorted order, like {@code impsort:sort}</li>
 * <li>{@code check} (CI) fails the build if a file is not sorted, like {@code impsort:check}</li>
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
        final List<String> unsorted = new ArrayList<>();

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
            final List<String> sorted = original.stream().sorted(COMPARATOR).toList();

            if (original.equals(sorted)) {
                System.out.println(name + ": sorted (" + sorted.size() + " entries)");
            }
            else if (check) {
                reportOffenders(name, original);
                unsorted.add(name);
            }
            else {
                Files.write(file, sorted, StandardCharsets.UTF_8);
                System.out.println(name + ": sorted " + sorted.size() + " entries");
            }
        }

        if (!unsorted.isEmpty()) {
            fail("Contributor file(s) not sorted: " + String.join(", ", unsorted) + ". Run 'mvn validate' to sort them.");
        }
    }

    // Report the first few offenders so the contributor can see what to move.
    private static void reportOffenders(String name, List<String> lines) {
        final List<Integer> offenders = new ArrayList<>();
        for (int i = 1; i < lines.size(); i++) {
            if (COMPARATOR.compare(lines.get(i - 1), lines.get(i)) > 0) {
                offenders.add(i);
            }
        }

        System.err.println(name + " is not sorted; " + offenders.size() + " entr" + (offenders.size() == 1 ? "y is" : "ies are") + " out of order:");
        for (int i : offenders.subList(0, Math.min(offenders.size(), MAX_REPORTED_OFFENDERS))) {
            System.err.println("  line " + (i + 1) + ": '" + lines.get(i) + "' should come before '" + lines.get(i - 1) + "'");
        }
        if (offenders.size() > MAX_REPORTED_OFFENDERS) {
            System.err.println("  ... and " + (offenders.size() - MAX_REPORTED_OFFENDERS) + " more");
        }
    }

    private static void fail(String message) {
        System.err.println(message);
        System.exit(1);
    }
}
