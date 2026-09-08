/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Sorts (or validates the sort order of) the contributor name files.
 *
 * These files are append-only lists that every contributor touches. When names are appended to the
 * end, two concurrent pull requests always modify the same line and always conflict. Keeping the
 * files sorted spreads insertions across the whole file, so two contributors only conflict when
 * their names are immediate alphabetical neighbours.
 *
 * Driven by the 'format.names.goal' property, mirroring 'format.imports.goal' used by impsort:
 *
 *   sort  (default)  rewrites the files in sorted order, like 'impsort:sort'
 *   check  (CI)      fails the build if a file is not sorted, like 'impsort:check'
 */

// Files to keep sorted, relative to the build root. Comparison is on the whole line, which for
// Aliases.txt means the GitHub login (the first field) is the sort key.
def targets = [
    'COPYRIGHT.txt',
    'jenkins-jobs/scripts/config/Aliases.txt',
    'jenkins-jobs/scripts/config/FilteredNames.txt'
]

// System properties first so that '-Dformat.names.goal=check' on the command line wins over the
// default declared in the pom; gmavenplus binds 'properties' to the project properties only.
def property = { String key, String fallback ->
    System.getProperty(key) ?: properties[key] ?: fallback
}

def goal = property('format.names.goal', 'sort')
if (!(goal in ['sort', 'check'])) {
    fail("Unknown format.names.goal '${goal}'; expected 'sort' or 'check'")
}
if (Boolean.parseBoolean(property('format.skip', 'false'))) {
    log.info("Contributor file sort skipped (format.skip=true)")
    return
}

// Deterministic across platforms and locales: case-insensitive, then case-sensitive as a
// tie-breaker so the ordering is total and two machines never disagree.
def comparator = { String a, String b ->
    int c = a.compareToIgnoreCase(b)
    c != 0 ? c : a.compareTo(b)
} as Comparator<String>

def basedir = new File(project.basedir as String)
def unsorted = []

targets.each { name ->
    def file = new File(basedir, name)
    if (!file.exists()) {
        fail("Contributor file '${name}' does not exist")
    }

    // Blank lines carry no meaning in these files; drop them so a stray trailing newline does not
    // sort to the top of the file.
    def original = file.getText('UTF-8').readLines().findAll { !it.trim().isEmpty() }
    def sorted = original.toSorted(comparator)

    if (original == sorted) {
        log.info("${name}: sorted (${sorted.size()} entries)")
        return
    }

    if (goal == 'check') {
        // Report the first few offenders so the contributor can see what to move.
        def offenders = (1..<original.size()).findAll { comparator.compare(original[it - 1], original[it]) > 0 }
        log.error("${name} is not sorted; ${offenders.size()} entr${offenders.size() == 1 ? 'y is' : 'ies are'} out of order:")
        offenders.take(10).each { i ->
            log.error("  line ${i + 1}: '${original[i]}' should come before '${original[i - 1]}'")
        }
        if (offenders.size() > 10) {
            log.error("  ... and ${offenders.size() - 10} more")
        }
        unsorted << name
    }
    else {
        file.withWriter('UTF-8') { w -> sorted.each { w.writeLine(it) } }
        log.info("${name}: sorted ${sorted.size()} entries")
    }
}

if (unsorted) {
    fail("Contributor file(s) not sorted: ${unsorted.join(', ')}. Run 'mvn validate' to sort them.")
}
