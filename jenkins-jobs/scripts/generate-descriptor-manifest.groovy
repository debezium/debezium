import groovy.json.*

/**
 * Generates a manifest.json file for the Debezium descriptor registry.
 *
 * Usage: generate-descriptor-manifest.groovy <descriptors-output-dir> <commit-hash> <branch-name> <timestamp>
 *
 * Arguments:
 *   descriptors-output-dir - Directory containing the generated descriptor files
 *   commit-hash            - Git commit hash of the source Debezium repository
 *   branch-name            - Git branch name of the source Debezium repository
 *   timestamp              - Build timestamp in ISO 8601 format
 */

if (args.length != 4) {
    println "Usage: generate-descriptor-manifest.groovy <descriptors-output-dir> <commit-hash> <branch-name> <timestamp>"
    return -1
}

def descriptorsOutputDir = args[0]
def commitHash = args[1]
def branchName = args[2]
def timestamp = args[3]

// Validate output directory exists
def outputDir = new File(descriptorsOutputDir)
if (!outputDir.exists() || !outputDir.isDirectory()) {
    println "ERROR: Descriptors output directory does not exist or is not a directory: ${descriptorsOutputDir}"
    return -1
}

// Build the manifest structure
def manifest = [
    schemaVersion: "1.0",
    build: [
        timestamp: timestamp,
        sourceRepository: "debezium/debezium",
        sourceCommit: commitHash,
        sourceBranch: branchName
    ],
    components: [:]
]

println "Scanning descriptors in: ${descriptorsOutputDir}"

outputDir.listFiles()?.findAll { it.isDirectory() }.each { dir ->
    def componentType = dir.name
    def componentList = []

    println "Processing component type: ${componentType}"

    dir.listFiles()?.findAll { it.name.endsWith('.json') }.each { file ->
        try {
            def json = new JsonSlurper().parse(file)

            def className = file.name.replaceAll(/\.json$/, '')

            def entry = [
                'class': className,
                name: json.name ?: '',
                description: json.metadata?.description ?: '',
                descriptor: "${componentType}/${file.name}"
            ]

            componentList << entry
            println "  - Added: ${className}"
        } catch (Exception e) {
            println "  - WARNING: Failed to parse ${file.name}: ${e.message}"
        }
    }

    if (componentList) {
        manifest.components[componentType] = componentList
        println "Added ${componentList.size()} items for ${componentType}"
    }
}

def manifestFile = new File(outputDir, "manifest.json")
manifestFile.text = JsonOutput.prettyPrint(JsonOutput.toJson(manifest))

println "\n✓ Generated manifest at: ${manifestFile.absolutePath}"
println "\nSummary:"
manifest.components.each { type, list ->
    println "  - ${type}: ${list.size()} items"
}

return 0
