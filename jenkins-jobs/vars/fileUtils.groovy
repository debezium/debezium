import groovy.json.*

def modifyFile(filename, modClosure) {
    echo "========================================================================"
    echo "Modifying file $filename"
    echo "========================================================================"
    def originalFile = readFile(filename)
    echo "Content to be modified:\n$originalFile"
    echo "========================================================================"
    def updatedFile = modClosure.call(originalFile)
    echo "Content after modification:\n$updatedFile"
    echo "========================================================================"
    writeFile(
            file: filename,
            text: updatedFile
    )
}

def generateDescriptorManifest(String outputDirPath, String commit, String branch, String timestamp) {
    // Build the manifest structure
    def manifest = [
        schemaVersion: "1.0",
        build: [
            timestamp: timestamp,
            sourceRepository: "debezium/debezium",
            sourceCommit: commit,
            sourceBranch: branch
        ],
        components: [:]
    ]

    echo "Scanning descriptors in: ${outputDirPath}"

    // Change to the descriptors output directory
    dir(outputDirPath) {
        // Find all subdirectories (component types) using shell
        def componentDirs = sh(
            script: 'find . -maxdepth 1 -type d ! -path . -exec basename {} \\; | sort',
            returnStdout: true
        ).trim().split('\n')

        componentDirs.each { componentType ->
            if (!componentType) return

            def componentList = []
            echo "Processing component type: ${componentType}"

            // Find all JSON files in this component directory
            def jsonFiles = findFiles(glob: "${componentType}/*.json")

            jsonFiles.each { file ->
                try {
                    // Read and parse the JSON file using Jenkins readFile step
                    def jsonContent = readFile(file: file.path)
                    def json = new JsonSlurper().parseText(jsonContent)

                    // Use filename (without .json) as the class name
                    def className = file.name.replaceAll(/\.json$/, '')

                    def entry = [
                        'class': className,
                        name: json.name ?: '',
                        description: json.metadata?.description ?: '',
                        descriptor: "${componentType}/${file.name}"
                    ]

                    componentList << entry
                    echo "  - Added: ${className}"
                } catch (Exception e) {
                    echo "  - WARNING: Failed to parse ${file.name}: ${e.message}"
                }
            }

            if (componentList) {
                manifest.components[componentType] = componentList
                echo "Added ${componentList.size()} items for ${componentType}"
            }
        }

        // Write manifest using Jenkins writeFile step
        def manifestJson = JsonOutput.prettyPrint(JsonOutput.toJson(manifest))
        writeFile(file: 'manifest.json', text: manifestJson)

        echo "✓ Generated manifest at: ${outputDirPath}/manifest.json"
        echo "\nSummary:"
        manifest.components.each { type, list ->
            echo "  - ${type}: ${list.size()} items"
        }
    }

    return manifest
}