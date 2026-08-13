def call() {
}

def validateVersionFormat(version) {
    if (!(version ==~ /\d+\.\d+.\d+\.(Final|(Alpha|Beta|CR)\d+)/)) {
        error "Release version '$version' is not of the required format x.y.z.suffix"
    }
}

def getBooleanParameter(param) {
    if (param == null) {
        return false
    }
    else if (param instanceof String) {
        return Boolean.valueOf(param)
    }
    return param
}

def getDryRun() {
    def dryRun = getBooleanParameter(env.DRY_RUN)
    echo "Dry run: ${dryRun}"

    return dryRun
}

static String convertToSemver(String releaseTag) {

    def baseVersionMatcher = releaseTag =~ /^(\d+\.\d+\.\d+)\..*$/
    def baseVersion = baseVersionMatcher ? baseVersionMatcher[0][1] : releaseTag

    if (releaseTag.endsWith(".Final")) {
        return baseVersion
    } else {

        def prereleasePartMatcher = releaseTag =~ /^${baseVersion}\.(.*)$/
        def prereleasePart = prereleasePartMatcher ? prereleasePartMatcher[0][1] : ""

        def semverPrerelease = prereleasePart.toLowerCase().replace('.', '-')

        return "${baseVersion}-${semverPrerelease}"
    }
}
