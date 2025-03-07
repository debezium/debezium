def validateVersionFormat(version) {
    if (!(version ==~ /\d+\.\d+.\d+\.(Final|(Alpha|Beta|CR)\d+)/)) {
        error "Release version '$version' is not of the required format x.y.z.suffix"
    }
}

def setDryRun() {
    if (DRY_RUN == null) {
        DRY_RUN = false
    }
    else if (DRY_RUN instanceof String) {
        DRY_RUN = Boolean.valueOf(DRY_RUN)
    }
    echo "Dry run: ${DRY_RUN}"
}