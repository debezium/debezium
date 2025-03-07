def validateVersionFormat(version) {
    if (!(version ==~ /\d+\.\d+.\d+\.(Final|(Alpha|Beta|CR)\d+)/)) {
        error "Release version '$version' is not of the required format x.y.z.suffix"
    }
}