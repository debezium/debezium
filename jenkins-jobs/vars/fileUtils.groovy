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