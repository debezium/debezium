ACTION="split"           # Set the action: copy, move, delete, or custom
CUSTOM_COMMAND="gzip {}" # Specify the custom command (for custom action)

# Check if the file list is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <file_list.txt>" >&2
    exit 1
fi

# Read the file list
file_list="$1"

# Check if file_list exists
if [ ! -f "$file_list" ]; then
    echo "Error: File list '$file_list' not found." >&2
    exit 1
fi

# Process each file in the list
while IFS= read -r file; do
    # Skip empty lines
    [ -z "$file" ] && continue
    


    case "$ACTION" in
        split)
            echo "processing $file"
            nebel split -a attributes.adoc --conditions product --category-prefix debezium  --legacybasedir modules/ROOT/pages modules/ROOT/pages/$file
            ;;
                
        custom)
            if [ -z "$CUSTOM_COMMAND" ]; then
                echo "Error: 'custom' action requires a command to execute." >&2
                exit 1
            fi
            eval "${CUSTOM_COMMAND//\{\}/\"$file\"}" && echo "Executed custom command on '$file'"
            ;;
        
        *)
            echo "Error: Unknown action '$ACTION'." >&2
            exit 1
            ;;
    esac
done < "$file_list"