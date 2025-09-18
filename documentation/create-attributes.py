import yaml
import os

# Define the input and output file paths
input_file = 'antora.yml'
output_file = 'gen-attributes.adoc'

# Function to parse the YAML file and extract attributes
def parse_antora_yaml(file_path):
    with open(file_path, 'r') as file:
        antora_data = yaml.safe_load(file)
        
    return antora_data.get('asciidoc', {}).get('attributes', {})

# Function to write attributes to an AsciiDoc file
def write_asciidoc(attributes, output_file):
    with open(output_file, 'w') as file:
        file.write("// AsciiDoc Attributes\n// DO NOT EDIT - generated from antora.yml\n")
        for key, value in attributes.items():
            file.write(f":{key}: {value}\n")

# Main script execution
if __name__ == "__main__":
    if os.path.exists(input_file):
        attributes = parse_antora_yaml(input_file)
        write_asciidoc(attributes, output_file)
        print(f"AsciiDoc attributes have been written to {output_file}")
    else:
        print(f"Error: {input_file} does not exist.")
