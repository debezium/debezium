# Debezium Documentation

The Debezium documentation in [documentation](https://github.com/debezium/debezium/tree/main/documentation) is built using the [Antora Framework](https://www.antora.org).

- [Antora Quickstart](#antora-quickstart)
    - [Component descriptors](#component-descriptors)
    - [Page content](#page-content)
    - [Linking to content](#linking-to-content)
        - [External](#external)
        - [Internal](#internal)
    - [Navigation Pane](#navigation-pane)
    - [Antora Configuration](#antora-configuration)
    - [Attributes](#attributes)
- [Contributing to the Documentation](#contributing-to-the-documentation)
    - [Using AsciiDoc attributes](#using-asciidoc-attributes)
    - [Cross references](#cross-references)
    - [Adding images](#adding-images)
    - [Best practices](#best-practices)

## Antora Quickstart

The Antora framework is opinionated about its directory structure, which is why the folder layout is as follows:

```
  documentation
  |    antora.yml
  |
  └─── modules
       |
       └─── ROOT
            |    nav.adoc
            |
            └─── pages
                 |
                 └─── (content)  
```

### Component descriptors

The Antora documentation system uses a concept called component descriptors.  Debezium makes use of a single component descriptor called `ROOT` since we want to document all features in a single section.  In the future if we find documenting connectors or certain features separately from others, we can look at using multiple component descriptor layouts, however for now we use the `ROOT` descriptor for this purpose.

### Page content

All documentation content should be added under the `pages` directory and should use the extension `.adoc` for consistency.  The hierarchy used for pages under this point is mainly for organizational purposes for documentation writers.  If you wish to reference a specific `.adoc` from another file, the directory hierarchy will be used in the cross reference link however.

### Linking to content

In the Antora-based documentation, there are two styles of links, _External_ and _Internal_.

#### External

An external link is one that points to a resource that is not maintained in the Antora documentation.  In order to link to these resources, you should use the normal AsciiDoc `link` macro as shown below:

```
link:<scheme>://<host>/<path-to-resource>(<text-friendly-name>)
```

#### Internal

An internal link is one that points to a resource _in_ the Antora documentation.  Antora comes with a special AsciiDoc macro called `xref` that should be used for this purpose.  This macro should be used as shown below:

```
xref:<version>@<component>:<module>:<path-to-file><#fragment>[<text-friendly-name>]
```

As an example, if you want to link to a file called `example.adoc` within the same component and for the same version of the documentation being rendered, you would write the link as `xref:example.adoc[This is my example]`.  More information on Antora's page-id structure and how to build links can be found [here](https://docs.antora.org/antora/2.1/page/page-id/#how-is-a-page-referenced-with-its-id).

### Navigation Pane

The Antora documentation layout uses a navigational pane on the left-side of the UI.  This navigation pane is driven by content found in the `nav.adoc` located inside the component descriptor's top-level directory.  This file must be **manually** maintained when a new topic is to be present in the left navigation layout.  You can see our current navigation pane layout [here](https://wwww.github.com/debezium/tree/main/documentation/modules/ROOT/nav.adoc).

### Antora Configuration

A component descriptor is identified by the existence of an `antora.yml` file located in the root of the component's documentation layout.  The most important aspect of this file pertains to the `version` attribute found in the file's contents:

```
name: debezium
title: Debezium Documentation
version: `0.10`
nav:
  - modules/ROOT/nav.adoc
```

The `name` attribute describes a path that will be appended to the Antora's base build path.  This allows differing components to output their documentation content separately.

The `title` attribute describes a UI friendly name for this component.  Since Debezium uses a single _ROOT_ component, all documentation will be located under the name `Debezium Documentation` in the UI.

The `nav` attribute describes an array of navigation AsciiDoc files uses to build the left navigation pane.  Debezium makes use of a single navigation file.

The `version` attribute is the most important aspect of this file.  This designates the naming convention to be used when referencing what _version_ this documentation represents.  This can be an actual version number as shown above, in which case it should be quoted.  Other examples could be things like `stable` or `latest`.

**NOTE**: As of Antora 2.3, the `antora.yml` file can now define Asciidoc attributes, which are discussed below.

### Attributes

Should a documentation page need to reference attributes, e.g. version numbers, this can be accomplished in two ways:

1. Defined in the `antora.yml` component descriptor in this repository.
2. Defined in the Antora playbook files in the Debezium website repository.

The general rule is if an attribute changes frequently or is related to a specific version or subset of versions of Debezium, it likely belongs in the `antora.yml` file in this repository.  If the attribute changes infrequent or is not specific to a given version of Debezium, its easier to maintain that in the various playbook files in the Debezium website repository.

Lets say we need to add an attribute that points to our JIRA issue for issue links, 
that would be an example of an attrbute that would be defined in the playbook.
But if we needed to add an attribute that points to a specific version of a Maven artifact or reference a specific version of a dependency, 
that's more appropriate for the `antora.yml` component descriptor located in this repository. 

The current `antora.yml` component descriptor looks similar to the following:
```
asciidoc:
  attributes:
    debezium-version: '1.1.0.Final'
    debezium-dev-version: '1.2'
    debezium-kafka-version: '2.4.0'
    debezium-docker-label: '1.1'
    install-version: '1.1'
    assemblies: '../assemblies'
    modules: '../../modules'
    mysql-connector-plugin-download: 'https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/1.1.0.Final/debezium-connector-mysql-1.1.0.Final-plugin.tar.gz'
    mysql-version: '8.0'    
    strimzi-version: '0.13.0'
```

Attributes are defined by creating a nested yaml structure under `asciidoc.attributes` where the key-value attribute pairs are to be defined.  

The playbook files in the website repository use the same layout, shown here:
```
# Global asciidoc attributes here, used across all versions of documentation
asciidoc:
  attributes:
    prodname: 'Debezium'
    context: 'debezium'
    jira-url: 'https://issues.redhat.com'
    # because of how handlebars templates work with page.attributes, this must be prefixed with "page-"
    page-copyright-year: '2020'
``` 

**NOTE**: Given that the Debezium documentation is consumed downstream by other processes, do not define attributes in the `_attributes.adoc` file and use it as an include nor should you define attributes locally in a given .adoc file.

## Contributing to the Documentation

Follow these guidelines for contributing to the Debezium documentation.

### Using AsciiDoc attributes

AsciiDoc attributes are effectively like variables, enabling you to insert short snippets of text into the documentation.
They are typically used for abstracting content like version numbers, branding, and root URLs.
For example, to insert the contents of the `prodname` attribute (which currently resolves to `Debezium`), use the AsciiDoc syntax, `{prodname}`.

We recommend that you use the following attributes when contributing content to the Debezium documentation:

* `{prodname}` instead of Debezium.

See the [Attributes](#attributes) section for more details.

### Cross references

If you need to link to another section or location anywhere in the Debezium documentation, the recommended approach is to use anchor IDs in combination with the `xref:` cross-referencing macro.

For example, if you want to link to a section called _Custom connector_, first of all define an anchor on the line preceding the heading, as follows:
```
[id="custom-connector"]
== Custom connector
```

You can then link to this section from anywhere in the documentation using this syntax:
```
xref:custom-connector[]
```

Note the following advantages of the `xref:` macro:
* You do not need to specify or know the location of the file where the `custom-connector` ID is defined.
AsciiDoc automatically figures this out at build time.
* Consequently, if you move files around, you will not break any links.
* When you build the documentation, AsciiDoc automatically converts the cross-reference into a link using the title text, _Custom connector_, in the link.
If you change the title of the section, AsciiDoc will automatically update the link text to match.

### Adding images

To add an image to the documentation:
1. Add your image file to the `documentation/modules/ROOT/assets/images` directory.
2. Reference the image using the syntax `image::MY_IMAGE.png[]` (or for an inline image, `image:MY_IMAGE.png[]`).

At build time, AsciiDoc reads the value of the standard `imagesdir` attribute to discover the location of images.
There is no need to set this attribute yourself, it is already defined for you.
Note that if you view or render a _single_ AsciiDoc file, you will not be able to view the image.
Because of the way the images are organized (in combination with the `imagesdir` attribute), you can only see the images correctly rendered when you build the whole documentation set using Antora.

### Best practices

Note the following additional recommendations:

* If your contribution also involves reformatting large amounts of text (modifying white space, line breaks, and so on), please make these reformatting updates in a _separate_ commit.
It is much easier for reviewers to focus on the technical changes in the content, if commits are not polluted by large amounts of trivial formatting changes.
* Avoid long lines for the sake of simpler diffs;
instead of sticking to a hard character number limit,
put one sentence or line of thought per line.
It's not a black-or-white rule, but it works surprisingly well after getting used to it.
E.g. commas are a good indicator for moving to a new line.
