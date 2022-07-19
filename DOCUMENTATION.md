# Debezium Documentation

The Debezium documentation in [documentation](https://github.com/debezium/debezium/tree/main/documentation) is built using the [Antora Framework](https://www.antora.org).

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
    debezium-kafka-version: '2.4.0'
    debezium-docker-label: '1.1'
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

### Adding cross-references that link to other content

The Debezium Antora-based documentation uses two styles of links for creating cross-references to other content, _External_ and _Internal_.

#### External links

An external link is one that points to a resource that is not included within one of the Debezium AsciiDoc files.
To link to an external resource, use the AsciiDoc `link` macro as in the following example:

```
link:<scheme>://<host>/<path-to-resource>[<link-text>]
```

For example:

```
link:https://docs.antora.org/antora/latest/navigation/xrefs-and-link-text/[Antora linking documentation]
```

#### Internal links

An internal link is one that points to a resource that is included in one of the Debezium AsciiDoc files.
The target of an internal link is the anchor ID that is defined for the element that you want to link to.
For example, to link to a section header with the name _Custom connector_, the header must have an anchor ID defined on the line that precedes it, as in the following examples:

```
[id="custom-connector"]
== Custom connector
```

```
[[schema-change-topic]]
== Schema change topic
```

The method that you use to construct the link depends on whether the link targets content that is in the same AsciiDoc file or in a different file.

##### Links that target content within the same file
To link to content within the same file, use the AsciiDoc `xref` macro.
To use the `xref` macro, construct the link as in the following example:

```
xref:<anchorID>[<link-text>]
```
For example,
```
xref:custom-connector[Custom connector]
```
Although the Antora framework that we use to publish the Debezium documentation can accept an empty `[<link-text>]` value, to enable the Debezium documentation to be consumed downstream in other frameworks, it's necessary to supply a `[<link-text>]` value.

##### Links that target content in a different file
To link to Debezium content that is in a different file, construct the link according to the following format:

```
{link-prefix}:<content-category-attribute>#<anchorID>
```
For example,

```
{link-prefix}:{link-outbox-event-router}#basic-outbox-configuration
```
In the Antora-based documentation, the `{link-prefix}` attribute resolves to the `xref` macro, but when the content is reused in a downstream framework, it can contain a different value.

Similarly, in the Antora-based documentation, the `<content-category-attribute>` corresponds to the path to the targeted file, but when the content is rendered in a downstream framework, it can contain a different value. To determine which category attribute to use when linking to a different file, see the `link-*` attributes that are listed in the `asciidoc` section of the [`documentation/antora.yml`](/documentation/antora.yml) file.

Although the preceding format is not required for the Antora-based documentation, it is required to enable the Debezium documentation to be consumed in other downstream frameworks.

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
