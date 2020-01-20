# Debezium Documentation

The Debezium documentation in [documentation](https://github.com/debezium/debezium/tree/master/documentation) is built using the [Antora Framework](https://www.antora.org).

- [Antora Quickstart](#antora-quickstart)
    - [Component descriptors](#component-descriptors)
    - [Page content](#page-content)
    - [Linking to content](#linking-to-content)
        - [External](#external)
        - [Internal](#internal)
    - [Navigation Pane](#navigation-pane)
    - [Antora Configuration](#antora-configuration)
    - [Attributes](#attributes)
- [What to change _after_ releasing new version](#what-to-change-_after_-releasing-new-version)
    - [Attributes](#attributes-1)
    - [Antora YAML](#antora-yaml)
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

The Antora documentation system uses a concept called component descriptors.  Debezium makes use of a single component descriptor called `ROOT` since we want to document all features in a single section.  In the future if we find documenting connectors or certain features separately from others, we can look at using multiple component desecriptor layouts, however for now we use the `ROOT` descriptor for this purpose.

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

The Antora documentation layout uses a navigational pane on the left-side of the UI.  This navigation pane is driven by content found in the `nav.adoc` located inside the compoennt descriptor's top-level directory.  This file must be **manually** maintained when a new topic is to be present in the left navigation layout.  You can see our current navigation pane layout [here](https://wwww.github.com/debezium/tree/master/documentation/modules/ROOT/nav.adoc).

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

### Attributes

Should a documentation page need to reference attributes, e.g. version numbers, this can be accomplished by importing the `_attributes.adoc` file in the `.adoc` documentation page.  If the attribute is new, its generally a good idea to update the `_attributes.adoc` with it so that it can be reused and updated in a single place.

The current attributes file looks similar to the following:
```
:moduledir: ..
:attachmentsdir: {moduledir}/assets/attachments
:examplesdir: {moduledir}/examples
:imagesdir: {moduledir}/assets/images
:partialsdir: {moduledir}/pages/_partials

:debezium-version: 0.10.0.Beta4
:debezium-dev-version: 0.10
:debezium-kafka-version: 2.3.0
:debezium-docker-label: 0.10
:install-version: 0.10
:confluent-platform-version: 5.1.2
:strimzi-version: 0.10.0

```

## What to change _after_ releasing new version

It's important that 2 files be properly maintained after the repository transitions between releases

* _attributes.adoc
* antora.yml

### Attributes

Whenever any release is made, its important that the `_attributes.adoc` file [here](https://www.github.com/debezium/tree/master/documentation/modules/ROOT/pages/_attributes.adoc) is updated to reflect the version change.  For example, if version _0.9.4.Final_ was just released, the attributes file should be updated to with version _0.9.5.Final_ or whatever the next anticipated release will be.

### Antora YAML

The antora component descriptor file, `antora.yml` only needs to be updated when the next anticipated release will be a new major or minor.  Since documentation is maintained using a `<MAJOR>.<MINOR>` scheme, this file would only be changed when transitioning from version _0.9_ to _0.10_ or _1.1_ to _2.0_ as an example.

## Contributing to the Documentation

Follow these guidelines for contributing to the Debezium documentation.

### Using AsciiDoc attributes

AsciiDoc attributes are effectively like variables, enabling you to insert short snippets of text into the documentation.
They are typically used for abstracting content like version numbers, branding, and root URLs.
For example, to insert the contents of the `prodname` attribute (which currently resolves to `Debezium`), use the AsciiDoc syntax, `{prodname}`.

We recommend that you use the following attributes when contributing content to the Debezium documentation:

* `{prodname}` instead of Debezium.

Attributes are defined both in the [playbook.yml](https://github.com/debezium/debezium.github.io/blob/develop/playbook.yml) file and also (replicated) in the [playbook_author.yml](https://github.com/debezium/debezium.github.io/blob/develop/playbook_author.yml) file in the https://github.com/debezium/debezium.github.io/ repository.

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
