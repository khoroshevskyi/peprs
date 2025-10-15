<h1 align="center">
<img src="docs/img/peprs_logo.svg" alt="peprs logo" height="100px">
</h1>

`peprs` - A spicy libary for managing biological sample metadata to enable reproducible and scalable bioinformatics

## About
`peprs` is a rust implementation of the [PEP specification](https://pep.databio.org/) and expanded ecosystem. In short, PEP is a framework for managing biological sample metadata. PEP is a **community driven** effort to create a **fast**, **reliable**, and **scalable** library for handling biological sample metadata.

PEP and its ecosystem is developed and maintained by the [Databio](https://databio.org) team. As a challenge and learning experience, we have been rewriting the core components of the PEP ecosystem in Rust for performance and reliability. This library is still in early development.

We are starting with the core PEP specification for metadata management and will expand to include the full ecosystem (looper, pephub-client, pipestat). The core PEP specification is implemented in the `peprs-core` crate. The Python bindings are implemented in the `peprs-py` crate.