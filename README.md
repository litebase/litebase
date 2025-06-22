<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/litebase-github-readme-banner-dark.png">
  <source media="(prefers-color-scheme: light)" srcset="docs/images/litebase-github-readme-banner.png">
  <img alt="Fallback image description" src="docs/images/litebase-github-readme-banner.png">
</picture>

---
[![Continuous Integration](https://github.com/litebase/litebase/actions/workflows/ci.yml/badge.svg)](https://github.com/litebase/litebase/actions/workflows/ci.yml)
<br />
---
<br />

# Welcome to Litebase

[SQLite](https://sqlite.org/) is a fully-featured database that has been around for 25 years, and today it is the most used database in the world. Building on this strong legacy, Litebase is a distributed relational database that embeds SQLite and complements it with additional features, aiming to scale with most modern applications.

Litebase is cloud-native, **scales horizontally**, supports **strongly-consistent** ACID transactions, and supports read replicas with **eventual consistency**. Litebase tiers data between instance storage, distributed file systems like AWS EFS, and object storage like AWS S3 optimizing both performance and cost.  The design decision to leverage existing distributed storage architectures allows Litebase to inherit distributed system properties like high availability, fault tolerance, and disaster recovery without the need for complex replication protocols.

### Features

* Authentication
* Authorization
* Backups
* Branching
* Distributed storage
* Object storage
* Point-in-time recovery
* Primary forwarding writes
* Read replicas

## Docs

You can learn more about Litebase in our documentation. You'll find all the information you need to get started, including installation, development, deployment, and administration of Litebase.

**[Read the Docs →](https://litebase.com/docs)**


## Need help?

If you have any questions or need help, we encourage you to start a discussion on GitHub. This is a great place to ask questions, share ideas, request features and get help from the community. Please do not use Issues for general questions or support requests, as they are intended for tracking bugs and accepted feature requests.

**[Start a Discussion →](https://github.com/litebase/litebase/discussions/new/choose)**

If you find a bug, please open an issue with a detailed description of the problem, including steps to reproduce it. If you have a feature request, please start a discussion instead.

**[Create an Issue →](https://github.com/litebase/litebase/issues)**


## Community

We expect all community members to adhere to our Code of Conduct. Please follow these guidelines to ensure everyone has a positive experience.

**[Code of Conduct →](https://github.com/litebase/litebase?tab=coc-ov-file#readme)**

Please review our Contribution Guide before submitting an issue or pull request to understand how to contribute effectively.

**[Contributing Guide →](https://github.com/litebase/litebase/blob/main/docs/CONTRIBUTING.md)**


## Security Vulnerabilities

All security related issues should be reported directly to [security@litebase.com](mailto:security@litebase.com).

## License

Litebase is [open-sourced](https://opensource.org/) software licensed under the [MIT License](LICENSE.md).

