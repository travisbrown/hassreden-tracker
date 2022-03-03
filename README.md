# Hassreden-Tracker

This repository is currently being used to coordinate work on the [Hassreden-Tracker][hassreden-tracker-pf] project.
This means that for the immediate future the [issue tracker](https://github.com/travisbrown/hassreden-tracker/issues)
will be the most interesting part of this repository, although eventually some code will be migrated here.

## Projects

The project builds on several existing open source projects that I maintain (all of which rely on other open source projects):

* [cancel-culture](https://github.com/travisbrown/cancel-culture): Tools for Twitter archiving, indexing, and block list management.
* [wayback-rs](https://github.com/travisbrown/wayback-rs): Rust library for working with the [Wayback Machine](https://web.archive.org/).
* [twitter-watch](https://github.com/travisbrown/twitter-watch): Reports about screen name changes and suspensions.
* [twitter-tracker](https://github.com/travisbrown/twitter-tracker): Services that produce the twitter-watch reports (currently private).
* [evasion](https://github.com/travisbrown/evasion): Report tracking far-right ban evasion accounts.
* [egg-mode-extras](https://github.com/travisbrown/egg-mode-extras): Rate-limit-aware asynchronous streams for working with the [Twitter API](https://developer.twitter.com/en/docs/twitter-api).
* [orcrs](https://github.com/travisbrown/orcrs): [Apache ORC](https://orc.apache.org/) file reading library for Rust.
* [hkvdb](https://github.com/travisbrown/hkvdb): A key-value store interface built on [RocksDB](https://rocksdb.org/).
* [twpis](https://github.com/travisbrown/twpis): Twitter profile image collection.
* [memory.lol](https://memory.lol): A web service providing historical Twitter account information (currently private).
* [stop-the-steal](https://github.com/travisbrown/stop-the-steal): 9.7 million profile snapshots for Twitter users associated with the Stop the Steal movement.
* [octocrabby](https://github.com/travisbrown/octocrabby): Block list management for [GitHub](https://github.com/) accounts.

## Principles

### Technical

Most code is written in the [Rust programming language](https://www.rust-lang.org/). I've chosen to build this software primarily in Rust for a couple of reasons:

* The values of the Rust community tend to align with mine.
* Rust's focus on performance is especially valuable for projects operated by organizations or individuals with limited resources.

On the second point: almost all of the tools and services below can be run effectively on the smallest and cheapest Amazon Web Services EC2 instances, for example.

### Licensing and distribution

All code and data is made publicly available except in cases where this would undermine the core project goals or the privacy or safety of project members.

Most of these projects are published under the [Mozilla Public License](https://www.mozilla.org/en-US/MPL/).
Some projects that could be misused for commercial surveillance are published under the [Anti-Capitalist Software License](https://anticapitalist.software/).

Rust libraries are published to [crates.io](https://crates.io/), a widely-used Rust package registry.

[hassreden-tracker-pf]: https://prototypefund.de/project/hassreden-tracker/