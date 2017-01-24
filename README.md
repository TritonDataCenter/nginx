# nginx

This repository represents the version of [nginx](https://nginx.com)
that is used as part of the [Manta
project](https://github.com/joyent/manta) in the
[mako](https://github.com/joyent/manta-mako) project.

We have changes to nginx currently covering the following broader
features:

* Returning calculated md5 checksums from streamed request bodies
* Ensuring that the dav module properly renames are atomic (fsync)

## Repository Management

This repository is downstream of the [github nginx
mirror](https://github.com/nginx/nginx).

To better understand and maintain our differences from nginx, we try to
manage branches and tags in a specific fashion. First and foremost, all
branches and tags from the upstream nginx repository are mirrored here.

Anything that is Joyent-specific begins with a `joyent/` prefix. The one
exception to this is the old `mako` branch which represents the original
changes we made against nginx back in 2012 and 2013.

Branches with Joyent modifications are named `joyent/<version>`, such as
`joyent/1.10.2`. This is a branch that starts from the nginx
`release-1.10.2` tag. These branches will have all of our patches
rebased on top of them. Currently, this repository is consumed by
`mako`, which contains a git submodule for this repository. That
submodule will point to a tag in this repository that uses the form
`joyent/v<version>j<branch release num>`. The first release as described
above would be: `joyent/v1.10.2j1`. If we need to cut another release
from this branch, we would tag it `joyent/v1.10.2j2` and continue to
increment the number after the `j`. Note we use the `j` instead of `r`
which would more traditionally be used to indicate a revision.  We use
`j` in case nginx for some reason wants to use it in its version strings
for whatever reason.

When it comes time to update to a newer version of nginx, we would take
the following steps:

* Ensure that we have pushed all changes from `nginx/nginx` and synced
  all of our branches and tags.
* Identify the release tag that corresponds to the point release. For
  this example, we'll say that's `release-1.12.3`.
* Create a new branch named `joyent/<version>` from the tag. In this
  case we would name the branch `joyent/1.12.3`.
* Rebase all of our patches on to that new branch, removing any patches
  that are no longer necessary.
* Test the new nginx binary.
* Review and Commit all relevant changes.
* Create a new tag `joyent/v1.12.3j1`.
* Update the [manta-mako](https://github.com/joyent/manta-mako)
  submodule to point to the new tag.

## Licensing

All of our changes to nginx are under the terms of nginx's 2-clause BSD
license.
