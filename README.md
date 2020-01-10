# nginx

This repository represents the version of [nginx](https://nginx.com)
that is used as part of the [Manta
project](https://github.com/joyent/manta) in the
[mako](https://github.com/joyent/manta-mako) project.

We have changes to nginx currently covering the following broader
features:

* Return the calculated md5 checksums from the streamed request bodies
* Ensure that the dav module's renames are properly atomic (fsync)

## Repository Management

This repository is downstream of the [github nginx
mirror](https://github.com/nginx/nginx).

To better understand and maintain our differences from nginx, we try to manage
branches in a specific fashion. First and foremost, all branches from the
upstream nginx repository are mirrored here.

Anything that is Joyent-specific begins with a `joyent/` prefix. The one
exception to this is the old `mako` branch which represents the original changes
we made against nginx back in 2012 and 2013.

Branches with Joyent modifications are named `joyent/<version>-mantav<version>`,
such as `joyent/1.10.2-mantav2`.  This is a branch that starts from the nginx
`release-1.10.2` tag and is intended for use by the mantav2 versions of Manta
components.

These branches will have all of our patches rebased on top of them. Currently,
this repository is consumed by `mako`, which contains a git submodule for this
repository. That submodule will point to a commit in this repository in the
appropriate manta version branch.

When it comes time to update to a newer version of nginx, we would take
the following steps:

* Ensure that we have pushed all changes from `nginx/nginx` and synced
  all of our branches and tags.
* Identify the release tag that corresponds to the point release. For
  this example, we'll say that's `release-1.12.3`.
* Create a new branch named `joyent/<version>-mantavX` from the tag. In this
  case we would name the branch `joyent/1.12.3-mantav2`.
* Rebase all of our patches on to that new branch, removing any patches
  that are no longer necessary.
* Test the new nginx binary.
* Review and Commit all relevant changes.
* Update the [manta-mako](https://github.com/joyent/manta-mako)
  submodule to point to the new commit.
