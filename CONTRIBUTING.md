# MinSQL Contribution Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minsql.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minsql)

``MinSQL`` community welcomes your contribution. To make the process as seamless as possible, we recommend you read this contribution guide.

## Development Workflow

Start by forking the MinSQL GitHub repository, make changes in a branch and then send a pull request. We encourage pull requests to discuss code changes. Here are the steps in detail:

### Setup your MinSQL GitHub Repository
Fork [MinSQL upstream](https://github.com/minio/minsql/fork) source repository to your own personal repository. Copy the URL of your MinSQL fork (you will need it for the `git clone` command below).

```sh
$ git clone https://github.com/$USER_ID/minsql
$ cd minsql
```

### Set up git remote as ``upstream``
```sh
$ cd minsql
$ git remote add upstream https://github.com/minio/minsql
$ git fetch upstream
$ git merge upstream/master
...
```

### Create your feature branch
Before making code changes, make sure you create a separate branch for these changes

```
$ git checkout -b my-new-feature
```

### Test MinSQL server changes
After your code changes, make sure

- To add test cases for the new code. If you have questions about how to do it, please ask on our [Slack](slack.min.io) channel.
- To squash your commits into a single commit `git rebase -i`. It's okay to force update your pull request.


### Commit changes
After verification, commit your changes. This is a [great post](https://chris.beams.io/posts/git-commit/) on how to write useful commit messages

```
$ git commit -am 'Add some feature'
```

### Push to the branch
Push your locally committed changes to the remote origin (your fork)
```
$ git push origin my-new-feature
```

### Create a Pull Request
Pull requests can be created via GitHub. Refer to [this document](https://help.github.com/articles/creating-a-pull-request/) for detailed steps on how to create a pull request. After a Pull Request gets peer reviewed and approved, it will be merged.

### What are the coding guidelines for MinSQL?
``MinSQL`` is fully conformant with Rust style. Refer: [Style Guidelines](https://doc.rust-lang.org/1.0.0/style/) article from Rust project. If you observe offending code, please feel free to send a pull request or ping us on [Slack](https://slack.min.io).
