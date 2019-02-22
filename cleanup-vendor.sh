#!/bin/bash

find vendor/ -name '.editorconfig' -exec rm -v {} \;
find vendor/ -name '*.lock' -exec rm -v {} \;
find vendor/ -name 'Gopkg.toml' -exec rm -v {} \;
find vendor/ -name '*.md' -exec rm -v {} \;
find vendor/ -name '*.yml' -exec rm -v {} \;
find vendor/ -name '*.md' -exec rm -v {} \;
find vendor/ -name 'Gopkg.toml' -exec rm -v {} \;
find vendor/ -name '*.lock' -exec rm -v {} \;
find vendor/ -name '.editorconfig' -exec rm -v {} \;
find vendor/ -name '*_tests.go' -exec rm -v {} \;
find vendor/ -name '.gitignore' -exec rm -v {} \;
find vendor/ -name 'Makefile' -exec rm -v {} \;
