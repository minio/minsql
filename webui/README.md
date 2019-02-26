# MinSQL WebUI

The Web browser UI code is developed as a React app. It is built and
embedded into the `minsql` executable.

## Required tools for development

1. Nodejs (>= 11.9.0)
2. Yarn (>= 1.13.0)
3. Statik (`go get github.com/rakyll/statik`)

## How to develop the Web UI?

1. Make changes to the files of the React app in the current
   directory - this is usual React development using `yarn` (>= 1.13.0)
2. To embed the updated application into `minsql`, in the current directory run:
   ```shell
   $ yarn && yarn build # Builds optimized React app in `./build`
   $ statik -src=build -p assets -f # Update the `assets` go package with new React app
   ```
3. Now switch to the root of the project and build `minsql` in the usual way.

This project was bootstrapped with [Create React
App](https://github.com/facebook/create-react-app).
