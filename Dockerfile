# Build UI Dependencies

FROM node:11.1.0 as npm_builder
# Set the entrypoint as bin bash incase we want to inspect the container
ENTRYPOINT ["/bin/bash"]
# Manually copy the package.json
COPY ./webui/package.json /usr/src/app/package.json
COPY ./webui/package-lock.json /usr/src/app/package-lock.json
# Set the work directory to where we copied our source files
WORKDIR /usr/src/app
# Install all of our dependencies
RUN npm install

FROM npm_builder as ui_builder
# Copy the app excluding everything in the .dockerignore
COPY webui /usr/src/app
# Put node_modules into the path, this will purely be used for accessing the angular cli
ENV PATH /usr/src/app/node_modules/.bin:$PATH
# Set the work directory to where we copied our source files
WORKDIR /usr/src/app
# Build our distributable
RUN npm run build:prod

# Build MinSQL

FROM minio/minsql:deps as rust_builder

WORKDIR /usr/src/minsql
COPY . .
COPY --from=ui_builder /usr/src/app/dist/minsql-ui ./static/ui

RUN cargo install --path .

# build final container

FROM alpine:3.9

WORKDIR /

COPY --from=rust_builder /root/.cargo/bin/minsql /usr/bin/minsql

CMD ["minsql"]
