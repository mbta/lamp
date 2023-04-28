# elixir version and erlang version are defined in .tool_versions
ARG ELIXIR_VERSION=1.14.4
ARG ERLANG_VERSION=25.3

# this is the latest as of 27 april 2023
ARG ALPINE_VERSION=3.17.2

# use this image to install dependencies and build the application
FROM hexpm/elixir:${ELIXIR_VERSION}-erlang-${ERLANG_VERSION}-alpine-${ALPINE_VERSION} as builder

# create a directory and add the project files to it
WORKDIR /root

# define this as a prod environment
ENV MIX_ENV=prod

COPY assets assets
COPY config config
COPY lib lib
COPY mix.* /root/

#  install hex, rebar, and dependencies
RUN mix local.hex --force
RUN mix local.rebar --force
RUN mix deps.get --only prod

# build the application
RUN mix release 

# use this image to run the application
FROM alpine:${ALPINE_VERSION}

# add dumb init for entrypoint
RUN apk upgrade --no-cache --update
RUN apk add --no-cache --update dumb-init libstdc++ ncurses-libs libgcc libssl1.1

# create a directory to run in and a directory for the compiled application
RUN mkdir /work /api
RUN adduser -D api && chown api /api && chown api /work
COPY --from=builder /root/_build/prod/rel/api /api

ENV MIX_ENV=prod

# create a user to own the work dir, switch to that user and dir
USER api
WORKDIR /work

ENTRYPOINT ["/usr/bin/dumb-init", "--"]

# run the application
CMD ["/api/bin/api", "start"]