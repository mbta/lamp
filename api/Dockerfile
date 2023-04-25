FROM elixir:1.14.4

# create a directory and add the project files to it
WORKDIR /root

ADD assets assets
ADD config config
ADD lib lib
ADD mix.* /root/

#  install hex, rebar, and dependencies
RUN mix local.hex --force
RUN mix local.rebar --force
RUN mix deps.get --only prod

# build the application
RUN mix do compile

# run the application
CMD ["mix", "phx.server"]