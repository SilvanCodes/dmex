ARG RUNNER_IMAGE="debian:bullseye-20210902-slim"

# does user setup and gives 'install-packages' script, see https://github.com/gitpod-io/workspace-images
FROM gitpod/workspace-base@sha256:5fcdb99366114646be6a82559aedb498b0f8b500192195782546664514ed166d as development

USER gitpod

ENV ASDF_VERSION=v0.20.0
ENV ASDF_ERLANG_PLUGIN_COMMIT=7d76f6bed86d01c77b792534c5d69bcadb08b9eb
ENV ASDF_ELIXIR_PLUGIN_COMMIT=859f99759430a44eedbf37dab62782ca3aeaaae9

# setup asdf-vm (Go rewrite, single binary release since v0.16.0): https://asdf-vm.com/guide/getting-started.html
RUN mkdir -p $HOME/.asdf/bin \
    && curl -L "https://github.com/asdf-vm/asdf/releases/download/${ASDF_VERSION}/asdf-${ASDF_VERSION}-linux-$(dpkg --print-architecture).tar.gz" \
    | tar -xzC $HOME/.asdf/bin \
    && echo 'export PATH="$HOME/.asdf/bin:${ASDF_DATA_DIR:-$HOME/.asdf}/shims:$PATH"' >> $HOME/.bashrc \
    && echo '. <(asdf completion bash)' >> $HOME/.bashrc

# in the following 'bash -i -c' is used to have asdf available as '-i' reads from .bashrc

# Plugin Versions are pinned because we want a 💯 Percent reproducible dev environment.
# install and pin erlang: https://github.com/asdf-vm/asdf-erlang
RUN bash -i -c 'asdf plugin add erlang https://github.com/asdf-vm/asdf-erlang.git && asdf plugin update erlang ${ASDF_ERLANG_PLUGIN_COMMIT}'

# install and pin asdf elixir plugin: https://github.com/asdf-vm/asdf-elixir
RUN bash -i -c 'asdf plugin add elixir https://github.com/asdf-vm/asdf-elixir.git && asdf plugin update elixir ${ASDF_ELIXIR_PLUGIN_COMMIT}'

# get .tool-versions from repo
COPY .tool-versions $HOME/

# install versions specified in .tool-versions
RUN bash -i -c 'asdf install'

ENV ELIXIR_HEX_VERSION=v2.5.1

# install # hex (v2.5.1 -> see: https://github.com/hexpm/hex/releases/tag/v2.5.1)
RUN bash -i -c 'mix archive.install github hexpm/hex ref ${ELIXIR_HEX_VERSION} --force'

# install # rebar (matching version depending on elixir? -> see: https://github.com/elixir-lang/elixir/blob/cc9e9b29a7b473010ed17f894e6a576983a9c294/lib/mix/lib/mix/tasks/local.rebar.ex#L124)
RUN bash -i -c 'mix local.rebar --force'

USER root

# get inotify-tools for live reload from phoenix, see: https://hexdocs.pm/phoenix/installation.html#inotify-tools-for-linux-users
RUN install-packages inotify-tools

# install common utilities
RUN install-packages \
    curl

USER gitpod

# install flyctl, see: https://fly.io/docs/getting-started/installing-flyctl/
RUN curl -L https://fly.io/install.sh | sh \
    && echo 'export FLYCTL_INSTALL="$HOME/.fly"' >> $HOME/.bashrc \
    && echo 'export PATH="$HOME/.fly/bin:$PATH"' >> $HOME/.bashrc

# install Claude Code, see: https://code.claude.com/docs/en/setup
RUN curl -fsSL https://claude.ai/install.sh | bash \
    && echo 'export PATH="$HOME/.local/bin:$PATH"' >> $HOME/.bashrc


# FROM development as builder

# USER root

# # prepare build dir
# WORKDIR /app

# # set build ENV
# ENV MIX_ENV="prod"

# # install mix dependencies
# COPY mix.exs mix.lock ./
# RUN bash -i -c 'mix deps.get --only $MIX_ENV'
# RUN mkdir config

# # copy compile-time config files before we compile dependencies
# # to ensure any relevant config change will trigger the dependencies
# # to be re-compiled.
# COPY config/config.exs config/${MIX_ENV}.exs config/
# RUN bash -i -c 'mix deps.compile'

# COPY priv priv

# # note: if your project uses a tool like https://purgecss.com/,
# # which customizes asset compilation based on what it finds in
# # your Elixir templates, you will need to move the asset compilation
# # step down so that `lib` is available.
# COPY assets assets

# # Compile the release
# COPY lib lib

# RUN bash -i -c 'mix compile'

# # compile assets
# RUN bash -i -c 'mix assets.deploy'

# # Changes to config/runtime.exs don't require recompiling the code
# COPY config/runtime.exs config/

# COPY rel rel
# RUN bash -i -c 'mix release'

# # start a new build stage so that the final image will only contain
# # the compiled release and other runtime necessities
# FROM ${RUNNER_IMAGE} as release

# RUN apt-get update -y && apt-get install -y libstdc++6 openssl libncurses5 locales \
#     && apt-get clean && rm -f /var/lib/apt/lists/*_*

# # Set the locale
# RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && locale-gen

# ENV LANG en_US.UTF-8
# ENV LANGUAGE en_US:en
# ENV LC_ALL en_US.UTF-8

# WORKDIR "/app"
# RUN chown nobody /app

# # Only copy the final release from the build stage
# COPY --from=builder --chown=nobody:root /app/_build/prod/rel/ephemeral ./

# USER nobody

# CMD ["/app/bin/server"]

# # Appended by flyctl
# ENV ECTO_IPV6 true
# ENV ERL_AFLAGS "-proto_dist inet6_tcp"