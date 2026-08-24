import Config

# DMEx is a library; this configuration exists only for its own dev and test
# environments. Host applications either pass their repo to `use DMEx.Log` / the
# `DMEx.Supervisor` child spec explicitly, or name it once as
# `config :dmex, repo: MyApp.Repo` — DMEx's one configuration key, read by
# `DMEx.Storage.default_repo/0`. Deliberately unset here: the suite passes
# `repo:` explicitly everywhere, and a default would mask a door that forgot to.

# Build exqlite's NIF from source rather than downloading a precompiled one.
# The published artifacts are linked against a newer glibc than this dev
# container ships, so the prebuilt library loads with `GLIBC_2.33 not found`.
# Compiling locally costs one `make` and works on whatever the host has.
config :exqlite, force_build: true

if config_env() == :test do
  import_config "test.exs"
end
