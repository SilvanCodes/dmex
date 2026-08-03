import Config

# DMEx is a library; this configuration exists only for its own dev and test
# environments. Host applications configure their repo themselves and pass it to
# `use DMEx.Log` / the `DMEx.Supervisor` child spec explicitly — DMEx reads no
# global application configuration of its own.

# Build exqlite's NIF from source rather than downloading a precompiled one.
# The published artifacts are linked against a newer glibc than this dev
# container ships, so the prebuilt library loads with `GLIBC_2.33 not found`.
# Compiling locally costs one `make` and works on whatever the host has.
config :exqlite, force_build: true

if config_env() == :test do
  import_config "test.exs"
end
