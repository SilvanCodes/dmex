import Config

# DMEx is a library; this configuration exists only for its own dev and test
# environments. Host applications configure their repo themselves and pass it to
# `use DMEx.Log` / the `DMEx.Supervisor` child spec explicitly — DMEx reads no
# global application configuration of its own.

if config_env() == :test do
  import_config "test.exs"
end
