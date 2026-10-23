FROM elixir:1.20-slim@sha256:e500da1777164f9be05f7ffc0fe06cdb692f453bf7d651755e72310ec8a92eed AS build

ENV MIX_ENV=prod
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN mix local.hex --force && mix local.rebar --force

COPY mix.exs mix.lock ./
COPY config/config.exs config/prod.exs config/
RUN mix deps.get --only prod
RUN mix deps.compile

COPY lib lib
COPY config/runtime.exs config/
RUN mix compile
RUN mix run --no-start -e 'name = Smolquery.DockerSmoke; {:ok, _pid} = Smolquery.Engine.start_link(name: name, extensions: []); version = Smolquery.Engine.version(name); expected = "v" <> Smolquery.DuckDB.version(); if version != expected, do: raise("expected DuckDB #{expected}, got #{version}"); IO.puts("DuckDB #{version}")'
ENV SMOLQUERY_EXTENSION_DIRECTORY=/app/duckdb-extensions
RUN mix run --no-start -e '{:ok, _pid} = Smolquery.Engine.start_link(name: Smolquery.DockerExtensions, extensions: [:httpfs, :json, :ducklake, :aws, :postgres, :sqlite, {:cache_httpfs, :community}]); IO.puts("installed: " <> Enum.join(File.ls!(hd(Path.wildcard("/app/duckdb-extensions/*/*"))), " "))'
COPY assets assets
COPY priv/static priv/static
COPY priv/pg_catalog priv/pg_catalog
RUN mix assets.deploy
COPY rel rel
RUN mix release

FROM elixir:1.20-slim@sha256:e500da1777164f9be05f7ffc0fe06cdb692f453bf7d651755e72310ec8a92eed

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# T-722: the release runs as smolquery (10001), not root. A locked DuckDB
# engine may write any path in its allowed_paths, the node's CA bundle among
# them (T-718); as this user it cannot write a root-owned file. The release
# writes /data, RELEASE_TMP (/app/tmp), DuckDB's default spill root
# (/app/.tmp) and a crash dump (ERL_CRASH_DUMP, /app/tmp), so those are the
# user's; the rest of /app stays root's, the DuckDB extensions with it, so
# every extension the node loads is installed here at build time (sqlite
# among them: the default catalog is a SQLite file).
RUN groupadd --system --gid 10001 smolquery \
    && useradd --system --uid 10001 --gid 10001 --home-dir /data --no-create-home \
       --shell /usr/sbin/nologin smolquery

WORKDIR /app
COPY --from=build /app/_build/prod/rel/smolquery ./
COPY --from=build /app/duckdb-extensions ./duckdb-extensions
RUN mkdir -p /data /app/tmp /app/.tmp \
    && chown smolquery:smolquery /data /app/tmp /app/.tmp

ARG GIT_SHA=""
ENV HOME=/data \
    SMOLQUERY_DATA_DIR=/data \
    SMOLQUERY_EXTENSION_DIRECTORY=/app/duckdb-extensions \
    ERL_CRASH_DUMP=/app/tmp/erl_crash.dump \
    SMOLQUERY_GIT_SHA=$GIT_SHA

VOLUME /data
EXPOSE 4000 4002 5432 8123

USER 10001:10001

ENTRYPOINT ["/app/bin/smolquery"]
CMD ["start"]
